package org.dbtoaster.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.dbtoaster.gui.DBPerfPanel;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.PlatformUI;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.experimental.chart.swt.ChartComposite;

import DBToaster.Profiler.Protocol.ProfileLocation;
import DBToaster.Profiler.Protocol.SampleType;
import DBToaster.Profiler.Protocol.SampleUnits;
import DBToaster.Profiler.Protocol.StatisticsProfile;
import DBToaster.Profiler.Protocol.Profiler.Client;

public class Executor
{
    String binaryPath;
    HashMap<String, String> binaryEnv;

    ProcessBuilder queryProcess;
    Process currentQuery;

    public class DBToasterProfiler implements Runnable
    {        
        Client client;
        TTransport transport;
        boolean terminated;
        
        TimeSeries samples;
        HashMap<Integer, String> codeLocations;
        LinkedList<Integer> handlerLocations;

        Millisecond currentMs;
        double currentSample;

        static final long profilerPeriod = 1000;

        public DBToasterProfiler(String codeLocationsFile, TProtocol protocol,
                TimeSeries profilerSamples)
        {
            reset();
            try {
                transport = protocol.getTransport();
                if ( !transport.isOpen() ) transport.open();
    
                client = new Client(protocol);
                samples = profilerSamples;
                
                codeLocations = new HashMap<Integer, String>();
                handlerLocations = new LinkedList<Integer>();
                readCodeLocations(codeLocationsFile);
            } catch (Exception e) {
                
            }
        }

        void reset() {
            client = null;
            transport = null;
            terminated = false;
        }

        void readCodeLocations(String codeLocationsFile)
        {
            try {
                BufferedReader locReader =
                    new BufferedReader(new FileReader(codeLocationsFile));
    
                // Track max location ids for each prefix since these are
                // the full handler profiles.
                HashMap<String, Integer> runningHandlerMax =
                    new HashMap<String, Integer>();

                String line = null;
                while ( (line = locReader.readLine()) != null )
                {
                    String[] fields = line.split(",");
                    
                    if ( fields.length != 2 ) {
                        System.err.println("Invalid code location: " + line);
                        break;
                    }
    
                    Integer locId = Integer.parseInt(fields[0]);
                    String codeLocName = fields[1];
                    codeLocations.put(locId, codeLocName);
                    
                    String handlerPrefix =
                        codeLocName.substring(0, codeLocName.lastIndexOf('_'));

                    if ( (!runningHandlerMax.containsKey(handlerPrefix)) ||
                            (runningHandlerMax.get(handlerPrefix) < locId) )
                    {
                        runningHandlerMax.put(handlerPrefix, locId);
                    }
                }
                
                locReader.close();
                
                for (Map.Entry<String, Integer> e : runningHandlerMax.entrySet()) {
                    int maxLoc = e.getValue();
                    for (int i = 0; i < maxLoc; ++i)
                        handlerLocations.add(i);
                }

            } catch (FileNotFoundException e) {
                System.err.println(
                    "Could not find code locations file: " + codeLocationsFile);
                e.printStackTrace();
            } catch (IOException e) {
                System.err.println(
                    "Error reading code locations file: " + codeLocationsFile);
                e.printStackTrace();
            }
        }

        void runOnce()
        {
            try {
                StatisticsProfile codeProfile = client.getStatisticsProfile();
                Calendar currentCal = Calendar.getInstance();
                currentCal.setTimeInMillis(codeProfile.t.tv_sec * 1000);
                currentCal.roll(Calendar.MILLISECOND,
                    (int) (codeProfile.t.tv_nsec / 1000000));
              
                // Compute total avg exec time over all handlers per sample period.
                double sumExecTime = 0.0;
                for (Integer l : handlerLocations)
                {
                    ProfileLocation loc = new ProfileLocation("cpu", l);
                    if ( codeProfile.getProfile().containsKey(loc) )
                    {
                        double locAvgExecTime = 0.0;
                        int sampleCount = 0;
                        List<SampleUnits> newSamples =
                            codeProfile.getProfile().get(loc);

                        System.out.println("Found " + newSamples.size() +
                            " samples for loc: " + loc.statName + ", " + loc.codeLocation);

                        for (SampleUnits s : newSamples)
                        {
                            if ( s.getExecTime() == null ) {
                                System.out.println("loc: " +
                                    loc.statName + ", " + loc.codeLocation + " null");
                            }
                            
                            if ( !(s.getSampleType() == SampleType.EXEC_TIME) ) {
                                System.err.println("Found invalid cpu sample type!");
                                break;
                            }

                            locAvgExecTime += ((s.getExecTime().getTv_sec() * 1e9)
                                + s.getExecTime().getTv_nsec());
                            ++sampleCount;
                            System.out.println(locAvgExecTime + " " + sampleCount);
                        }

                        sumExecTime += (locAvgExecTime / sampleCount);
                    }
                    else {
                        String msg = "Could not find cpu profile for location: "
                            + codeLocations.get(l) + " " + l;
                        System.err.println(msg);
                    }
                }

                currentMs = new Millisecond(currentCal.getTime());
                currentSample = sumExecTime;

                Display.getDefault().asyncExec(new Runnable()
                {
                    public void run()
                    {
                        samples.add(currentMs, currentSample);
                    }
                });

            } catch (Exception e) {
                System.out.println("getStatisticsProfile failed!");
                e.printStackTrace();
            }
        }

        public void run()
        {
            while ( !terminated && transport.isOpen() ) {
                // TODO: hand off data to visualizer.
                runOnce();
                try {
                    Thread.sleep(profilerPeriod);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        
            transport.close();
        }
    
        public void terminate() { terminated = true; }
    };

    DBToasterProfiler profiler;
    Thread currentProfilerThread;

    // Profiler client
    TBinaryProtocol.Factory profilerProtocolFactory;
    int currentPort;
    String currentHost;

    // Alternative databases
    private final static String POSTGRES_JDBC_DRIVER = "org.postgresql.Driver";
    private final static String POSTGRES_URL = "";

    private final static String HSQLDB_JDBC_DRIVER = "";
    private final static String HSQLDB_URL = "";

    public Executor() {}

    public Executor(String engineBinary)
    {
        File binaryFile = new File(engineBinary);
        if ( binaryFile.exists() && binaryFile.isFile() && binaryFile.canExecute() )
        {
            binaryPath = engineBinary;
            binaryEnv = new HashMap<String, String>();
            binaryEnv.putAll(System.getenv());

            queryProcess = new ProcessBuilder(binaryFile.getAbsoluteFile().toString());
            currentQuery = null;
            profilerProtocolFactory = new TBinaryProtocol.Factory();
            profiler = null;
        }
        else {
            System.err.println("Could not find engine binary " + engineBinary);
        }
    }
    
    public String getBinaryPath() { return binaryPath; } 
    
    String run(String codeLocationsFile, String execLogFile,
            int profilerServicePort, TimeSeries profilerSamples)
    {
        // Run query binary.
        String status = null;

        if ( queryProcess != null ) {
            LinkedList<String> args = new LinkedList<String>();
            args.add(binaryPath);
            args.add(Integer.toString(profilerServicePort));
            queryProcess.command(args);

            try {
                queryProcess.redirectErrorStream(true);
                currentPort = profilerServicePort;
                currentHost = "127.0.0.1";
                currentQuery = queryProcess.start();
            } catch (IOException e)
            {
                String fa = "";
                for (String a : args) fa += (fa.isEmpty()? "" : " ") + a;
                
                status = "IOException while running: "+fa;
                System.err.println(status);
                e.printStackTrace();
            }
        }
        
        if ( status != null ) return status;

        // Connect debugger client.
        if ( currentQuery != null )
        {
            
            // Periodically retrieve statistics while binary is still running.
            TSocket s = new TSocket(currentHost, currentPort);
            System.out.println("Opening socket....");
            try { s.open();
            if ( !s.isOpen() ) { System.out.println("Failed to connect!!"); }
            } catch (Exception e) { e.printStackTrace(); }
            
            System.out.println("Getting protocol....");
            
            TProtocol protocol = profilerProtocolFactory.getProtocol(s);
            
            profiler = new DBToasterProfiler(codeLocationsFile, protocol, profilerSamples);
            currentProfilerThread = new Thread(profiler);
            currentProfilerThread.start();
            
            try {
                BufferedReader logReader = new BufferedReader(
                        new InputStreamReader(currentQuery.getInputStream()));
                    
                Writer logWriter = new BufferedWriter(new FileWriter(execLogFile));
                String line = "";
                while ( (line = logReader.readLine()) != null )
                    logWriter.write(line + "\n");
    
                logWriter.close();
                logReader.close();
                    
                //int rs = currentQuery.waitFor();
                //if ( rs != 0 ) status = "Query returned non-zero exit status";
            } catch (IOException e) {
                status = "IOException while running query.";
                e.printStackTrace();
            }
            /*
            catch (InterruptedException e) {
                status = "Query interrputed while running...";
                e.printStackTrace();
            }
            */
        }
        System.out.println("EEEE: ");
        
        currentQuery = null;
        return status;
    }

    
    // TODO: data loading for alternative DBMS
    private File inFile;
    private BufferedReader br;
    
    int initFile(String filename)
    {
    	inFile = new File (filename);
    	
    	try {
    		br = new BufferedReader (new InputStreamReader (new FileInputStream(inFile)));
    	} catch (FileNotFoundException ex) {
    		System.err.println("File " + filename + " not found");
    		return -1;
    	}
    	
    	return 0;
    }
    
 
    void runJDBCQuery(String dbUrl, String sqlQuery,
            TimeSeries profilerSamples)
    {
        try {
            Connection conn = DriverManager.getConnection(dbUrl);
            Statement st = conn.createStatement();
            
            // TODO: loop adding chunks of data and repetitively issuing query
            // for ad-hoc queries
            long startTime = System.currentTimeMillis();
            
            ResultSet rs = st.executeQuery(sqlQuery);
            rs.last();
            
            long endTime = System.currentTimeMillis();
            long span = endTime - startTime;
            //profilerSamples.add(new Millisecond(span));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    private String post_username = "postgres";
    private String post_path = "/Library/PostgreSQL/8.4/";
    private String post_server = "localhost";
    private String post_port = "5432";
    private String post_dbname = "postgres";

    void initPostgreSQL()
    {
        // Set up JDBC driver. 
        try {
            Class.forName(POSTGRES_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find Postgres JDBC driver!");
            e.printStackTrace();
        }
    }
    
    void startupPostgreSQL()
    {
    	try {
    		Runtime rt = Runtime.getRuntime();
    		String str[] = {"sudo", "-u", "postgres" /*userid*/, 
    				post_path + "bin/pg_ctl", "start", "-w", "-D", post_path + "data"};
    		Process pr = rt.exec(str);
    		
    		int exitVal = pr.waitFor();
    		if (exitVal != 0) {
    			System.err.println("Could not start up postgres");
    		}
    	} catch (Exception e) {
    		System.err.println(e.toString());
    		e.printStackTrace();
    	}
    }
    
    void shutdownPostgreSQL()
    {
    	try {
    		Runtime rt = Runtime.getRuntime();
    		String str[] = {"sudo", "-u", "postgres", post_path + "bin/pg_ctl",
    				"stop", "-D", post_path + "data", "-m", "fast"};
    		Process pr = rt.exec(str);
    		
    		int exitVal = pr.waitFor();
    		if (exitVal != 0) {
    			System.err.println("Could not shut down postgres");	
    		}
    	} catch (Exception e) {
    		System.err.println(e.toString());
    		e.printStackTrace();
    	}
    }

    int postgreLoad(int num)
    {
    	Vector<String> buf = new Vector<String> (num);
    	int line = 0;
    	String tmp = "";
    	
    	try {
    		for (int i =0;i < num;i ++) {
    			tmp = br.readLine();
    			
    			if(tmp == null) 
    				break;
    			buf.add(tmp);
        		line ++;
    		}
    	} catch (IOException e) {
    		System.err.println("IO exception");
    		e.printStackTrace();
    	}
    	
    	if (line != 0) {
    		try {
    			Runtime rt = Runtime.getRuntime();
    			String str[] = {"psql", "-h", post_server, "-p", post_port, "-U", post_username, 
    					"-c", "copy test form stdin with csv", post_dbname};
    			Process pr = rt.exec(str);
    			
    			BufferedWriter output = new BufferedWriter(new OutputStreamWriter(pr.getOutputStream()));
    			
    			for (int i =0; i < line; i ++) {
    				output.write(buf.get(i));
    			}
    			output.write("\\.\n");
    			output.flush();
    			
    			int exitVal = pr.waitFor();
    			if(exitVal != 0) {
    				System.err.println("Error - loading postgres");
    				return -1;
    			}
    		} catch (Exception e) {
    			System.out.println(e.toString());
    			e.printStackTrace();
    		}
    			
    	}
    	return line;
    }

    void runPostgreSQL(String sqlOrTriggerQuery, boolean triggerScript,
            TimeSeries profilerSamples)
    {
    	startupPostgreSQL();
    	
    	initFile("5.csv"); /* database filename */
    	
    	int howmany = 50;
    	int total = 0;
    	int thres_total = 5000;
    	while (postgreLoad(howmany) == howmany && (total += howmany) <= thres_total) {
    		/* sleep ? */
    	}
    	
        if ( triggerScript )
            runPGTriggers(sqlOrTriggerQuery);
        else
            runPGQuery(sqlOrTriggerQuery, profilerSamples);
        
        shutdownPostgreSQL();
    }
    
    void runPGQuery(String sqlQuery, TimeSeries profilerSamples)
    {
        runJDBCQuery(POSTGRES_URL, sqlQuery, profilerSamples);
    }
    
    void runPGTriggers(String triggerScript)
    {
        
    }
    
    private String hsql_libpath = "/Users/yanif/software/hsqldb/lib/";
    private String hsql_dbname = "db0/db0";
    private String hsql_username = "sa";
    private String hsql_server = "localhost";    
    
    void startupHsqlDB()
    {
    	try {
    		String str[] = {"java", "org.hsqldb.Server", "-address", hsql_server,
    				"-database.0", "file:" + hsql_dbname, "-dbname.0"};
    		Runtime rt = Runtime.getRuntime();
    		rt.exec(str);
    		
    	} catch (Exception e) {
    		System.out.println(e.toString());
    		e.printStackTrace();
    	}
    }
    
    void shutdownHsqlDB() 
    {
    	try {
    		String str[] = { "java", "-jar", hsql_libpath + "hsqldb.jar",
    				"--sql", "shutdown;", hsql_server + "-" + hsql_username};
    		Runtime rt = Runtime.getRuntime();
    		rt.exec(str);
    	} catch (Exception e) {
    		System.out.println(e.toString());
    		e.printStackTrace();
    	}
    }
    
    void runHsqlDB(String sqlQuery, TimeSeries profilerSamples)
    {
    	startupHsqlDB();
        runJDBCQuery(HSQLDB_URL, sqlQuery, profilerSamples);
        shutdownHsqlDB();
    }
    
    void runOracle() {}
    
    void runSPE() {}
    
    public void runComparison( final Query q, LinkedHashMap<String, DBPerfPanel> dbPanels, 
    		Integer[] numdatabases, final String[] dbNames) {
    	for (int i = 0 ; i < numdatabases.length; i ++) {
    		if(numdatabases[i] == 1) {
    			DBPerfPanel panel = dbPanels.get(dbNames[i]);
    			final TimeSeries ts = panel.getCpuTimeSeries();
//    	        String[] dbNames = { "DBToaster", "Postgres", "HSQLDB", "DBMS1", "SPE1" };
    			System.out.println("Profiling "+q.getQueryName() + " with "+ dbNames[i]);
    			if(dbNames[i].equals("DBToaster")) {
    				System.out.println("Starting");
    				q.runQuery(20000, ts);
    			}
    			else if(dbNames[i].equals("Postgres")) {
    			    Thread th = new Thread (new Runnable() {
    			        public void run() {
    			            runPostgreSQL(q.getQuery(), false, ts);
    			        }
    			    });
    			    th.start();
    			}
    		}
    	}
        
    }
}
