package org.dbtoaster.model;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
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

import javax.swing.Timer;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.dbtoaster.gui.DBPerfPanel;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.PlatformUI;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.experimental.chart.swt.ChartComposite;

import DBToaster.Profiler.Protocol.ProfileLocation;
import DBToaster.Profiler.Protocol.SampleType;
import DBToaster.Profiler.Protocol.SampleUnits;
import DBToaster.Profiler.Protocol.StatisticsProfile;
import DBToaster.Profiler.Protocol.Profiler.Client;
import java.net.Socket; 
import java.net.UnknownHostException;
import java.lang.Thread;
import org.eclipse.swt.widgets.Display;

public class Executor
{
    String binaryPath;
    HashMap<String, String> binaryEnv;

    ProcessBuilder queryProcess;
    Process currentQuery;

    public class DBToasterProfiler extends Timer implements ActionListener
    {        
        Client client;
        TTransport transport;
        boolean terminated;
        
        TimeSeries samples;
        ChartComposite chart;
        HashMap<Integer, String> codeLocations;
        LinkedList<Integer> handlerLocations;

        Millisecond currentMs;
        double currentSample;

        static final int profilerPeriod = 1000;

        public DBToasterProfiler(String codeLocationsFile, TProtocol protocol,
                TimeSeries profilerSamples, ChartComposite profilerChart)
        {
            super(profilerPeriod, null);
            reset();
            try {
                transport = protocol.getTransport();
                if ( !transport.isOpen() ) transport.open();
    
                client = new Client(protocol);
                samples = profilerSamples;
                chart = profilerChart;
                
                // Clear previous run.
                chart.getDisplay().asyncExec(new Runnable()
                    { public void run() { samples.clear(); } });

                codeLocations = new HashMap<Integer, String>();
                handlerLocations = new LinkedList<Integer>();
                readCodeLocations(codeLocationsFile);
                addActionListener(this);

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

                        //System.out.println("Found " + newSamples.size() +
                        //    " samples for loc: " + loc.statName + ", " + loc.codeLocation);

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
                            //System.out.println(locAvgExecTime + " " + sampleCount);
                        }

                        sumExecTime += (locAvgExecTime / sampleCount);
                    }
                    else {
                        //String msg = "Could not find cpu profile for location: "
                        //    + codeLocations.get(l) + " " + l;
                        //System.err.println(msg);
                    }
                }

                currentMs = new Millisecond(currentCal.getTime());
                currentSample = sumExecTime * 1e-6;

                chart.getDisplay().asyncExec(new Runnable()
                {
                    public void run()
                    {
                        //System.out.println("Adding sample...");
                        samples.add(currentMs, currentSample);
                        
                        //System.out.println("Redrawing chart!");
                        //chart.forceRedraw();
                    }
                });

            } catch (Exception e) {
                System.out.println("getStatisticsProfile failed!");
                e.printStackTrace();
            }
        }

        public void terminate() { terminated = true; }

        public void actionPerformed(ActionEvent e)
        {
            runOnce();
        }
    };

    class ExecutionLogger extends Thread implements Runnable
    {
        String logFile;
        boolean terminated;

        public ExecutionLogger(String f) { logFile = f; terminated = false; }

        public void run()
        {
            try {
                BufferedReader logReader = new BufferedReader(
                        new InputStreamReader(currentQuery.getInputStream()));
                    
                Writer logWriter = new BufferedWriter(new FileWriter(logFile));
                String line = "";
                while ( ((line = logReader.readLine()) != null) && !terminated )
                    logWriter.write(line + "\n");
    
                logWriter.close();
                logReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        public void terminate() { terminated = true; }
    }

    DBToasterProfiler profiler;
    ExecutionLogger currentLogger;

    // Profiler client
    TBinaryProtocol.Factory profilerProtocolFactory;
    int currentPort;
    String currentHost;

    // Alternative databases
    private final static String POSTGRES_JDBC_DRIVER = "org.postgresql.Driver";
    private final static String POSTGRES_URL = "";

    private final static String HSQLDB_JDBC_DRIVER = "org.hsql.jdbcDriver";
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
        int profilerServicePort, TimeSeries profilerSamples,
        ChartComposite profilerChart)
    {
        // Run query binary.
        String status = null;

        if ( queryProcess != null ) {
            LinkedList<String> args = new LinkedList<String>();
            System.out.println("Running " + binaryPath);
            args.add(binaryPath);
            args.add(Integer.toString(profilerServicePort));
            queryProcess.command(args);

            try {
                queryProcess.redirectErrorStream(true);
                currentPort = profilerServicePort;
                currentHost = "127.0.0.1";
                currentQuery = queryProcess.start();
           
            } catch (IOException e) {
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

            try { 
                
                Thread.sleep(200);
                TSocket s = new TSocket(currentHost, currentPort);
                
                System.out.println("Opening socket....");
                
                s.open();
                if ( !s.isOpen() ) {
                    System.out.println("Failed to connect!!");
                }
                
                System.out.println("Getting protocol....");
                
                TProtocol protocol = profilerProtocolFactory.getProtocol(s);
                
                profiler = new DBToasterProfiler(
                    codeLocationsFile, protocol, profilerSamples, profilerChart);
                profiler.start();

                currentLogger = new ExecutionLogger(execLogFile);
                currentLogger.start();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        return status;
    }

    public DBToasterProfiler getProfiler() { return profiler; }

    void stop()
    {
        if ( profiler != null && profiler.isRunning() )
            profiler.stop();

        if ( currentLogger != null )
            currentLogger.terminate();

        if ( currentQuery != null )
            currentQuery.destroy();

        currentLogger = null;
        profiler = null;
        currentQuery = null;
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
    
    int startupPostgreSQL()
    {
    	int exitVal = 0;
    	try {
    		Runtime rt = Runtime.getRuntime();
    		String str[] = {//"sudo", "-u", "postgres" /*userid*/, 
    				post_path + "bin/pg_ctl", "start", "-w", "-D", post_path + "data"};
    	
    		Process pr = rt.exec(str);
                		
    		exitVal = pr.waitFor();
    		if (exitVal != 0) {
    			System.err.println("Could not start up postgres " + exitVal);
    		}
    		else {
    			System.out.println("Server is up");
    		}
    		
    	} catch (Exception e) {
    		System.err.println(e.toString());
    		e.printStackTrace();
    	}
    	return exitVal;
    }
    
    int shutdownPostgreSQL()
    {
    	int exitVal = 0;
    	try {
    		Runtime rt = Runtime.getRuntime();
    		String str[] = { //"sudo", "-u", "postgres", 
    				post_path + "bin/pg_ctl",
    				"stop", "-D", post_path + "data", "-m", "fast"};
    		Process pr = rt.exec(str);
    		
    		exitVal = pr.waitFor();
    		if (exitVal != 0) {
    			System.err.println("Could not shut down postgres");	
    		}
    	} catch (Exception e) {
    		System.err.println(e.toString());
    		e.printStackTrace();
    	}
    	return exitVal;
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
    			String str[] = {"/Library/PostgreSQL/8.4/bin/psql", "-h", post_server, "-p", post_port, "-U", post_username, 
    					"-c", "copy test from stdin with csv", post_dbname};
    			
    			Process pr = rt.exec(str);
    			
    			BufferedWriter output = new BufferedWriter(new OutputStreamWriter(pr.getOutputStream()));
    			
    			for (int i =0; i < line; i ++) {
    				output.write(buf.get(i)+"\n");
    				System.out.println(buf.get(i));	
    			}
    			output.write("\\.\n");
    			output.flush();
    			System.out.println("!!");
    			int exitVal = pr.waitFor();
    			System.out.println("@@");     
    			if(exitVal != 0) {
    				System.err.println("Error - loading postgres");
    				return -1;
    			}
    		} catch (Exception e) {
    			System.out.println(e.toString());
    			e.printStackTrace();
    		}
    		
    //		for (String t: buf) {
    //			System.out.println("Data "+t);
    //		}
    		System.out.println(line +" tuples are loaded");
    	}
    	return line;
    }

    void runPostgreSQL(String sqlOrTriggerQuery, boolean triggerScript,
            final TimeSeries profilerSamples)
    {
    	final Calendar currentCal = Calendar.getInstance();
    	
    	System.out.println("File init");
    	if(initFile("/Users/mavkisuh/homework/DBToaster/dbtoaster/experiments/vwap/data/20081201.csv") != 0) {
    		return;
    	}
    	
    	System.out.println("start up postgre");
    	if(startupPostgreSQL() != 0) {
    		return;
    	}
    	
    	int howmany = 50;
    	int total = 0;
    	int thres_total = 5000;
    	while (postgreLoad(howmany) == howmany && (total += howmany) <= thres_total) {
    		System.out.println("REading?");
    		try {
    		Thread.sleep(1000);
    		Display.getDefault().asyncExec(new Runnable() {
				public void run() {
					profilerSamples.add(new Millisecond(currentCal.getTime()), 1);
				}
			});
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		/* sleep ? */
    	}
    	System.out.println("loading done");
//        if ( triggerScript )
//            runPGTriggers(sqlOrTriggerQuery);
//        else
//            runPGQuery(sqlOrTriggerQuery, profilerSamples);
        
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
    		Integer[] numdatabases, final String[] dbNames)
    {
    	for (int i = 0 ; i < numdatabases.length; i ++)
    	{
    		if(numdatabases[i] == 1)
    		{
    			DBPerfPanel panel = dbPanels.get(dbNames[i]);
    			ChartComposite chart = panel.getCpuChart();
    			final TimeSeries ts = panel.getCpuTimeSeries();
    			//String[] dbNames = { "DBToaster", "Postgres", "HSQLDB", "DBMS1", "SPE1" };
    			System.out.println("Profiling "+q.getQueryName() + " with "+ dbNames[i]);
    			
    			if(dbNames[i].equals("DBToaster")) {
    				System.out.println("Starting");
    				q.runQuery(20000, ts, chart);
    			}
    			else if(dbNames[i].equals("Postgres")) {
    				Thread th = new Thread (new Runnable() {
    					public void run() {
    						System.out.println("Running Postgres");
    						runPostgreSQL(q.getQuery(), true, ts);
    					/*	final Random r = new Random();
    						while(true) {
    							try {Thread.sleep(1000);} catch (Exception e) {}
    							Display.getDefault().asyncExec(new Runnable() {
    								public void run() {
    									ts.add(new Minute(r.nextInt(), 12, 8, 8, 2009), 10+r.nextInt());
    								}
    							});
    						} */
    					}
    				});
    				th.start();
    			}
    			else if(dbNames[i].equals("HSQLDB")) {
    				
    			}
    		}
    	}
        
    }
}
