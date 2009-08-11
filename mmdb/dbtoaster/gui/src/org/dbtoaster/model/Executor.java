package org.dbtoaster.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;

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
                
                for (Map.Entry<String, Integer> e : runningHandlerMax.entrySet())
                    handlerLocations.add(e.getValue());

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

                        for (SampleUnits s : newSamples)
                        {
                            if ( !(s.getSampleType() == SampleType.EXEC_TIME) ) {
                                System.err.println("Found invalid cpu sample type!");
                                break;
                            }

                            locAvgExecTime += ((s.getExecTime().getTv_sec() * 1e9)
                                + s.getExecTime().getTv_nsec());
                            ++sampleCount;
                        }
                        
                        sumExecTime += (locAvgExecTime / sampleCount);
                    }
                    else {
                        String msg = "Could not find cpu profile for location: "
                            + codeLocations.get(l);
                        System.err.println(msg);
                    }
                }

                samples.add(new Millisecond(currentCal.getTime()), sumExecTime);

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
                
                int rs = currentQuery.waitFor();
                if ( rs != 0 ) status = "Query returned non-zero exit status";
            } catch (IOException e) {
                status = "IOException while running query.";
                e.printStackTrace();
            } catch (InterruptedException e) {
                status = "Query interrputed while running...";
                e.printStackTrace();
            }
        }
        
        currentQuery = null;
        return status;
    }

    
    // TODO: data loading for alternative DBMS

    void runJDBCQuery(String dbUrl, String sqlQuery,
            ConcurrentLinkedQueue<Long> profilerSamples)
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
            profilerSamples.add(span);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

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

    void runPostgreSQL(String sqlOrTriggerQuery, boolean triggerScript,
            ConcurrentLinkedQueue<Long> profilerSamples)
    {
        if ( triggerScript )
            runPGTriggers(sqlOrTriggerQuery);
        else
            runPGQuery(sqlOrTriggerQuery, profilerSamples);
    }
    
    void runPGQuery(String sqlQuery, ConcurrentLinkedQueue<Long> profilerSamples)
    {
        runJDBCQuery(POSTGRES_URL, sqlQuery, profilerSamples);
    }
    
    void runPGTriggers(String triggerScript)
    {
        
    }
    
    void runHsqlDB(String sqlQuery, ConcurrentLinkedQueue<Long> profilerSamples)
    {
        runJDBCQuery(HSQLDB_URL, sqlQuery, profilerSamples);
    }
    
    void runOracle() {}
    
    void runSPE() {}
    
    void runComparison() {
        
    }
}
