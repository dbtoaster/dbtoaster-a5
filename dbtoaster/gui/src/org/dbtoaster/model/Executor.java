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
import java.util.Random;

import javax.swing.Timer;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.dbtoaster.gui.DBPerfPanel;
import org.dbtoaster.io.DBToasterTMLWriter.lastRelationArgs;
import org.dbtoaster.model.DatasetManager.Dataset;
import org.eclipse.swt.widgets.Display;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.experimental.chart.swt.ChartComposite;

import DBToaster.Profiler.Protocol.ProfileLocation;
import DBToaster.Profiler.Protocol.SampleType;
import DBToaster.Profiler.Protocol.SampleUnits;
import DBToaster.Profiler.Protocol.StatisticsProfile;
import DBToaster.Profiler.Protocol.Profiler.Client;
import java.lang.Thread;
import java.sql.*;

public class Executor
{
    public final static String[] dbNames =
        { "DBToaster", "Postgres", "HSQLDB", "DBMS1", "SPE1" };

    DatasetManager datasets;

    String binaryPath;
    HashMap<String, String> binaryEnv;

    ProcessBuilder queryProcess;
    //Process currentQuery;

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

    interface DatabaseRunTask {
        public void runTask();
        public void waitForTask();
        public void terminateTask();
    };
    
    class ComparisonRunner extends Thread
    {
        LinkedList<DatabaseRunTask> tasks;
        DatabaseRunTask currentTask;
        ToasterRunner toastedQuery;
        
        ComparisonRunner(LinkedList<DatabaseRunTask> runQueue)
        {
            tasks = runQueue;
        }
        
        public void run()
        {
            while ( !tasks.isEmpty() ) {
                currentTask = tasks.pop();
                currentTask.runTask();
                currentTask.waitForTask();
            }
        }
        
        public void terminate() 
        {
            if ( currentTask != null ) currentTask.terminateTask();
            tasks.clear();
        }

        void addToastedQuery(ToasterRunner t) {
            if ( !tasks.contains(t) ) tasks.add(t);
            toastedQuery = t;
        }

        ToasterRunner getToastedQuery() { return toastedQuery; }
        
        DBToasterProfiler getProfiler() { 
            return (toastedQuery != null? toastedQuery.getProfiler() : null);
        }
    };

    class DatabaseRunner extends Thread implements DatabaseRunTask
    {
        boolean terminated;

        String dbName;
        String queryText;
        LinkedList<Vector<lastRelationArgs>> queryRelations;
        boolean triggerQuery;
        final TimeSeries profilerSamples;
        long tupleLimit;
        long chunkSize;

        DatabaseRunner(String databaseName, String queryText,
            LinkedList<Vector<lastRelationArgs>> queryRelations,
            boolean triggerQuery, TimeSeries profilerSamples,
            long tupleLimit, long chunkSize)
        {
            dbName = databaseName;
            this.queryText = queryText;
            this.queryRelations = queryRelations;
            this.triggerQuery = triggerQuery;
            this.profilerSamples = profilerSamples;
            this.tupleLimit = tupleLimit;
            this.chunkSize = chunkSize;
            
            terminated = false;
        }

        // Process helpers
        int logAndWaitForProcess(Process p)
        {
            int exitVal = -1;
            try {
                BufferedReader logReader = new BufferedReader(
                    new InputStreamReader(p.getErrorStream()));
                
                String line = "";
                while ( ((line = logReader.readLine()) != null)  )
                    System.out.println(line + "\n");
            
                exitVal = p.waitFor();
                if (exitVal != 0) {
                    System.err.println("Command exited with " + exitVal);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return exitVal;
        }

        // Dataset -> database schema translation
        String getDatabaseRelation(String datasetName, String relationName)
        {
            //return datasetName + "_" + relationName;
            return relationName;
        }

        String getDatabaseType(String dbToasterType)
        {
            String r = null;
            if ( dbToasterType.equals("int") )
                r = "int8";
            else if ( dbToasterType.equals("long") )
                r = "bigint";
            else if ( dbToasterType.equals("float") )
                r = "real";
            else if ( dbToasterType.equals("double") )
                r = "double precision";
            else if ( dbToasterType.equals("string") )
                r = "varchar";
            else {
                System.err.println("Invalid DBToaster type: " + dbToasterType);
            }
            
            return r;
        }
        
        String getHsqldbDatabaseType(String dbToasterType)
        {
            String r = null;
            if ( dbToasterType.equals("int") )
                r = "bigint";
            else if ( dbToasterType.equals("long") )
                r = "bigint";
            else if ( dbToasterType.equals("float") )
                r = "real";
            else if ( dbToasterType.equals("double") )
                r = "double precision";
            else if ( dbToasterType.equals("string") )
                r = "varchar";
            else {
                System.err.println("Invalid DBToaster type: " + dbToasterType);
            }
            
            return r;
        }

        // File multiplexing and random I/O
        class FileMultiplexer
        {
            class FileSource
            {
                public String fileName;
                public String datasetName;
                public String relationName;
                public String dbRelationName;
                public Character delimiter;
            }

            Vector<BufferedReader> fileReaders;
            HashMap<BufferedReader, FileSource> fileSources;
            Random filePicker;
            
            FileMultiplexer() {
                filePicker = new Random();
                fileReaders = new Vector<BufferedReader>();
                fileSources = new HashMap<BufferedReader, FileSource>();
            }
            
            FileMultiplexer(Vector<lastRelationArgs> datasetRelations)
            {
                filePicker = new Random();
                fileReaders = new Vector<BufferedReader>();
                fileSources = new HashMap<BufferedReader, FileSource>();

                for (lastRelationArgs e : datasetRelations)
                {
                    String ds = e.getDataset();
                    String rel = e.getRelName();
                    String loc = datasets.getRelationHandlerInputLocation(ds, rel);
                    Character delimiter = datasets.getSourceDelimiters(ds, rel);

                    if ( loc == null ) {
                        System.err.println(
                            "Unknown file source for dataset " + ds + " relation " + rel);
                    }
                    else {
                        String msg = "Adding " + getDatabaseRelation(ds, rel) + " from " + loc;
                        System.out.println(msg);
                        add(loc, ds, rel, delimiter);
                    }
                }
            }
            
            void add(String fileName, String dataset, String relation, Character delimiter)
            {
                File f = new File(fileName);
                try {
                    BufferedReader r = new BufferedReader(
                        new InputStreamReader(new FileInputStream(f)));
                    fileReaders.add(r);
                    FileSource fs = new FileSource();
                    fs.datasetName = dataset;
                    fs.relationName = relation;
                    fs.dbRelationName = getDatabaseRelation(dataset, relation);
                    fs.delimiter = delimiter;
                    fileSources.put(r, fs);

                } catch (FileNotFoundException e) {
                    
                }
            }
            
            long loadDatabase(String dbName, long count)
            {
                long r = 0;

                int nextFile = filePicker.nextInt(fileReaders.size());
                BufferedReader br = fileReaders.get(nextFile);
                FileSource fs = fileSources.get(br);

                LinkedHashMap<String, String> relFields =
                    datasets.getDataset(fs.datasetName).
                        getRelationFields(fs.relationName);

                Connection conn = null;
                if ( dbJDBCConnections.containsKey(dbName) )
                    conn = dbJDBCConnections.get(dbName);
                else {
                    String msg = "Could not find cached connection for " + dbName;
                    System.err.println(msg);
                    return r;
                }
                    

                if ( dbName.equals(dbNames[1]) ) {
                    r = loadPostgreSQL(br, fs.dbRelationName, count, fs.delimiter);
                }
                else if ( dbName.equals(dbNames[2]) ) {
                    r = loadHsqlDB(conn, br, fs.dbRelationName, count, fs.delimiter);
                }
                else if ( dbName.equals(dbNames[3]) ) {
                    System.err.println("DBMS1 not yet implemented.");
                }
                else if ( dbName.equals(dbNames[4]) ) {
                    System.err.println("SPE1 not yet implemented.");
                }
                
                if ( r != count ) {
                    System.out.println("Only read " + r + " lines, done with file " + fs.fileName);
                    fileReaders.remove(nextFile);
                    fileSources.remove(nextFile);
                    try {
                        br.close();
                    } catch (IOException e) { e.printStackTrace(); }
                }
                return r;
            }
            
            boolean hasData() { return !fileReaders.isEmpty(); }
            
        };

        LinkedList<String> bufferLines(BufferedReader br, long num, char delim)
        {
            LinkedList<String> buf = new LinkedList<String>();
            String tmp = "";
            
            try {
                for (int i =0;i < num;i ++) {
                    tmp = br.readLine();
                    String subtmp = tmp.replace(delim, ',');
                    
                    if(tmp == null) break;
                    buf.add(subtmp);
                }
            } catch (IOException e) {
                System.err.println("IO exception");
                e.printStackTrace();
            }
            
            return buf;
        }

        // Snapshot query execution primitive.
        void runJDBCQuery(String dbName, String sqlQuery,
            final TimeSeries profilerSamples, final long numTuples)
        {
            try {
                final Calendar currentCal = Calendar.getInstance();

                //Connection conn = DriverManager.getConnection(dbUrl, user, passwd);
                Connection conn = null;
                if ( dbJDBCConnections.containsKey(dbName) )
                {
                    conn = dbJDBCConnections.get(dbName);
                    Statement st = conn.createStatement();

                    //System.out.println("Running jdbc cmd: " + sqlQuery);

                    long startTime = System.currentTimeMillis();
                    ResultSet rs = st.executeQuery(sqlQuery);
                    while ( rs.next() ) {}
                    
                    long endTime = System.currentTimeMillis();
                    final long span = endTime - startTime;
        
                    Display.getDefault().asyncExec(new Runnable() {
                        public void run() {
                            profilerSamples.add(new Millisecond(currentCal.getTime()), span / numTuples);
                        }
                    });
                }
                else {
                    System.err.println("Could not find cached connection for " + dbName);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        boolean runJDBCStatement(String dbName, String statement)
        {
            boolean success = true;
            Connection conn = null;
            if ( dbJDBCConnections.containsKey(dbName) ) {
                try {
                    System.out.println("Running jdbc cmd: " + statement);

                    conn = dbJDBCConnections.get(dbName);
                    Statement stmt = conn.createStatement();
                    if ( stmt.execute(statement) ) {
                        System.err.println("WARNING: statement returned a result set.");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    success = false;
                }
            }
            else success = false;

            return success;
        }

        void createDatabaseDataset(Connection conn, String dbName, String dsName, Dataset ds)
        {
            for (String relName : ds.getRelationNames())
            {
                LinkedHashMap<String, String> fields = ds.getRelationFields(relName);
                String dbRelName = getDatabaseRelation(dsName, relName);
                
                if ( dbName.equals(dbNames[1]) )
                    createPostgreSQLTable(conn, dbRelName, fields);

                else if ( dbName.equals(dbNames[2]) )
                    createHsqldbTable(conn, dbRelName, fields);
                
                else if ( dbName.equals(dbNames[3]) )
                    System.err.println("DBMS1 unsupported.");
                
                else if ( dbName.equals(dbNames[4]) )
                    System.err.println("SPE1` unsupported.");
            }
        }
        
        void createPostgreSQLTable(Connection conn, String relation,
            LinkedHashMap<String, String> fields)
        {
            String dropTableStatement = "DROP TABLE IF EXISTS " + relation;
            
            String fieldStr = "";
            
            // TODO: dbtoaster -> database type conversion for other databases
            for (Map.Entry<String, String> e : fields.entrySet()) {
                fieldStr += (fieldStr.isEmpty()? "" : ", ") +
                    e.getKey() + " " + getDatabaseType(e.getValue());
            }

            String createTableStatement = "CREATE TABLE " + relation +
                "(" + fieldStr + ")";

            String currentStmtText = dropTableStatement;
            try {
                Statement stmt = conn.createStatement();
                stmt.execute(dropTableStatement);
                
                currentStmtText = createTableStatement;
                stmt.execute(createTableStatement);
            }
            catch (SQLException e) {
                System.err.println("Failed to create relation " + relation + " in Postgres.");
                System.err.println("Statement: '"+ currentStmtText + "' failed.");
                e.printStackTrace();
            }
        }

        void createHsqldbTable(Connection conn, String relation,
            LinkedHashMap<String, String> fields)
        {
            String checkTableStatement =
                "SELECT * FROM INFORMATION_SCHEMA.SYSTEM_TABLES "+
                "WHERE TABLE_NAME='"+ relation.toUpperCase() + "'";

            String dropTableStatement = "DROP TABLE " + relation;
            
            String fieldStr = "";
            
            // TODO: dbtoaster -> database type conversion for other databases
            for (Map.Entry<String, String> e : fields.entrySet()) {
                fieldStr += (fieldStr.isEmpty()? "" : ", ") +
                    e.getKey() + " " + getHsqldbDatabaseType(e.getValue());
            }

            String createTableStatement = "CREATE TABLE " + relation +
                "(" + fieldStr + ")";

            String currentStmtText = checkTableStatement;
            try {
                Statement stmt = conn.createStatement();
                ResultSet checkRs = stmt.executeQuery(checkTableStatement);
                if ( checkRs.next() ) {
                    System.out.println("Found relation " + relation + ", dropping...");
                    currentStmtText = dropTableStatement;
                    stmt.execute(dropTableStatement);
                    System.out.println("Dropped relation " + relation + ".");
                }

                System.out.print("Creating relation " + relation + "...");
                currentStmtText = createTableStatement;
                stmt.execute(createTableStatement);
                System.out.print("done.");
            }
            catch (SQLException e) {
                System.err.println("Failed to create relation " + relation + " in HsqlDB.");
                System.err.println("Statement : '" + currentStmtText + "' failed.");
                e.printStackTrace();
            }
            
        }

        boolean validateTable(String dbName, String relationName)
        {
            boolean r = false;
            
            Connection dbConn = null;
            if ( dbJDBCConnections.containsKey(dbName) )
                dbConn = dbJDBCConnections.get(dbName);
            else
                return r;

            if ( dbName.equals(dbNames[1]) ) {
                r = validatePostgreSQLTable(dbConn, relationName);
            }
            else if ( dbName.equals(dbNames[2]) ) {
                r = validateHsqldbTable(dbConn, relationName);
            }
            else if ( dbName.equals(dbNames[3]) ) {
                System.err.println("DBMS1 not yet implemented.");
            }
            else if ( dbName.equals(dbNames[4]) ) {
                System.err.println("SPE1 not yet implemented.");
            }
            
            return r;
        }

        boolean validatePostgreSQLTable(Connection dbConn, String relationName)
        {
            boolean valid = false;
            String checkQuery =
                "select * from pg_tables where tablename = '" + relationName + "'";
            
            try {
                Statement checkStmt = dbConn.createStatement();
                ResultSet rs = checkStmt.executeQuery(checkQuery);
                valid = rs.next();
            } catch (SQLException e) { e.printStackTrace(); }
            
            return valid;
        }

        boolean validateHsqldbTable(Connection dbConn, String relationName)
        {
            boolean valid = false;
            String checkQuery =
                "SELECT * FROM INFORMATION_SCHEMA.SYSTEM_TABLES "+
                "WHERE TABLE_NAME = '" + relationName.toUpperCase() + "'";
            
            try {
                Statement checkStmt = dbConn.createStatement();
                ResultSet rs = checkStmt.executeQuery(checkQuery);
                valid = rs.next();
            } catch (SQLException e) { e.printStackTrace(); }
            
            return valid;
        }

        void validateDatabase(String dbName)
        {
            if ( datasets != null )
            {
                Connection conn = null;
                if ( dbJDBCConnections.containsKey(dbName) )
                    conn = dbJDBCConnections.get(dbName);
                else {
                    String msg = "Could not find cached connection for validating " + dbName;
                    System.err.println(msg);
                    return;
                }

                for (String dsName : datasets.getDatasetNames())
                {
                    Dataset ds = datasets.getDataset(dsName);
                    boolean datasetValid = true;
                    for (String relName : ds.getRelationNames())
                    {
                        String dbRel = getDatabaseRelation(dsName, relName); 
                        datasetValid = datasetValid && validateTable(dbName, dbRel);
                        if ( !datasetValid ) {
                            System.out.println("Invalid dataset rel " +
                                dsName + " " + relName + " " + dbRel);
                            break;
                        }
                    }
                    
                    System.out.println("Database " + dbName + " dataset: " +
                        dsName + " " + (datasetValid? "valid" : "invalid"));

                    if ( !datasetValid ) {
                        System.out.print(
                            "Creating dataset " + dsName + " in " + dbName + " ... ");
                        createDatabaseDataset(conn, dbName, dsName, ds);
                        System.out.println("done.");
                    }
                }
            }
            else {
                System.err.println("Missing datasets to validate against " + dbName);
            }
        }

        boolean startupDatabase(String dbName)
        {
            boolean r = false;
            if ( dbName.equals(dbNames[1]) ) {
                r = startupPostgreSQL();
            }
            else if ( dbName.equals(dbNames[2]) ) {
                r = startupHsqlDB();
            }
            else if ( dbName.equals(dbNames[3]) ) {
                System.err.println("DBMS1 not yet implemented.");
            }
            else if ( dbName.equals(dbNames[4]) ) {
                System.err.println("SPE1 not yet implemented.");
            }
           
            System.out.print("Validating database " + dbName + " ... ");
            validateDatabase(dbName);
            System.out.println("done.");
            
            return r;
        }
        
        boolean setupDatabaseTriggers(String dbName, String query)
        {
            boolean r = false;
            if ( dbName.equals(dbNames[1]) ) {
                r = setupPostgreSQLTriggers(query);
            }
            else if ( dbName.equals(dbNames[2]) ) {
                r = setupHsqldbTriggers(query);
            }
            else if ( dbName.equals(dbNames[3]) ) {
                System.err.println("DBMS1 not yet implemented.");
            }
            else if ( dbName.equals(dbNames[4]) ) {
                System.err.println("SPE1 not yet implemented.");
            }
            
            return r;
        }
        
        void runDatabase(String dbName, String query,
            TimeSeries profilerSamples, long numTuples)
        {
            if ( dbJDBCConnections.containsKey(dbName) )
                runJDBCQuery(dbName, query, profilerSamples, numTuples);
            else {
                System.err.println("Cannot query " + dbName + ", no connection found.");
            }
        }
        
        void shutdownDatabase(String dbName)
        {
            if ( dbName.equals(dbNames[1]) ) {
                shutdownPostgreSQL();
            }
            else if ( dbName.equals(dbNames[2]) ) {
                shutdownHsqlDB();
            }
            else if ( dbName.equals(dbNames[3]) ) {
                System.err.println("DBMS1 not yet implemented.");
            }
            else if ( dbName.equals(dbNames[4]) ) {
                System.err.println("SPE1 not yet implemented.");
            }
        }

        String runSnapshotQueries(String databaseName, String queryText,
            LinkedList<Vector<lastRelationArgs>> queryRelations,
            boolean triggerQuery, final TimeSeries profilerSamples,
            long tupleLimit, long chunkSize)
        {
            String status = null;

            System.out.println("Running iterated snapshot query for " +
                databaseName + " query: '" + queryText + "'");
            
            for (Vector<lastRelationArgs> qr : queryRelations) {
                for (lastRelationArgs r : qr) {
                    System.out.println(
                        "Dataset relation: " + r.getDataset() + " " + r.getRelName());
                }
            }
            
            System.out.print("Creating multiplexer... ");

            FileMultiplexer sources = new FileMultiplexer(queryRelations.getFirst());

            System.out.println("done.");

            System.out.print("Starting database " + databaseName + " ... ");

            if ( !startupDatabase(databaseName) ) {
                System.out.println("Failed to start database " + databaseName);
                status = "Failed to start " + databaseName;
                return status;
            }

            System.out.print("done.");
            
            long triggerStart = System.currentTimeMillis();
            if ( triggerQuery ) {
                
                System.out.print("Setting up triggers ... ");

                if ( !setupDatabaseTriggers(databaseName, queryText) ) {
                    status = "Failed to load triggers on " + databaseName;
                    System.out.println(status);
                    return status;
                }
                
                System.out.print("done.");
            }

            long total = 0;
            long limit = tupleLimit > 0? tupleLimit : Long.MAX_VALUE;
            
            System.out.println("Starting chunk loop, limit: " + limit +
                ", chunk size " + chunkSize);
            
            while ( sources.hasData() && total < limit && !terminated )
            {
                
                System.out.print("Loading database, total before " + total + " ... ");
                sources.loadDatabase(databaseName, chunkSize);
                total += chunkSize;
                
                System.out.println(" done, new total: " + total);

                if ( !triggerQuery ) {
                    System.out.print("Running database " + databaseName + " on chunk ... ");
                    runDatabase(databaseName, queryText, profilerSamples, total);
                    System.out.println(" done.");
                }
                else {
                    long triggerEnd = System.currentTimeMillis();
                    final long span = triggerEnd - triggerStart;
                    
                    System.out.println("Time triggers, span: " + span);
                    
                    final Calendar currentCal = Calendar.getInstance();
                    Display.getDefault().asyncExec(new Runnable() {
                        public void run() {
                            profilerSamples.add(new Millisecond(currentCal.getTime()), span);
                        }
                    });

                    triggerStart = triggerEnd;
                }
                
                System.out.println("Next loop test: " + sources.hasData() +
                    ", total " + total + " limit " + limit +
                    " continue: " + (total < limit));
            }

            System.out.print("Shutting down database " + databaseName + " ... ");
            shutdownDatabase(databaseName);
            System.out.println(" done.");
            
            return null;
        }

        private String post_username = "postgres";
        private String post_path = "/Users/yanif/software/postgres/";
        private String post_server = "localhost";
        private String post_port = "5432";
        private String post_dbname = "postgres";
        private String post_passwd = "password";

        boolean startupPostgreSQL()
        {
            int exitVal = 0;
            try {
                Runtime rt = Runtime.getRuntime();
                String str[] = {//"sudo", "-u", "postgres" /*userid*/, 
                    post_path + "bin/pg_ctl", "start", "-w",
                        "-D", post_path + "data", "-l", "postgres.log"};
            
                Process pr = rt.exec(str);
                exitVal = logAndWaitForProcess(pr);
                
                if ( exitVal != 0 ) {
                    System.err.println("Could not start postgres.");
                }
                else
                {
                    // Cache a connection
                    try {
                        String dbUrl = POSTGRES_URL;
                        String user = post_username;
                        String passwd = post_passwd;
                        
                        System.out.print("Caching connection for " +
                            dbNames[1] + " url: " + dbUrl + " user: " + user +
                            " pass: " + passwd + " ... ");

                        Connection conn = DriverManager.getConnection(dbUrl, user, passwd);
                        dbJDBCConnections.put(dbNames[1], conn);
                        
                        System.out.println(" done.");
                    } catch (SQLException e) { e.printStackTrace(); }
                }
            } catch (Exception e) {
                System.err.println(e.toString());
                e.printStackTrace();
            }
            return exitVal == 0;
        }
        
        boolean shutdownPostgreSQL()
        {
            int exitVal = 0;
            try {
                Runtime rt = Runtime.getRuntime();
                String str[] = { //"sudo", "-u", "postgres", 
                        post_path + "bin/pg_ctl",
                        "stop", "-D", post_path + "data", "-m", "fast"};
                Process pr = rt.exec(str);
                
                exitVal = logAndWaitForProcess(pr);

                if (exitVal != 0) {
                    System.err.println("Could not shut down postgres"); 
                }
            } catch (Exception e) {
                System.err.println(e.toString());
                e.printStackTrace();
            }

            return (exitVal == 0);
        }

        long loadPostgreSQL(BufferedReader br, String relation, long num, char delim)
        {
            LinkedList<String> buf = bufferLines(br, num, delim);
            long line = buf.size();
            
            if (line != 0) {
                
                System.out.println("Read " + line + " lines.");
                
                try {
                    Runtime rt = Runtime.getRuntime();
                    String str[] = {
                        "/Users/yanif/software/postgres/bin/psql",
                        "-h", post_server, "-p", post_port,
                        "-U", post_username, "-d", post_dbname,
                            "-c", "copy " + relation + " from stdin with csv"};
                    
                    String copyCmd = "";
                    for (String s : str) copyCmd += (copyCmd.isEmpty()? "" : " ") + s;

                    System.out.print("Execing " + dbNames[1] + " copy ... " + copyCmd);
                    Process pr = rt.exec(str);
                    System.out.println("done.");
                    
                    System.out.print("Writing lines to stdin ... ");
                    BufferedWriter output =
                        new BufferedWriter(new OutputStreamWriter(pr.getOutputStream()));
                    
                    for (String s : buf) {
                        output.write(s+"\n");
                    }
                    output.write("\\.\n");
                    output.flush();
                    
                    int exitVal = pr.waitFor();
                    if(exitVal != 0) {
                        System.out.println("Error loading postgres, return val " + exitVal);
                        return -1;
                    } else {
                        System.out.println("done.");
                    }

                } catch (Exception e) {
                    System.err.println(e.toString());
                    e.printStackTrace();
                }
            }
            return line;
        }

        boolean setupPostgreSQLTriggers(String triggerQuery)
        {
            return runJDBCStatement(dbNames[1], triggerQuery);
        }

        // Note: change the HSQLDB_URL if changing these.
        private String hsql_libpath = "/Users/yanif/workspace/dbtoaster-gui/lib/";
        private String hsql_dbname = "db0/db0";
        private String hsql_username = "sa";
        private String hsql_server = "localhost";    
        
        boolean startupHsqlDB()
        {
            boolean r = true;
            try {
                String str[] = {"java", "-cp", hsql_libpath + "hsqldb.jar",
                        "org.hsqldb.Server", "-address", hsql_server,
                        "-database.0", "file:" + hsql_dbname, "-dbname.0", "dbtoaster"};
                Runtime rt = Runtime.getRuntime();
                Process pr = rt.exec(str);
                
                String cmd = "";
                for (String s : str) cmd += (s.isEmpty()? "" : " ") + s;
                
                System.out.println("Running cmd: " + cmd);
                
                // Cache a connection
                Thread.sleep(1000);
                String dbUrl = HSQLDB_URL;
                String user = hsql_username;
                String passwd = "";
                
                System.out.print("Caching connection for " +
                    dbNames[2] + " url: " + dbUrl + " user: " + user +
                    " pass: " + passwd + " ... ");

                Connection conn = DriverManager.getConnection(dbUrl, user, passwd);
                dbJDBCConnections.put(dbNames[2], conn);
                
                System.out.println(" done.");
            }
            catch (Exception e) {
                r = false;
                System.out.println(e.toString());
                e.printStackTrace();
            }
            
            return r;
        }
        
        boolean shutdownHsqlDB() 
        {
            int exitVal = -1;
            try {
                String str[] = {
                    "java", "-jar", hsql_libpath + "hsqldb.jar",
                        "--inlineRc", "URL=" + HSQLDB_URL +
                            ",USER=" + hsql_username + ",PASSWORD=" +
                        "--sql", "\"shutdown;\"" };
                
                String cmd = "";
                for (String s : str) cmd += (cmd.isEmpty()? "" : " ") + s;
                System.out.println("Executing shutdown cmd: " + cmd);
                
                Runtime rt = Runtime.getRuntime();
                Process pr = rt.exec(str);
                
                exitVal = logAndWaitForProcess(pr);
                if ( exitVal != 0 ) {
                    System.err.println("Could not shut down HSQLDB.");
                }
            } catch (Exception e) {
                System.out.println(e.toString());
                e.printStackTrace();
            }
            
            return (exitVal == 0);
        }
        
        long loadHsqlDB(Connection conn, BufferedReader br, String relationName, long num, char delim)
        {
            System.out.print("Buffering lines ...");
            LinkedList<String> buf = bufferLines(br, num, delim);
            System.out.println("done, found " + buf.size() + " lines.");
            
            long line = buf.size();
            if ( line != 0 ) {
                long counter = 0;
                try {
                    Statement stmt = conn.createStatement();
                    for (String valStr : buf)
                    {
                        String insertStmt =
                            "insert into " + relationName + " values(" + valStr + ")";
                        stmt.executeUpdate(insertStmt);
                        ++counter;
                    }
                } catch (Exception e) {
                    String msg = "Failed insert on line " + counter;
                    System.err.println(msg);
                    e.printStackTrace();
                }
            }
            
            return line;
        }
        
        boolean setupHsqldbTriggers(String triggerQuery)
        {
            return runJDBCStatement(dbNames[2], triggerQuery);
        }  
    
        // Thread methods.
        public void run()
        {
            System.out.println("Running " + dbName);
            runSnapshotQueries(dbName, queryText, queryRelations, false,
                profilerSamples, 0, chunkSize);
        }
    
        public void terminate() { terminated = true; }
        
        // DatabaseRunTask interface
        public void runTask() { start(); }

        public void waitForTask()
        {
            try { join(); }
            catch (InterruptedException e) { e.printStackTrace(); }
        }

        public void terminateTask() { terminate(); }
    }

    class ToasterRunner implements DatabaseRunTask
    {
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

        ToasterRunner(ProcessBuilder pb,
            String codeLocationsFile, String execLogFile,
            TimeSeries profilerSamples, ChartComposite profilerChart)
        {
            currentBuilder = pb;
            this.codeLocationsFile = codeLocationsFile;
            this.execLogFile = execLogFile;
            this.profilerSamples = profilerSamples;
            this.profilerChart = profilerChart;
        }

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
        
        DBToasterProfiler getProfiler() { return profiler; }

        ProcessBuilder currentBuilder;
        Process currentQuery;
        
        String codeLocationsFile;
        String execLogFile;
        TimeSeries profilerSamples;
        ChartComposite profilerChart;

        DBToasterProfiler profiler;
        ExecutionLogger currentLogger;
        DatabaseRunner currentDBRunner;
        
        public void runTask()
        {
            try {
                currentQuery = currentBuilder.start();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            // Connect debugger client.
            if ( currentQuery != null )
            {
                System.out.println("DYLD_LIBRARY_PATH: " + System.getenv("DYLD_LIBRARY_PATH"));
                System.out.println("Started query process...");

                // Periodically retrieve statistics while binary is still running.

                try { 
                    
                    Thread.sleep(10000);
                    TSocket s = new TSocket(currentHost, currentPort);
                    
                    System.out.println("Opening socket.... " + currentHost + ":" + currentPort);
                    
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
        }

        public void terminateTask() { stop(); }

        public void waitForTask()
        {
            if ( currentQuery != null ) {
                try {
                    currentQuery.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    ComparisonRunner currentComparison;

    // Profiler client
    TBinaryProtocol.Factory profilerProtocolFactory;
    int currentPort;
    String currentHost;

    // Alternative databases
    private final static String POSTGRES_JDBC_DRIVER = "org.postgresql.Driver";
    private final static String POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres";
    
    private final static String POSTGRES_SETUP_SCRIPT =
        "/Users/yanif/workspace/dbtoaster-gui/scripts/setup-postgresql.sql";

    private final static String HSQLDB_JDBC_DRIVER = "org.hsqldb.jdbcDriver";
    private final static String HSQLDB_URL = "jdbc:hsqldb:hsql://localhost:9001/dbtoaster";

    private final static String HSQLDB_SETUP_SCRIPT =
        "/Users/yanif/workspace/dbtoaster-gui/scripts/setup-hsqldb.sql";

    // JDBC connection caching
    HashMap<String, Connection> dbJDBCConnections;

    public Executor() {
    	for (String dbName : dbNames) initDatabase(dbName);
    }

    public Executor(String engineBinary, DatasetManager datasets)
    {
        this.datasets = datasets;

        File binaryFile = new File(engineBinary);
        if ( binaryFile.exists() && binaryFile.isFile() && binaryFile.canExecute() )
        {
            binaryPath = engineBinary;
            binaryEnv = new HashMap<String, String>();
            binaryEnv.putAll(System.getenv());

            queryProcess = new ProcessBuilder(binaryFile.getAbsoluteFile().toString());
            currentComparison = null;
            
            profilerProtocolFactory = new TBinaryProtocol.Factory();
            
            for (String dbName : dbNames) initDatabase(dbName);
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

            queryProcess.redirectErrorStream(true);
            currentPort = profilerServicePort;
            currentHost = "127.0.0.1";
        }
        
        if ( status != null ) return status;

        ToasterRunner t = new ToasterRunner(queryProcess,
            codeLocationsFile, execLogFile, profilerSamples, profilerChart);

        System.out.println("Adding toasted query.");
        currentComparison.addToastedQuery(t);

        return status;
    }

    public DBToasterProfiler getProfiler() {
        return (currentComparison != null?
            currentComparison.getProfiler() : null);
    }

    void stop()
    {
        if ( currentComparison != null )
            currentComparison.getToastedQuery().stop();
    }

    void initDatabase(String dbName)
    {
        dbJDBCConnections = new HashMap<String, Connection>();

        if ( dbName.equals(dbNames[1]) ) {
            initPostgreSQL();
        }
        else if ( dbName.equals(dbNames[2]) ) {
            initHsqlDB();
        }
        else if ( dbName.equals(dbNames[3]) ) {
            System.err.println("DBMS1 not yet implemented.");
        }
        else if ( dbName.equals(dbNames[4]) ) {
            System.err.println("SPE1 not yet implemented.");
        }
    }

    void initPostgreSQL()
    {
        // Set up JDBC driver. 
        System.out.println("initPostgreSQL");
        try {
            Class.forName(POSTGRES_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find Postgres JDBC driver!");
            e.printStackTrace();
        }
    }

    void initHsqlDB()
    {
        // Set up JDBC driver. 
        System.out.println("initHsqlDB");
        try {
            Class.forName(HSQLDB_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find HSQLDB JDBC driver!");
            e.printStackTrace();
        }
    }
    

    /*
    private String oracle_libpath = "";
    private String oracle_dbname = "";
    private String oracle_username = "";
    private String oracle_server = "";

    void initOracle() {}
    
    boolean startupOracle() {}
    
    boolean shutdownOracle() {}
    
    long loadOracle(Connection conn, BufferReader br, String relationName, long num)
    {
        
    }
    
    boolean setupOracleTriggers(String triggerQuery)
    {
        
    }
    */

    void runSPE() {}
    
    public void runComparison(
        final Query q, LinkedHashMap<String, DBPerfPanel> dbPanels, 
		Integer[] numdatabases, String[] dbNames)
    {
        LinkedList<DatabaseRunTask> comparisonTasks =
            new LinkedList<DatabaseRunTask>();

        if ( currentComparison == null )
            currentComparison = new ComparisonRunner(comparisonTasks);

    	for (int i = 0 ; i < numdatabases.length; i ++)
    	{
    	    final String dbName = dbNames[i];

    		if(numdatabases[i] == 1)
    		{
    			DBPerfPanel panel = dbPanels.get(dbNames[i]);
    			ChartComposite chart = panel.getCpuChart();
    			final TimeSeries ts = panel.getCpuTimeSeries();

    			System.out.println("Profiling "+q.getQueryName() + " with "+ dbNames[i]);
    			
    			if(dbNames[i].equals("DBToaster")) {
    			    // Note: runQuery ends up calling this.run() which adds
    			    // to currentComparison
    				q.runQuery(20000, ts, chart);
    			}
    			else if( dbNames[i].equals("Postgres") ||
    			            dbNames[i].equals("HSQLDB") )
    			{
    			    final long chunkSize = 50;
    			    DatabaseRunner r = new DatabaseRunner(dbName, q.getQuery(),
    			        q.getQueryRelations(), false, ts, 0, chunkSize);
    				comparisonTasks.add(r);
    			}
    		}
    	}
    	
    	currentComparison.start();
    }

    public void stopComparison()
    {
        if ( currentComparison != null ) {
            currentComparison.terminate();
            currentComparison = null;
        }
    }
}
