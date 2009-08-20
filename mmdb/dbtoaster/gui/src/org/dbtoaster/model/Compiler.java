package org.dbtoaster.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.dbtoaster.io.DBToasterSourceConfigWriter;
import org.dbtoaster.io.DBToasterTMLWriter;

import org.eclipse.datatools.modelbase.sql.query.QuerySelectStatement;
import org.eclipse.datatools.modelbase.sql.query.QueryStatement;
import org.eclipse.datatools.modelbase.sql.query.helper.StatementHelper;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParseErrorInfo;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserException;
import org.eclipse.datatools.sqltools.parsers.sql.SQLParserInternalException;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParseResult;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManager;
import org.eclipse.datatools.sqltools.parsers.sql.query.SQLQueryParserManagerProvider;

public class Compiler
{
    public enum CompileMode { HANDLERS, ENGINE, DEBUGGER };

    private final static String DEFAULT_COMPILER_BINARY = "dbtoaster.byte";
    private final static String DEFAULT_COMPILER_PATH =
        "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2";

    // C++ profiler interface
    final static String PROFILER_THRIFT_MODULE =
        "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/profiler/profiler.thrift";

    final static String PROFILER_THRIFT_MODULE_BASE =
        "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/profiler/protocol";
    
    // C++ demo datasets interface
    final static String DATASET_THRIFT_MODULE =
        "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/examples/datasets/datasets.thrift";

    final static String DATASET_THRIFT_MODULE_BASE =
        "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/examples/datasets/protocol";

    // Java profiler client
    final static String PROFILER_JAR_FILE=
        "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/profiler/profiler.jar";
    
    // Java demo datasets classes
    final static String DATASET_JAR_FILE=
        "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/examples/datasets/datasets.jar";

    String dbToasterPath;
    ProcessBuilder dbToasterProcess;
    Process currentToaster;

    DBToasterTMLWriter tmlWriter;
    DBToasterSourceConfigWriter sourceConfigWriter;

    public Compiler(DatasetManager datasets)
    {
        dbToasterPath = findDBToaster();
        if ( dbToasterPath == null ) {
            System.out.println("Could not find DBToaster!");
            System.exit(1);
        }
        else {
            System.out.println("Using DBToaster at: " + dbToasterPath);
        }

        dbToasterProcess = new ProcessBuilder(dbToasterPath);
        currentToaster = null;
        tmlWriter = new DBToasterTMLWriter(datasets);
        sourceConfigWriter = new DBToasterSourceConfigWriter(datasets);
    }

    private String findDBToaster()
    {
        LinkedList<String> testPaths = new LinkedList<String>();
        testPaths.add(DEFAULT_COMPILER_PATH);
        
        String pathEnvVar = System.getenv("PATH");
        String[] binPaths = pathEnvVar.split(":");
        testPaths.addAll(Arrays.asList(binPaths));
        
        String r = null;
        for (String path : testPaths)
        {
            File testPath = new File(path, DEFAULT_COMPILER_BINARY);
            if ( testPath.exists() && testPath.isFile() && testPath.canExecute() )
            {
                r = testPath.getAbsolutePath();
                break;
            }
        }
        
        return r;
    }
    
    ////////////////////////////////////////
    //
    // DBToaster library configuration
    // -- TODO: push this down into DBToaster itself.
    // -- need to find nice tool to do library configuration, ideally something
    //    with a caml interface and something simpler than autoconf/automake

    private String getSiblingDir(String dir, String siblingBase)
    {
        String r = null;
        File dirPath = new File(dir);
        File dirParent = (dirPath == null? null : dirPath.getParentFile());
        if ( dirParent != null ) {
            File siblingDirPath = new File(dirParent, siblingBase);
            if ( siblingDirPath.exists() && siblingDirPath.isDirectory() )
                r = siblingDirPath.getAbsolutePath();
            else {
                // Recur upwards.
                r = getSiblingDir(dirParent.getAbsolutePath(), siblingBase);
            }
        }
        
        System.out.println("getSiblingDir(" + dir + "," + siblingBase + "): "
                + dirPath + ", " + dirParent + ", " + r);
            
        return r;
    }

    private boolean checkInclude(String dir, String testFile)
    {
        File testDirPath = new File(dir);
        File testFilePath = new File(dir, testFile);
        
        System.out.println("checkInclude: " +
                testDirPath.toString() + ", " + testFilePath.toString());
        
        return ( testDirPath.exists() && testDirPath.isDirectory() &&
            testFilePath.exists() && testFilePath.isFile() );
    }
    
    private String checkSystemIncludePaths(LinkedList<String> additionalPaths, String testFile)
    {
        LinkedList<String> fileTestPaths = new LinkedList<String>();
        if ( additionalPaths != null) fileTestPaths.addAll(additionalPaths);

        String pathEnvVar = System.getenv("PATH");
        
        System.out.println("Found $PATH: " + pathEnvVar);
        
        String[] testPaths = pathEnvVar.split(":");
        fileTestPaths.addAll(Arrays.asList(testPaths));
        
        String r = null;
        for (String p : fileTestPaths)
        {
            String dirToCheck = p;
            if ( checkInclude(dirToCheck, testFile) ) {
                r = dirToCheck; break;
            }

            dirToCheck = getSiblingDir(p, "include"); 
            if ( dirToCheck != null && checkInclude(dirToCheck, testFile) ) {
                r = dirToCheck; break;
            }
        }
        
        return r;
    }

    private void addOption(LinkedHashMap<String, LinkedList<String>> options,
            String optKey, String optVal)
    {
        if ( options.containsKey(optKey) )
            options.get(optKey).add(optVal);
        else {
            LinkedList<String> newVals = new LinkedList<String>();
            newVals.add(optVal);
            options.put(optKey, newVals);
        }  
    }

    // Boost configuration.
    private String findBoostPath()
    {
        LinkedList<String> ap = new LinkedList<String>();
        //File homeDir = new File(System.getenv("HOME"));
        //if ( homeDir.exists() ) {
            String p = "/opt/local/include/boost-1_35/";
            ap.add(new File(p).getAbsolutePath());
        //}

        return checkSystemIncludePaths(ap, "boost/smart_ptr.hpp");
    }

    // TODO: check for individual boost library features
    private void buildBoostOptions(LinkedHashMap<String, LinkedList<String>> options)
    {
        System.out.println("Building boost options...");

        String boostIncPath = findBoostPath();
        String boostLibPath = null;
        
        System.out.println("Found boost path: " + boostIncPath);

        if ( boostIncPath != null )
            boostLibPath = getSiblingDir(boostIncPath, "lib");
        
        if ( !(boostIncPath == null || boostLibPath == null) )
        {
            // Flags for query sources.
            addOption(options, "-cI", boostIncPath);
            addOption(options, "-cL", boostLibPath);

            // TODO: add individual boost libraries.
            addOption(options, "-cl", "boost_thread-mt");

            // Flags for thrift sources.
            addOption(options, "-tCI", boostIncPath);
        }
        else {
            System.out.println(
                "Failed to find Boost, attempting to compile anyway.");
        }
    }
    
    // Thrift configuration
    private String findThriftPath()
    {
        LinkedList<String> ap = new LinkedList<String>();
        //File homeDir = new File(System.getenv("HOME"));
        //if ( homeDir.exists() ) {
            String p = "/usr/local/include/thrift";
            ap.add(new File(p).getAbsolutePath());
        //}

        return checkSystemIncludePaths(ap, "Thrift.h");
    }
    
    private void buildThriftOptions(LinkedHashMap<String, LinkedList<String>> options)
    {
        System.out.println("Building Thrift options.");

        String thriftIncPath = findThriftPath();
        String thriftLibPath = null;
        
        System.out.println("Found thrift path: " + thriftIncPath);

        if ( thriftIncPath != null )
            thriftLibPath = getSiblingDir(thriftIncPath, "lib");

        if  ( !(thriftIncPath == null || thriftLibPath == null) )
        {
            // Flags for query sources.
            addOption(options, "-cI", thriftIncPath);
            addOption(options, "-cL", thriftLibPath);
            addOption(options, "-cl", "thrift");
            
            // Flags for Thrift sources
            addOption(options, "-tCI", thriftIncPath);
        }
        else {
            System.out.println(
                "Failed to find Thrift, attempting to compile anyway.");
        }
    }
    
    private String getCompileMode(CompileMode m)
    {
        String r = null;
        switch(m) {
        case HANDLERS:
            r = "handlers";
            break;
        case DEBUGGER:
            r = "debugger";
            break;
        case ENGINE:
        default:
            r = "engine";
            break;
        }
        return r;
    }

    // Compile only
    public String toastQuery(String tmlFile, String sourceConfigFile, String outputFile,
            CompileMode compileMode, String compilationDir, String compilerLogFile)
    {
        System.out.println("Toasting query from TML: " + tmlFile);

        String returnStatus = null;

        // Invoke compiler on TML through ProcessBuilder
        LinkedHashMap<String, LinkedList<String>> options =
            new LinkedHashMap<String, LinkedList<String>>();
        addOption(options, "-thrift" , "/usr/local/bin/thrift");
        addOption(options, "-o", outputFile);
        addOption(options, "-m", getCompileMode(compileMode));
        addOption(options, "-d", sourceConfigFile);
        
        // TODO: move these to a config/build file.
        addOption(options, "-cI", "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/profiler");
        addOption(options, "-cI", "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/standalone");
        addOption(options, "-cI", "/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/examples");

        buildBoostOptions(options);
        buildThriftOptions(options);

        // Additional thrift options
        addOption(options, "-tI",
            "I,/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/examples/datasets");
        addOption(options, "-tI",
            "I,/Users/mavkisuh/homework/DBToaster/original/compiler/alpha2/profiler");

        switch (compileMode) {
        case ENGINE:
            addOption(options, "-tm",
                    PROFILER_THRIFT_MODULE+","+PROFILER_THRIFT_MODULE_BASE);
            break;
        case DEBUGGER:
            addOption(options, "-tm",
                    PROFILER_THRIFT_MODULE+","+PROFILER_THRIFT_MODULE_BASE);
            addOption(options, "-tm",
                    DATASET_THRIFT_MODULE+","+DATASET_THRIFT_MODULE_BASE);
            
            addOption(options, "-tcp", "/usr/local/lib/libthrift.jar");
            addOption(options, "-tcp", "/Users/mavkisuh/Documents/workspace/dbtoaster-gui/lib/log4j-1.2.15.jar");
            addOption(options, "-tcp", PROFILER_JAR_FILE);
            addOption(options, "-tcp", DATASET_JAR_FILE);
            break;
        default:
            break;
        }
        
        System.out.println("Finished building DBToaster options...");

        returnStatus = runDBToaster(
            compilationDir, options, tmlFile, compilerLogFile);

        return returnStatus;
    }

    // Parse and compile.
    // Note: tmlFile, outputFile are absolute paths on the local filesystem.
    public String toastQuery(String sqlQuery,
            String tmlFile, String sourceConfigFile, String outputFile,
            CompileMode compileMode, String compilationDir, String compilerLogFile)
    {
        String returnStatus = null;

        try
        {
            // Parse SQL
            SQLQueryParserManager parserManager = SQLQueryParserManagerProvider
                    .getInstance().getParserManager(null, null);

            SQLQueryParseResult parseResult = parserManager
                    .parseQuery(sqlQuery);

            QueryStatement userQuery = parseResult.getQueryStatement();

            // Write out TML.
            String parsedSQL = userQuery.getSQL();
            System.out.println("Creating map expression for: " + parsedSQL);

            switch (StatementHelper.getStatementType(userQuery)) {
            case StatementHelper.STATEMENT_TYPE_FULLSELECT:
                returnStatus = "Unsuported: FULLSELECT query.";
                break;

            case StatementHelper.STATEMENT_TYPE_SELECT:
                QuerySelectStatement select = (QuerySelectStatement) userQuery;

                System.out.println("Creating TML...");
                String queryTml = tmlWriter.createSelectStatementTreeML(select);

                System.out.println("Creating source config...");
                String sourceConfig = sourceConfigWriter.getSourceConfiguration(
                    tmlWriter.getRelationsUsedFromParsing());

                try
                {
                    System.out.println("Writing TML to " + tmlFile);
                    Writer tmlOut = new BufferedWriter(new FileWriter(tmlFile));
                    tmlOut.write(queryTml);
                    tmlOut.close();

                    System.out.println("Writing source config to " + sourceConfigFile);
                    Writer scOut = new BufferedWriter(new FileWriter(sourceConfigFile));
                    scOut.write(sourceConfig);
                    scOut.close();

                } catch (IOException e)
                {
                    returnStatus = "Compilation failed: " +
                        "IOException in writing TML/source config.";
                    e.printStackTrace();
                }
                break;

            default:
                returnStatus = "Invalid query.";
                break;
            }

        } catch (SQLParserException spe)
        {
            // handle the syntax error
            System.out.println(spe.getMessage());

            returnStatus = "Parser error at (line,col): ";
            String errorLocations = "";

            List<?> syntacticErrors = spe.getErrorInfoList();
            Iterator<?> itr = syntacticErrors.iterator();
            while (itr.hasNext())
            {
                SQLParseErrorInfo errorInfo = (SQLParseErrorInfo) itr.next();
                // Example usage of the SQLParseErrorInfo object
                // the error message
                //String errorMessage = errorInfo.getParserErrorMessage();
                
                // the line numbers of error
                int errorLine = errorInfo.getLineNumberStart();
                int errorColumn = errorInfo.getColumnNumberStart();
                errorLocations += ((errorLocations.isEmpty() ? "" : ", ") + "("
                        + errorLine + ", " + errorColumn + ")");
            }

            returnStatus += errorLocations;
        } catch (SQLParserInternalException spie)
        {
            System.out.println("Internal parser error: " + spie.getMessage());
            spie.printStackTrace();

            returnStatus = "Internal parser error: " + spie.getMessage();
        } catch (DBToasterTMLWriter.CreateTMLException dbte)
        {
            System.out.println("Query compilation failed!");
            dbte.printStackTrace();

            returnStatus = "Internal DBToaster parsing error: "
                    + dbte.getMessage();
        }

        if (returnStatus != null) return returnStatus;
        return toastQuery(tmlFile, sourceConfigFile, outputFile,
                compileMode, compilationDir, compilerLogFile);
    }

    public String runDBToaster(String dir,
            LinkedHashMap<String, LinkedList<String>> options, String tmlFile,
            String compilerLogFile)
    {
        System.out.println("Running DBToaster...");

        String r = null;
        LinkedList<String> args = new LinkedList<String>();
        args.add(dbToasterPath);
        for (Map.Entry<String, LinkedList<String>> e : options.entrySet())
        {
            for (String a : e.getValue()) {
                args.add(e.getKey());
                args.add(a);
            }
        }
        args.add(tmlFile);

        dbToasterProcess.directory(new File(dir));
        dbToasterProcess.command(args);

        String fa = "";
        for (String a : args) fa += (fa.isEmpty()? "" : " ") + a;

        // TODO: more robust logging of compilation attempts.
        System.out.println("Running (wd: " +
            dbToasterProcess.directory().getAbsolutePath() + "), args: " + fa);
        
        dbToasterProcess.redirectErrorStream(true);
        try
        {
            currentToaster = dbToasterProcess.start();
            BufferedReader logReader = new BufferedReader(
                new InputStreamReader(currentToaster.getInputStream()));
            
            Writer logWriter = new BufferedWriter(new FileWriter(compilerLogFile));
            String line = "";
            while ( (line = logReader.readLine()) != null )
                logWriter.write(line + "\n");

            logWriter.close();
            logReader.close();

            int rs = currentToaster.waitFor();
            if ( rs != 0 ) r = ("DBToaster returned non-zero exit status." + rs);
        } catch (IOException e)
        {
            e.printStackTrace();
            r = "I/O exception while compiling with DBToaster.";
        } catch (InterruptedException e)
        {
            e.printStackTrace();
            r = "GUI interrupted waiting for DBToaster.";
        }

        currentToaster = null;
        return r;
    }
}
