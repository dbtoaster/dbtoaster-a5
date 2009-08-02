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
        "/Users/yanif/workspace/dbtoaster_compiler/";

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
                r = path;
                break;
            }
        }
        
        return r;
    }

    private String getSiblingDir(String dir, String siblingBase)
    {
        String r = null;
        File dirPath = new File(dir);
        File dirParent = dirPath.getParentFile();
        if ( dirParent != null ) {
            File siblingDirPath = new File(dirParent, siblingBase);
            if ( siblingDirPath.exists() && siblingDirPath.isDirectory() )
                r = siblingDirPath.getAbsolutePath();
        }
            
        return r;
    }

    private boolean checkInclude(String dir, String testFile)
    {
        File testDirPath = new File(dir);
        File testFilePath = new File(dir, testFile);
        return ( testDirPath.exists() && testDirPath.isDirectory() &&
            testFilePath.exists() && testFilePath.isFile() );
    }
    
    private String checkSystemIncludePaths(LinkedList<String> additionalPaths, String testFile)
    {
        LinkedList<String> fileTestPaths = new LinkedList<String>();
        if ( additionalPaths != null) fileTestPaths.addAll(additionalPaths);

        String pathEnvVar = System.getenv("PATH");
        String[] testPaths = pathEnvVar.split(":");
        fileTestPaths.addAll(Arrays.asList(testPaths));
        
        String r = null;
        for (String p : fileTestPaths)
        {
            if ( checkInclude(getSiblingDir(p, "include"), testFile) ) {
                r = p; break;
            }
        }
        
        return r;
    }

    // Boost configuration.
    private String findBoostPath()
    {
        LinkedList<String> ap = new LinkedList<String>();
        File homeDir = new File(System.getenv("HOME"));
        if ( homeDir.exists() ) {
            String p = "software/boost/include/boost-1_39/";
            ap.add(new File(homeDir, p).getAbsolutePath());
        }

        return checkSystemIncludePaths(ap, "boost/boost.hpp");
    }
    
    private void buildBoostOptions(LinkedHashMap<String, String> options)
    {
        String boostIncPath = findBoostPath();
        if ( boostIncPath != null ) {
            String boostLibPath = getSiblingDir(boostIncPath, "lib");
            options.put("-cI", boostIncPath);
            options.put("-cL", boostLibPath);
            // TODO: add boost libraries.
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
        File homeDir = new File(System.getenv("HOME"));
        if ( homeDir.exists() ) {
            String p = "software/thrift/include/";
            ap.add(new File(homeDir, p).getAbsolutePath());
        }

        return checkSystemIncludePaths(ap, "thrift/Thrift.h");
    }
    
    private void buildThriftOptions(LinkedHashMap<String, String> options)
    {
        String thriftIncPath = findThriftPath();
        if ( thriftIncPath != null ) {
            String thriftLibPath = getSiblingDir(thriftIncPath, "lib");
            options.put("-cI", thriftIncPath);
            options.put("-cL", thriftLibPath);
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
    public String toastQuery(String tmlFile, String outputFile,
            CompileMode compileMode, String compilationDir, String compilerLogFile)
    {
        String returnStatus = null;

        // Invoke compiler on TML through ProcessBuilder
        LinkedHashMap<String, String> options = new LinkedHashMap<String, String>();
        options.put("-o", outputFile);
        options.put("-m", getCompileMode(compileMode));
        buildBoostOptions(options);
        buildThriftOptions(options);
        returnStatus = runDBToaster(
            compilationDir, options, tmlFile, compilerLogFile);

        return returnStatus;
    }

    // Parse and compile.
    // Note: tmlFile, outputFile are absolute paths on the local filesystem.
    public String toastQuery(String sqlQuery, String tmlFile,
            String sourceConfigFile, String outputFile,
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

            List syntacticErrors = spe.getErrorInfoList();
            Iterator itr = syntacticErrors.iterator();
            while (itr.hasNext())
            {
                SQLParseErrorInfo errorInfo = (SQLParseErrorInfo) itr.next();
                // Example usage of the SQLParseErrorInfo object
                // the error message
                String errorMessage = errorInfo.getParserErrorMessage();
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
        return toastQuery(tmlFile, outputFile,
                compileMode, compilationDir, compilerLogFile);
    }

    public String runDBToaster(String dir,
            LinkedHashMap<String, String> options, String tmlFile,
            String compilerLogFile)
    {
        String r = null;
        LinkedList<String> args = new LinkedList<String>();
        args.add(dbToasterPath);
        for (Map.Entry<String, String> e : options.entrySet())
            args.add(e.getKey()+" "+e.getValue());
        args.add(tmlFile);

        dbToasterProcess.directory(new File(dir));
        dbToasterProcess.command(args);

        String fa = "";
        for (String a : args) fa += (fa.isEmpty()? "" : " ") + a;

        // TODO: more robust loggingn of compilation attempts.
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

            int rs = currentToaster.waitFor();
            if ( rs != 0 ) r = "DBToaster returned non-zero exit status.";
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
