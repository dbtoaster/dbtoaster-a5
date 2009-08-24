package org.dbtoaster.model;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.io.BufferedWriter;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;


import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IFolder;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IProjectDescription;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.resources.IWorkspace;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.Path;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.TimeSeries;
import org.jfree.experimental.chart.swt.ChartComposite;

public class DBToasterWorkspace
{
    // Members
    private AtomicLong queryCounter;

    // Default workspace, sql, and c++ code projects.
    IWorkspace rcpWorkspace;

    final static String projectDescription = "DBToaster Queries";
    final static String defaultLocation = "/Users/yanif/tmp/dbtoaster";
    final static String defaultPathCon = 
    	"/Users/yanif/workspace/dbtoaster-gui/path_config.txt";
    
    final Map<String, String> path_map;
 
    IProject dbToasterProject;

    // Datasets and working queries.
    DatasetManager datasets;
    LinkedHashMap<String, Query> wsQueries;

    Compiler dbToaster;

    // Singleton class
    private static DBToasterWorkspace dbtWorkspace = null;

    // Constants
    private final static String QUERY_NAME_PREFIX = "query";

    public final static String QUERY_FILE_NAME = "query.sql";
    public final static String TML_FILE_NAME = "query.tml";
    public final static String SOURCE_CONFIG_FILE_NAME = "query.sources";

    public final static String CODE_FILE_EXT = "cc";
    
    // Analysis files
    public final static String TRACE_FILE_EXT = "tc";
    public final static String PROFILELOC_FILE_EXT = "prl";
    public final static String DEPGRAPH_FILE_EXT = "deps";
    public final static String PSEUDOCODE_FILE_EXT = "pseudo";
    
    // Binary files
    public final static String ENGINE_FILE_EXT = "dbte";
    public final static String DEBUGGER_FILE_EXT = "dbtd";
    
    public final static String THRIFT_OUTPUT_DIR = "thrift";
    
    public final static String DEBUGGER_CLIENT_DIR = THRIFT_OUTPUT_DIR+"/gen-java";
    public final static String DEBUGGER_CLIENT_JAR = "debugger.jar";

    private DBToasterWorkspace()
    {
        System.err.println("Invoking workspace constructor.");
        
        queryCounter = new AtomicLong();

        rcpWorkspace = ResourcesPlugin.getWorkspace();

        IProjectDescription projectDesc = rcpWorkspace
                .newProjectDescription(projectDescription);
        projectDesc.setLocation(new Path(defaultLocation));
        projectDesc.setComment("DBToaster working files");

        dbToasterProject = rcpWorkspace.getRoot()
                .getProject(projectDescription);

        try
        {
            if (!dbToasterProject.exists())
                dbToasterProject.create(projectDesc, null);

            dbToasterProject.open(null);
        } catch (CoreException e)
        {
            System.err.println("Failed to open default SQL, C projects.");
            e.printStackTrace();
            System.exit(1);
        }
        path_map = loadPath(null);
        
        datasets = DatasetManager.initDemoDatasetManager();
        wsQueries = new LinkedHashMap<String, Query>();

        dbToaster = new Compiler(datasets, this);

        loadWorkspace();
    }
    
    private Map<String, String> loadPath(String filename)
    {
    	if (filename == null)
    		filename = defaultPathCon;
    	
    	HashMap<String, String> paths = new HashMap<String, String>();
    	try {
    		File pf = new File(filename);
    		FileInputStream fin = new FileInputStream(pf);
        	BufferedReader input = new BufferedReader(new InputStreamReader(fin));
    	
        	String line = "";
        	
        	while ((line = input.readLine())!= null) {
        		String delim = "[=]";
        		String[] tokens = line.split(delim);
        		String key="", data="";
        		
        		if (tokens.length != 2) {
        			System.err.println("Wrong Format");
        			continue;
        		}
        		
        		int j = 0;
        		for (String s : tokens) {
        			String tmp = ""; 
        			for (int i = 0 ; i < s.length(); i ++) {
        				if(s.charAt(i) != ' ')
        					tmp += s.charAt(i);
        			}
        			if ( j == 0)
        				key = tmp;
        			else data = tmp;
        			j++;
        		}
        		paths.put(key, data);
        		
        	}
    	} catch (Exception e){
    		e.printStackTrace();
    	}
    	
    	for(Map.Entry<String, String> e: paths.entrySet()) {
    		System.out.println(e.getKey() + "  " + e.getValue());
    	}
    	
    	return paths;
    }

    public String getPath(String key)
    {
    	String r = path_map.get(key);
    	if (r == null)
    		return "";
    	
    	return r;
    }
    
    private void loadWorkspace()
    {
        try
        {
            System.err.println("Project members size " + dbToasterProject.members().length);

            for (IResource r : dbToasterProject.members())
            {

                System.err.println("Testing project member " + r.getName());

                if (r.getType() == IResource.FOLDER)
                {
                    // Load query from this path.
                    IFolder folder = (IFolder) r;
                    String queryName = folder.getName();
                    IPath sqlFilePath = new Path(QUERY_FILE_NAME);

                    System.err.println("Loading queries from project path " + queryName);

                    if (folder.exists(sqlFilePath))
                    {
                        IFile sqlFile = folder.getFile(sqlFilePath);
                        Query q = new Query(queryName, folder, sqlFile);
                        q.load(datasets);
                        wsQueries.put(queryName, q);

                        if (queryName.startsWith(QUERY_NAME_PREFIX))
                        {
                            // Advance counter
                            String[] qnFields = queryName.split(
                                    QUERY_NAME_PREFIX, 0);
                            try
                            {
                                Long c = Long
                                        .parseLong(qnFields[qnFields.length - 1]);
                                queryCounter.set(c + 1);
                            } catch (NumberFormatException nfe)
                            {
                                System.err
                                        .println("Found query name matching prefix,"
                                                + " but without counter: "
                                                + queryName);
                            }
                        }
                    }
                }
                else
                {
                    System.out.println("Unexpected resource type "
                            + r.getType() + " at path " + r.getLocation());
                }
            }
        } catch (CoreException e)
        {
            e.printStackTrace();
        }
    }

    public static synchronized DBToasterWorkspace getWorkspace()
    {
        if (dbtWorkspace == null) dbtWorkspace = new DBToasterWorkspace();

        return dbtWorkspace;
    }

    public DatasetManager getDatasetManager()
    {
        return datasets;
    }

    public Query getQuery(String queryName)
    {
        if (!wsQueries.containsKey(queryName))
        {
            System.err.println("Invalid query name " + queryName
                    + " (could not find query in working set).");
            return null;
        }

        return wsQueries.get(queryName);
    }

    public LinkedHashMap<String, Query> getWorkingQueries()
    {
        return wsQueries;
    }

    public LinkedList<Query> getCompiledQueries()
    {
        LinkedList<Query> r = new LinkedList<Query>();
        for (Map.Entry<String, Query> e : wsQueries.entrySet())
        {
            Query q = e.getValue();
            if (q.isCompiled()) r.add(q);
        }

        return r;
    }

    public LinkedList<Query> getExecutableQueries()
    {
        LinkedList<Query> r = new LinkedList<Query>();
        for (Map.Entry<String, Query> e : wsQueries.entrySet())
        {
            Query q = e.getValue();
            if (q.hasBinary()) r.add(q);
        }

        return r;
    }

    public LinkedList<Query> getDebuggingQueries()
    {
        LinkedList<Query> r = new LinkedList<Query>();
        for (Map.Entry<String, Query> e : wsQueries.entrySet()) {
            Query q = e.getValue();
            if ( q.hasDebugger() ) r.add(q);
        }
        return r;
    }
    
    public CompilationTrace getCompilationTrace(String queryName)
    {
        if (!wsQueries.containsKey(queryName))
        {
            System.err.println("Invalid query name " + queryName
                    + " (could not find query in working set).");
            return null;
        }

        Query q = wsQueries.get(queryName);
        return q.getTrace();
    }

    public LinkedList<Query> getQueriesWithPseudocode()
    {
        LinkedList<Query> r = new LinkedList<Query>();
        for (Map.Entry<String, Query> e : wsQueries.entrySet())
        {
            if ( e.getValue().getPseudocode() != null)
                r.add(e.getValue());
        }
        return r;
    }

    public String generateQueryName()
    {
        String r = QUERY_NAME_PREFIX
                + Long.toString(queryCounter.getAndIncrement());
        System.out.println("Generated new query name: " + r);
        return r;
    }

    public String getUniqueQueryName(String queryName)
    {
        String r = null;
        if ( dbToasterProject.exists(new Path(queryName)) )
            r = generateQueryName();
        else r = queryName;
        return r;
    }

    public String createQuery(String queryName, String querySQL)
    {
        String actualQueryName = getUniqueQueryName(queryName);
        String queryFolderName = actualQueryName;
        IFolder queryFolder = dbToasterProject.getFolder(queryFolderName);

        System.out.println("Creating query in " + queryFolderName + " at loc "
                + queryFolder.getLocation().toOSString());

        try
        {
            if (!queryFolder.exists())
            {
                System.out.println("Creating folder...");
                queryFolder.create(false, false, null);
            }

            IFile queryFile = queryFolder.getFile(QUERY_FILE_NAME);
            if (!queryFile.exists())
            {
                System.out.println("Creating file "
                        + queryFile.getLocation().toOSString());
                queryFile.create(null, false, null);
            }

            queryFile.setContents(
                    new ByteArrayInputStream(querySQL.getBytes()), false,
                    false, null);

            Query q = new Query(actualQueryName, queryFolder, queryFile);
            wsQueries.put(actualQueryName, q);

        } catch (CoreException e)
        {
            System.err.println("Failed to create query: " + queryName);
            e.printStackTrace();
        }

        return actualQueryName;
    }

    public String createQuery(String querySQL)
    {
        System.out.println("Attempting to create query.");
        String queryName = generateQueryName();
        System.out.println(getClass().getName() + ".createQuery(): "
                + queryName);
        return createQuery(queryName, querySQL);
    }

    public void removeQuery(String queryName)
    {
        if (!wsQueries.containsKey(queryName))
        {
            System.err.println("Invalid query name " + queryName
                    + " (could not find query in working set).");
            return;
        }

        // Delete from project
        Query q = wsQueries.get(queryName);
        try
        {
            q.getQueryFolder().delete(false, null);
        } catch (CoreException e)
        {
            System.err.println("Failed to delete query " + queryName
                    + ", manual cleanup required.");
            e.printStackTrace();
        }

        wsQueries.remove(queryName);
    }

    public String compileQuery(
        Query q, String querySQL, String outputFile, int compileMode)
    {
        IFolder queryFolder = q.getQueryFolder();
        IPath qfLoc = queryFolder.getLocation();

        IPath actualOutputFile = qfLoc.append(outputFile);

        IPath absOutputPath = actualOutputFile;
        if (!actualOutputFile.isAbsolute())
            absOutputPath = actualOutputFile.makeAbsolute();

        IPath absTmlPath = qfLoc.append(TML_FILE_NAME);
        if (!absTmlPath.isAbsolute()) absTmlPath = absTmlPath.makeAbsolute();

        IPath absSConfigPath = qfLoc.append(SOURCE_CONFIG_FILE_NAME);
        if (!absSConfigPath.isAbsolute())
            absSConfigPath = absSConfigPath.makeAbsolute();

        System.out.println("Toasting query:\n" + absTmlPath.toOSString() + "\n"
                + absOutputPath.toOSString());

        String compilerWorkingDir = queryFolder.getLocation().toOSString();
        String compilerLogFile =
            queryFolder.getLocation().append("compile.log").toOSString();
        
        LinkedList<LinkedHashMap<String, String>> queryRelations =
            new LinkedList<LinkedHashMap<String,String>>();

        String status = dbToaster.toastQuery(querySQL, absTmlPath.toOSString(),
                absSConfigPath.toOSString(), absOutputPath.toOSString(),
                compileMode, compilerWorkingDir, compilerLogFile,
                queryRelations);

        if (status == null)
        {
            q.setQueryRelations(queryRelations);

            try
            {
                // Sync presence of new files in workspace.
                queryFolder.refreshLocal(IResource.DEPTH_INFINITE, null);

                // Get files as relative to query folder.
                
                // Parsing phase
                IFile tmlFile = queryFolder.getFile(TML_FILE_NAME);
                IFile scFile = queryFolder.getFile(SOURCE_CONFIG_FILE_NAME);
                q.setTML(tmlFile);
                q.setSourceConfig(scFile);

                if (!q.isParsed())
                {
                    String error = "Failed to find parse result, and output to "
                        + absTmlPath.toOSString() + ", " + absSConfigPath.toOSString();
                    return error;
                }

                // DBToaster compilation phase.
                IPath compiledCodePath = absOutputPath.makeRelativeTo(qfLoc);
                IFile compiledCode = queryFolder.getFile(compiledCodePath);
                
                IPath queryBasePath = compiledCodePath.removeFileExtension();
                
                IPath tcPath = queryBasePath.addFileExtension(TRACE_FILE_EXT);
                IFile catalogFile = queryFolder.getFile(tcPath);

                IPath plPath = queryBasePath.addFileExtension(PROFILELOC_FILE_EXT);
                IFile profLocFile = queryFolder.getFile(plPath);

                IPath depGraphPath = queryBasePath.addFileExtension(DEPGRAPH_FILE_EXT);
                IFile depGraphFile = queryFolder.getFile(depGraphPath);

                IPath pseudoCodePath = queryBasePath.addFileExtension(PSEUDOCODE_FILE_EXT);
                IFile pseudoCodeFile = queryFolder.getFile(pseudoCodePath);

                LinkedHashMap<String, IFile> compilationResults =
                    new LinkedHashMap<String, IFile>();

                compilationResults.put("query code", compiledCode);
                compilationResults.put("compile trace", catalogFile);
                compilationResults.put("profile locations", profLocFile);
                compilationResults.put("dependency graph", depGraphFile);
                compilationResults.put("pseudocode", pseudoCodeFile);

                for (Map.Entry<String, IFile> e : compilationResults.entrySet())
                {
                    if ( !e.getValue().exists() ) {
                        String error = "Compilation failed to produce " +
                            e.getKey() + " for query " + q.getQueryName();
                        return error;
                    }
                }

                q.setCode(compiledCode);
                q.setTrace(catalogFile);
                q.setProfileLocationsFile(profLocFile);
                q.setDependencyGraph(depGraphFile);
                q.setPseudocode(pseudoCodeFile);

                // C++ compilation phase (performed by DBToaster)
                IPath enginePath = queryBasePath.addFileExtension(ENGINE_FILE_EXT);
                IFile engineBinFile = queryFolder.getFile(enginePath);

                IPath debuggerPath = queryBasePath.addFileExtension(DEBUGGER_FILE_EXT);
                IFile debuggerBinFile = queryFolder.getFile(debuggerPath);

                IPath debuggerClientBase =
                    queryFolder.getLocation().append(DEBUGGER_CLIENT_DIR);
                
                IFile debuggerJarFile = queryFolder.getFile(DEBUGGER_CLIENT_JAR);

                if ( (compileMode & Compiler.ENGINE) == Compiler.ENGINE )
                {
                    // Set up query executor
                    if ( engineBinFile.exists() )
                        q.setExecutor(enginePath.toOSString(), datasets);
                    else {
                        status = "Could not find engine binary " +
                            engineBinFile.getLocation().toOSString();
                    }
                }
                
                if ( (compileMode & Compiler.DEBUGGER) == Compiler.DEBUGGER )
                {
                    if ( debuggerBinFile.exists() )
                    {
                        // Load Java client for debugging and profiling
                        String clientBase = (debuggerJarFile.exists()?
                            debuggerJarFile.getLocation().toOSString() :
                            debuggerClientBase.toOSString());

                        q.setDebugger(debuggerPath.toOSString(), clientBase);
                    }
                    else {
                        status = "Could not find debugger binary " +
                            debuggerBinFile.getLocation().toOSString();
                    }
                }
                
                if ( status == null )
                    status = "Successfully compiled query!";

            }
            catch (CoreException e) {
                status = "Compilation failed: " + e.getCause().getMessage();
                e.printStackTrace();
            }
        }

        return status;
    }

    public String compileQuery(String queryName, String outputFile,
            int compilerMode)
    {
        if (!wsQueries.containsKey(queryName)) {
            return ("Invalid query name "
                + queryName + " (could not find query in working set).");
        }

        Query q = wsQueries.get(queryName);
        return compileQuery(q, q.getQuery(), outputFile, compilerMode);
    }

    public void runQuery(String queryName, int profilerPort,
            TimeSeries profilerSamples, ChartComposite profilerChart)
    {
        if (!wsQueries.containsKey(queryName))
        {
            System.err.println("Invalid query name " + queryName
                    + " (could not find query in working set).");
            return;
        }

        Query q = wsQueries.get(queryName);
        q.runQuery(profilerPort, profilerSamples, profilerChart);
    }

    public void stopQuery(String queryName)
    {
        if (!wsQueries.containsKey(queryName))
        {
            System.err.println("Invalid query name " + queryName
                    + " (could not find query in working set).");
            return;
        }
    
        Query q = wsQueries.get(queryName);
        q.stopQuery();
    }

    public Debugger getDebugger(String queryName)
    {
        if (!wsQueries.containsKey(queryName))
        {
            System.err.println("Invalid query name " + queryName
                    + " (could not find query in working set).");
            return null;
        }

        Query q = wsQueries.get(queryName);
        return q.getDebugger();
    }
}
