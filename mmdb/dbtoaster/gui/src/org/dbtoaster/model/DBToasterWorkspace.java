package org.dbtoaster.model;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.dbtoaster.model.Compiler.CompileMode;
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

public class DBToasterWorkspace
{
    // Members
    private AtomicLong queryCounter;

    // Default workspace, sql, and c++ code projects.
    IWorkspace rcpWorkspace;

    final static String projectDescription = "DBToaster Queries";
    final static String defaultLocation = "/Users/yanif/tmp/dbtoaster";

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
    public final static String TRACE_FILE_EXT = "tc";
    public final static String ENGINE_FILE_EXT = "dbte";
    public final static String DEBUGGER_FILE_EXT = "dbtd";

    private DBToasterWorkspace()
    {
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

        datasets = DatasetManager.initDemoDatasetManager();
        wsQueries = new LinkedHashMap<String, Query>();

        dbToaster = new Compiler(datasets);

        loadWorkspace();
    }

    private void loadWorkspace()
    {
        try
        {
            for (IResource r : dbToasterProject.members())
            {
                if (r.getType() == IResource.FOLDER)
                {
                    // Load query from this path.
                    IFolder folder = (IFolder) r;
                    String queryName = folder.getName();
                    IPath sqlFilePath = new Path(QUERY_FILE_NAME);

                    if (folder.exists(sqlFilePath))
                    {
                        IFile sqlFile = folder.getFile(sqlFilePath);
                        Query q = new Query(queryName, folder, sqlFile);
                        q.load();
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
        if (dbToasterProject.exists(new Path(queryName))) r = generateQueryName();
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

    public String compileQuery(Query q, String querySQL, String outputFile,
            CompileMode compilerMode)
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
        
        String status = dbToaster.toastQuery(querySQL, absTmlPath.toOSString(),
                absSConfigPath.toOSString(), absOutputPath.toOSString(),
                compilerMode, compilerWorkingDir, compilerLogFile);

        if (status == null)
        {
            try
            {
                // Sync presence of new files in workspace.
                queryFolder.refreshLocal(IResource.DEPTH_INFINITE, null);

                // Get files as relative to query folder.
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

                IFile compiledCode = queryFolder.getFile(outputFile);
                IPath tcPath = new Path(outputFile).removeFileExtension()
                        .addFileExtension(TRACE_FILE_EXT);
                IFile catalogFile = q.getQueryFolder().getFile(tcPath);

                if (!(compiledCode.exists() && catalogFile.exists()))
                {
                    String error =
                        "Compilation failed to product output and trace for " +
                            compiledCode.getLocation().toString() + ", " +
                            catalogFile.getLocation().toString();
                    return error;
                }

                q.setCode(compiledCode);
                q.setTrace(catalogFile);
                return "Successfully compiled query!";

            } catch (CoreException e)
            {
                e.printStackTrace();
            }
        }

        return status;
    }

    public String compileQuery(String queryName, String outputFile,
            CompileMode compilerMode)
    {
        if (!wsQueries.containsKey(queryName)) { return ("Invalid query name "
                + queryName + " (could not find query in working set)."); }

        Query q = wsQueries.get(queryName);
        return compileQuery(q, q.getQuery(), outputFile, compilerMode);
    }

    public void runQuery(String queryName)
    {
        if (!wsQueries.containsKey(queryName))
        {
            System.err.println("Invalid query name " + queryName
                    + " (could not find query in working set).");
            return;
        }

        Query q = wsQueries.get(queryName);
        q.runQuery();
    }

    public void debugQuery(String queryName)
    {
        if (!wsQueries.containsKey(queryName))
        {
            System.err.println("Invalid query name " + queryName
                    + " (could not find query in working set).");
            return;
        }

        Query q = wsQueries.get(queryName);
        q.runDebugger();
    }
}
