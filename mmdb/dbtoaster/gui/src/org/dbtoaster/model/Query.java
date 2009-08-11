package org.dbtoaster.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IFolder;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.Path;
import org.jfree.data.time.TimeSeries;

public class Query
{
    private String queryName;
    private IFolder queryFolder;
    // Parse phase files
    private IFile sqlCode;
    private IFile sqlTML;
    private IFile sourceConfigFile;
    
    // Compilation results (code, analysis)
    private IFile cCode;
    private CompilationTrace cTrace;
    private IFile profileLocations;
    private DependencyGraph dependencyGraph;
    private IFile pseudocode;
    
    // Binaries
    private Executor engine;
    private Debugger debugger;

    private Statistics performance;

    public Query(String name, IFolder qFolder, IFile sqlFile)
    {
        queryName = name;
        queryFolder = qFolder;
        sqlCode = sqlFile;
        sqlTML = null;
        sourceConfigFile = null;
        cCode = null;
        cTrace = null;
        engine = null;
        profileLocations = null;
        debugger = null;
        performance = null;
    }

    public void load()
    {
        // Load TML file.
        Path tmlPath = new Path(DBToasterWorkspace.TML_FILE_NAME);
        if (queryFolder.exists(tmlPath))
            sqlTML = queryFolder.getFile(tmlPath);
        else
            System.out.println("Failed to find TML: " + tmlPath.toString());
        
        Path scPath = new Path(DBToasterWorkspace.SOURCE_CONFIG_FILE_NAME);
        if (queryFolder.exists(scPath))
            sourceConfigFile = queryFolder.getFile(scPath);

        // Check if query folder has non-initialized parts.

        // Get the compiled code and trace file based on the trace catalog name
        try
        {
            for (IResource r : queryFolder.members())
            {
                if (r.getType() == IResource.FILE
                        && r.getFileExtension().equals(
                                DBToasterWorkspace.TRACE_FILE_EXT))
                {
                    IFile traceCatalog = (IFile) r;
                    IPath fnFullBasePath = traceCatalog.getFullPath()
                            .removeFileExtension();

                    IPath fullCodeFilePath = fnFullBasePath
                            .addFileExtension(DBToasterWorkspace.CODE_FILE_EXT);

                    IPath qfCodePath = fullCodeFilePath
                            .makeRelativeTo(queryFolder.getFullPath());
                    
                    IPath qfProfLocPath = fnFullBasePath.addFileExtension(
                            DBToasterWorkspace.PROFILELOC_FILE_EXT).
                            makeRelativeTo(queryFolder.getFullPath());
                    
                    IPath qfDepGraphPath = fnFullBasePath.addFileExtension(
                            DBToasterWorkspace.DEPGRAPH_FILE_EXT).
                            makeRelativeTo(queryFolder.getFullPath());

                    IPath qfPseudocodePath = fnFullBasePath.addFileExtension(
                            DBToasterWorkspace.PSEUDOCODE_FILE_EXT).
                            makeRelativeTo(queryFolder.getFullPath());

                    if (queryFolder.exists(qfCodePath))
                    {
                        System.out.println("Loading code: "
                                + qfCodePath.toString());
                        System.out.println("Loading trace: "
                                + traceCatalog.getLocation().toOSString());
                        System.out.println("Loading profile locs: "
                                + qfProfLocPath.toString());

                        cCode = queryFolder.getFile(qfCodePath);
                        
                        cTrace = new CompilationTrace(
                            traceCatalog.getLocation().toOSString());
                        
                        profileLocations = queryFolder.getFile(qfProfLocPath);

                        dependencyGraph = new DependencyGraph(queryFolder.
                            getFile(qfDepGraphPath).getLocation().toOSString());
                        
                        pseudocode = queryFolder.getFile(qfPseudocodePath);
                    }

                    IPath fullBinFilePath = fnFullBasePath
                            .addFileExtension(DBToasterWorkspace.ENGINE_FILE_EXT);

                    IPath qfBinPath = fullBinFilePath
                            .makeRelativeTo(queryFolder.getFullPath());

                    if (queryFolder.exists(qfBinPath))
                    {
                        IPath enginePath =
                            queryFolder.getFile(qfBinPath).getLocation();

                        System.out.println("Loading engine binary: "
                                + enginePath.toOSString());

                        engine = new Executor(enginePath.toOSString());
                    }

                    IPath fullDebugFilePath = fnFullBasePath
                            .addFileExtension(DBToasterWorkspace.DEBUGGER_FILE_EXT);

                    IPath qfDebugPath = fullDebugFilePath
                            .makeRelativeTo(queryFolder.getFullPath());

                    if (queryFolder.exists(qfDebugPath))
                    {
                        IPath debuggerPath =
                            queryFolder.getFile(qfDebugPath).getLocation();

                        System.out.println("Loading debugger binary: "
                                + debuggerPath.toOSString());

                        IFile debuggerJarFile = queryFolder.getFile(
                                DBToasterWorkspace.DEBUGGER_CLIENT_JAR);

                        IPath debuggerClientBase = queryFolder.getLocation().
                            append(DBToasterWorkspace.DEBUGGER_CLIENT_DIR);

                        // Load Java client for debugging and profiling
                        String clientBase = (debuggerJarFile.exists()?
                            debuggerJarFile.getLocation().toOSString() :
                            debuggerClientBase.toOSString());

                        debugger = new Debugger(queryName,
                            debuggerPath.toOSString(), clientBase);
                    }
                }
            }
        } catch (CoreException e)
        {
            e.printStackTrace();
        }
    }

    public String getQueryName()
    {
        return queryName;
    }

    public String getQuery()
    {
        InputStream sqlIS;
        String r = "";
        try
        {
            sqlIS = sqlCode.getContents();
            BufferedReader sqlReader = new BufferedReader(
                    new InputStreamReader(sqlIS));

            String line;
            while ((line = sqlReader.readLine()) != null)
                r += (line + "\n");

            sqlIS.close();
        } catch (CoreException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        return r.isEmpty() ? null : r;
    }

    public IFolder getQueryFolder() { return queryFolder; }

    // Parse result accessors

    public void setTML(IFile tml)
    {
        if (queryFolder.getFile(tml.getName()).exists()) sqlTML = tml;
    }

    public IFile getTML() { return sqlTML; }
    
    public void setSourceConfig(IFile sc)
    {
        if (queryFolder.getFile(sc.getName()).exists()) sourceConfigFile = sc;
    }
    
    public IFile getSourceConfig() { return sourceConfigFile; }

    // Query code accessors

    public void setCode(IFile codeFile)
    {
        if (queryFolder.getFile(codeFile.getName()).exists()) cCode = codeFile;
    }

    public IFile getCode() { return cCode; }

    // Analysis file accessors

    public void setTrace(IFile catalogFile)
    {
        String cpath = catalogFile.getLocation().toOSString();
        System.out.println("Reading trace from catalog " + cpath);
        cTrace = new CompilationTrace(cpath);
    }

    public CompilationTrace getTrace() { return cTrace; }

    public void setProfileLocationsFile(IFile profLocFile)
    {
        if ( queryFolder.getFile(profLocFile.getName()).exists() )
            profileLocations = profLocFile;
    }
    
    public IFile getProfileLocationsFile() { return profileLocations; }
    
    public void setDependencyGraph(IFile depGraph)
    {
        String gpath = depGraph.getLocation().toOSString();
        System.out.println("Reading dependency graph from " + gpath);
        dependencyGraph = new DependencyGraph(gpath);
    }
    
    public DependencyGraph getDependencyGraph() { return dependencyGraph; }
    
    public void setPseudocode(IFile pseudoCodeFile)
    {
        if ( queryFolder.getFile(pseudoCodeFile.getName()).exists() )
            pseudocode = pseudoCodeFile;
    }
    
    public IFile getPseudocode() { return pseudocode; }

    // Binary file accessors

    public Executor getExecutor() { return engine; }

    public void setExecutor(String engineBinary) {
        IFile binaryFile = queryFolder.getFile(engineBinary);
        if ( binaryFile.exists() ) {
            engine = new Executor(binaryFile.getLocation().toOSString());
        }
        else {
            String msg = "Could not find engine binary in query folder: " +
                binaryFile.getLocation().toOSString();
            System.err.println(msg);
        }
    }

    public void runQuery(int profilerServicePort, TimeSeries profilerSamples)
    {
        if (!hasBinary())
        {
            System.err.println("No executable found for " + queryName);
            return;
        }

        String profileLocationsFile = profileLocations.getLocation().toOSString();
        String execLogFile = queryFolder.getFile("run.log").getLocation().toOSString();
        engine.run(profileLocationsFile, execLogFile, profilerServicePort, profilerSamples);
    }

    public Debugger getDebugger() { return debugger; }

    public void setDebugger(String debuggerBinary, String debuggerClientBase)
    {
        IFile binaryFile = queryFolder.getFile(debuggerBinary);
        if ( binaryFile.exists() )
        {
            System.out.println("Creating new debugger from " +
                    binaryFile.getLocation().toOSString());

            debugger = new Debugger(queryName,
                binaryFile.getLocation().toOSString(), debuggerClientBase);
        }
        else {
            String msg = "Could not find debugger binary in query folder: " +
                binaryFile.getLocation().toOSString();
            System.err.println(msg);
        }
    }
    
    public boolean isValidQuery() { return !(sqlCode == null || sqlTML == null); }
    public boolean isParsed() { return !(sqlTML == null || sourceConfigFile == null); }
    public boolean isCompiled() { return !(cCode == null || cTrace == null); }
    public boolean isRunnable() { return !(engine == null && debugger == null); }
    public boolean hasBinary() { return engine != null; }
    public boolean hasDebugger() { return debugger != null; }
    public boolean hasStatistics() { return performance != null; }
};