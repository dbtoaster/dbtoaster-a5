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

public class Query
{
    private String queryName;
    private IFolder queryFolder;
    private IFile sqlCode;
    private IFile sqlTML;
    private IFile sourceConfigFile;
    private IFile cCode;
    private CompilationTrace cTrace;
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

                    if (queryFolder.exists(qfCodePath))
                    {
                        System.out.println("Loading code: "
                                + qfCodePath.toString());
                        System.out.println("Loading trace: "
                                + traceCatalog.getLocation().toOSString());

                        cCode = queryFolder.getFile(qfCodePath);
                        cTrace = new CompilationTrace(traceCatalog
                                .getLocation().toOSString());
                    }

                    IPath fullBinFilePath = fnFullBasePath
                            .addFileExtension(DBToasterWorkspace.ENGINE_FILE_EXT);

                    IPath qfBinPath = fullBinFilePath
                            .makeRelativeTo(queryFolder.getFullPath());

                    if (queryFolder.exists(qfBinPath))
                    {
                        System.out.println("Loading engine binary: "
                                + qfBinPath.makeAbsolute().toOSString());

                        engine = new Executor(qfBinPath.makeAbsolute()
                                .toOSString());
                    }

                    IPath fullDebugFilePath = fnFullBasePath
                            .addFileExtension(DBToasterWorkspace.DEBUGGER_FILE_EXT);

                    IPath qfDebugPath = fullDebugFilePath
                            .makeRelativeTo(queryFolder.getFullPath());

                    if (queryFolder.exists(qfDebugPath))
                    {
                        System.out.println("Loading debugger binary: "
                                + qfDebugPath.makeAbsolute().toOSString());

                        debugger = new Debugger(qfDebugPath.makeAbsolute()
                                .toOSString());
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

    public void setCode(IFile codeFile)
    {
        if (queryFolder.getFile(codeFile.getName()).exists()) cCode = codeFile;
    }

    public IFile getCode() { return cCode; }

    public void setTrace(IFile catalogFile)
    {
        String cpath = catalogFile.getLocation().toString();
        System.out.println("Reading trace from catalog " + cpath);
        cTrace = new CompilationTrace(cpath);
    }

    public CompilationTrace getTrace() { return cTrace; }

    public void runQuery()
    {
        if (!isRunnable())
        {
            System.err.println("No executable found for " + queryName);
            return;
        }

        if (engine == null)
        {
            runDebugger();
            // TODO: invoke run() on debugger.
        }
    }

    public void runDebugger()
    {
        if (!hasDebugger())
        {
            System.err.println("No debugger found for " + queryName);
            return;
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