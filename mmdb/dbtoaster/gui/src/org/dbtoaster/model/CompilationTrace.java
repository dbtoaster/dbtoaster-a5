package org.dbtoaster.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import prefuse.data.Node;
import prefuse.data.Table;
import prefuse.data.Tree;
import prefuse.data.io.DataIOException;
import prefuse.data.io.TreeMLReader;

public class CompilationTrace
{
    class CompilationStages
    {
        // stage type -> stage name -> map expression TML
        LinkedHashMap<String, LinkedHashMap<String, Tree>> stages;

        public CompilationStages()
        {
            stages = new LinkedHashMap<String, LinkedHashMap<String, Tree>>();
        }

        public void addStage(String typeName, String stageName, Tree s)
        {
            LinkedHashMap<String, Tree> stagesByName = null;
            if (stages.containsKey(typeName)) stagesByName = stages
                    .get(typeName);
            else
            {
                stagesByName = new LinkedHashMap<String, Tree>();
                stages.put(typeName, stagesByName);
            }

            stagesByName.put(stageName, s);
        }

        LinkedHashMap<String, LinkedList<String>> getStageTypesAndNames()
        {
            LinkedHashMap<String, LinkedList<String>> r = new LinkedHashMap<String, LinkedList<String>>();

            for (Map.Entry<String, LinkedHashMap<String, Tree>> typeEntry : stages
                    .entrySet())
            {
                LinkedList<String> stageNames = new LinkedList<String>(
                        typeEntry.getValue().keySet());
                r.put(typeEntry.getKey(), stageNames);
            }

            return r;
        }

        public Tree getStage(String typeName, String stageName)
        {
            if (stages.containsKey(typeName))
            {
                LinkedHashMap<String, Tree> stagesByName = stages.get(typeName);
                if (stagesByName.containsKey(stageName))
                    return stagesByName.get(stageName);
            }
            return null;
        }

        public int size()
        {
            return stages.size();
        }
    };

    LinkedHashMap<LinkedList<String>, CompilationStages> traces;

    public CompilationTrace(String catalogFileName)
    {
        traces = new LinkedHashMap<LinkedList<String>, CompilationStages>();
        loadTrace(catalogFileName);
    }

    public void loadTrace(String catalogFileName)
    {
        BufferedReader catalogFile;
        String currentTraceFile = "";
        try
        { 
            File catalogFilePath = new File(catalogFileName);
            catalogFile = new BufferedReader(new FileReader(catalogFilePath));
            
            // Get the catalog directory
            String catalogDir = null;
            File catalogDirPath = catalogFilePath.getParentFile();
            if ( catalogDirPath != null && catalogDirPath.exists() &&
                    catalogDirPath.isDirectory())
            {
                catalogDir = catalogDirPath.getAbsolutePath();
            }

            while (catalogFile.ready())
            {
                String line = catalogFile.readLine();
                String[] lineFields = line.split(",");
                if (lineFields.length != 2)
                {
                    System.out.println("Invalid catalog file entry: " + line);
                    break;
                }

                String[] eventPathFields = lineFields[0].split("/");
                LinkedList<String> eventPath = new LinkedList<String>();

                for (String e : eventPathFields)
                {
                    String ne = e;

                    if (e.startsWith("insert")) ne = e.replaceFirst("insert",
                            "+");
                    else if (e.startsWith("delete"))
                        ne = e.replaceFirst("delete", "-");

                    eventPath.add(ne);
                }

                String eventPathName = "";
                for (String e : eventPath)
                    eventPathName += (eventPathName.isEmpty() ? "" : ",") + e;
                System.out.println("Adding event path " + eventPathName);

                currentTraceFile = lineFields[1];
                File traceFilePath = new File(catalogDir, currentTraceFile);
                Tree t = (Tree) (new TreeMLReader().readGraph(
                        traceFilePath.getAbsolutePath()));

                System.out.println("Read trace file " + currentTraceFile);

                // Walk the tree creating subtrees for each stage.
                addStages(eventPath, t);
            }

        } catch (FileNotFoundException e)
        {
            System.out.println("Could not find file " + catalogFileName);
        } catch (IOException e)
        {
            System.out.println("Error reading from file " + catalogFileName);
            e.printStackTrace();
        } catch (DataIOException e)
        {
            System.out.println("Error reading trace file " + currentTraceFile);
            e.printStackTrace();
        }

    }

    private void addStages(LinkedList<String> eventPath, Tree traceTree)
    {
        LinkedHashMap<String, LinkedHashMap<String, Node>> stageRoots = getStageRoots(traceTree);

        CompilationStages stages = new CompilationStages();

        for (Map.Entry<String, LinkedHashMap<String, Node>> typeEntry : stageRoots
                .entrySet())
        {
            for (Map.Entry<String, Node> nameEntry : typeEntry.getValue()
                    .entrySet())
            {
                Tree t = createSubtree(nameEntry.getValue());
                stages.addStage(typeEntry.getKey(), nameEntry.getKey(), t);
            }
        }

        System.out.println("Adding event path with " + stages.size()
                + " stages.");
        traces.put(eventPath, stages);
    }

    private LinkedHashMap<String, LinkedHashMap<String, Node>> getStageRoots(
            Tree t)
    {
        LinkedList<Node> nodeQueue = new LinkedList<Node>();
        LinkedHashMap<String, LinkedHashMap<String, Node>> r = new LinkedHashMap<String, LinkedHashMap<String, Node>>();

        // Note: trace files include a dummy root.
        for (int i = 0; i < t.getRoot().getChildCount(); ++i)
            nodeQueue.add(t.getRoot().getChild(i));

        System.out.println("Getting stage roots from tree of size "
                + t.getNodeCount());

        String currentType = null;
        LinkedHashMap<String, Node> stagesByName = null;
        while (!nodeQueue.isEmpty())
        {
            Node p = nodeQueue.pop();

            if (p.canGetString("stage-type"))
            {
                String st = p.getString("stage-type");
                if (!(st == null || st.isEmpty()))
                {

                    if (!(currentType == null || stagesByName == null))
                        r.put(currentType, stagesByName);

                    currentType = st;
                    stagesByName = new LinkedHashMap<String, Node>();

                    for (int i = p.getChildCount() - 1; i >= 0; --i)
                        nodeQueue.addFirst(p.getChild(i));
                }
            }

            if (p.canGetString("stage-name"))
            {
                String sn = p.getString("stage-name");
                if (!(sn == null || sn.isEmpty()))
                {
                    if (p.getChildCount() != 1)
                    {
                        System.out
                                .println("WARNING: Found stage with multiple map expressions ("
                                        + p.getChildCount() + ")");
                    }

                    for (int i = 0; i < p.getChildCount(); ++i)
                        stagesByName.put(sn, p.getChild(i));
                }
            }
        }

        if (!(currentType == null || stagesByName == null))
            r.put(currentType, stagesByName);

        return r;
    }

    private void copyNode(Tree t, Node a, Node b)
    {
        // Copy columns
        for (int i = 0; i < b.getColumnCount(); ++i)
            a.set(i, b.get(i));

        // Recur over children
        for (int i = 0; i < b.getChildCount(); ++i)
        {
            Node c = t.addChild(a);
            copyNode(t, c, b.getChild(i));
        }
    }

    private Tree createSubtree(Node n)
    {
        Tree tr = new Tree();
        Table tb = tr.getNodeTable();

        // Copy schema
        for (int i = 0; i < n.getColumnCount(); ++i)
            tb.addColumn(n.getColumnName(i), n.getColumnType(i));

        // Copy nodes
        Node root = tr.addRoot();
        copyNode(tr, root, n);

        return tr;
    }

    public Set<LinkedList<String>> getEventPaths()
    {
        return traces.keySet();
    }

    public LinkedHashMap<String, LinkedList<String>> getEventPathStages(
            LinkedList<String> eventPath)
    {
        if (traces.containsKey(eventPath))
            return traces.get(eventPath).getStageTypesAndNames();
        return null;
    }

    public Tree getCompilationStage(LinkedList<String> eventPath,
            String typeName, String stageName)
    {
        if (traces.containsKey(eventPath))
            return traces.get(eventPath).getStage(typeName, stageName);
        return null;
    }
}
