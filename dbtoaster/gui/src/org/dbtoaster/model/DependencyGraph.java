package org.dbtoaster.model;

import java.io.File;

import prefuse.data.Graph;
import prefuse.data.io.DataIOException;
import prefuse.data.io.GraphMLReader;

public class DependencyGraph
{
    Graph dependencies;

    public DependencyGraph(String graphFileName)
    {
        File graphFile = new File(graphFileName);
        try {
            dependencies = (Graph) (new GraphMLReader().readGraph(
                graphFile.getAbsolutePath()));
        } catch (DataIOException e) {
            System.err.println("Failed to read dependency graph " +
                    graphFile.getAbsolutePath());
            e.printStackTrace();
        }
    }
    
    public Graph getDependencyGraph() { return dependencies; }
}
