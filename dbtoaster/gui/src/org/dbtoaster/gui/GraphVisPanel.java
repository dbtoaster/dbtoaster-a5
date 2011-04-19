package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.SwingConstants;

import prefuse.Constants;
import prefuse.Display;
import prefuse.Visualization;
import prefuse.action.Action;
import prefuse.action.ActionList;
import prefuse.action.GroupAction;
import prefuse.action.ItemAction;
import prefuse.action.RepaintAction;
import prefuse.action.animate.ColorAnimator;
import prefuse.action.animate.LocationAnimator;
import prefuse.action.animate.QualityControlAnimator;
import prefuse.action.animate.VisibilityAnimator;
import prefuse.action.assignment.ColorAction;
import prefuse.action.assignment.FontAction;
import prefuse.action.layout.CollapsedSubtreeLayout;
import prefuse.action.layout.graph.RadialTreeLayout;
import prefuse.activity.SlowInSlowOutPacer;
import prefuse.controls.Control;
import prefuse.controls.DragControl;
import prefuse.controls.FocusControl;
import prefuse.controls.HoverActionControl;
import prefuse.controls.PanControl;
import prefuse.controls.WheelZoomControl;
import prefuse.controls.ZoomControl;
import prefuse.controls.ZoomToFitControl;
import prefuse.data.Graph;
import prefuse.data.Node;
import prefuse.data.Tuple;
import prefuse.data.io.GraphMLReader;
import prefuse.data.tuple.TupleSet;
import prefuse.render.AbstractShapeRenderer;
import prefuse.render.DefaultRendererFactory;
import prefuse.render.EdgeRenderer;
import prefuse.render.LabelRenderer;
import prefuse.util.ColorLib;
import prefuse.util.FontLib;
import prefuse.util.GraphicsLib;
import prefuse.util.display.DisplayLib;
import prefuse.util.ui.JFastLabel;
import prefuse.visual.VisualItem;
import prefuse.visual.expression.InGroupPredicate;

public class GraphVisPanel extends JPanel
{

    private static final long serialVersionUID = -1470902580417435532L;

    private static final String graph = "graph";
    private static final String graphNodes = "graph.nodes";
    private static final String graphEdges = "graph.edges";
    
    Visualization graphVis;
    Display graphDisplay;

    Graph g;
    String labelField;
    String title;

    private LabelRenderer nodeRenderer;
    private EdgeRenderer edgeRenderer;

    private LinkedList<Control> controls;

    class GroupColors
    {
        public class KeyAndColor
        {
            public String key;
            public Integer color;

            public KeyAndColor(String k, Integer c)
            {
                key = k;
                color = c;
            }
        };

        public HashMap<String, KeyAndColor> colorings;

        public GroupColors()
        {
            colorings = new HashMap<String, KeyAndColor>();
        }

        public void put(String group, String key, Integer color)
        {
            colorings.put(group, new KeyAndColor(key, color));
        }

        public KeyAndColor get(String group)
        {
            return colorings.get(group);
        }
    };

    GroupColors groupColors;

    public GraphVisPanel(String title)
    {
        this(new Graph(), "name", title);
    }
    
    public GraphVisPanel(Graph g, String labelField, String title)
    {
        setLayout(new BorderLayout());
        
        this.g = g;
        this.labelField = labelField;
        this.title = title;
        
        graphVis = new Visualization();
        groupColors = new GroupColors();
        controls = new LinkedList<Control>();

        graphVis.add(graph, g);

        nodeRenderer = new LabelRenderer(labelField);
        nodeRenderer.setRenderType(AbstractShapeRenderer.RENDER_TYPE_FILL);
        nodeRenderer.setHorizontalAlignment(Constants.CENTER);
        nodeRenderer.setRoundedCorner(8, 8);

        edgeRenderer = new EdgeRenderer(Constants.EDGE_TYPE_CURVE);
        edgeRenderer.setHorizontalAlignment1(Constants.CENTER);
        edgeRenderer.setHorizontalAlignment2(Constants.CENTER);
        edgeRenderer.setVerticalAlignment1(Constants.BOTTOM);
        edgeRenderer.setVerticalAlignment2(Constants.TOP);

        DefaultRendererFactory rf = new DefaultRendererFactory(nodeRenderer);
        rf.add(new InGroupPredicate(graphEdges), edgeRenderer);
        graphVis.setRendererFactory(rf);

        resetActions();

        graphDisplay = new Display(graphVis);
        graphDisplay.setForeground(Color.BLACK);
        graphDisplay.setBackground(Color.WHITE);

        graphDisplay.addControlListener(new ZoomControl());
        graphDisplay.addControlListener(new ZoomToFitControl());
        graphDisplay.addControlListener(new WheelZoomControl());
        graphDisplay.addControlListener(new PanControl());
        graphDisplay.addControlListener(new DragControl());

        if (title != null)
        {
            final JFastLabel titleLabel = new JFastLabel(title);
            titleLabel.setPreferredSize(new Dimension(400, 20));
            titleLabel.setHorizontalAlignment(SwingConstants.EAST);
            titleLabel.setVerticalAlignment(SwingConstants.TOP);
            titleLabel.setBorder(BorderFactory.createEmptyBorder(3, 0, 0, 0));
            titleLabel.setFont(FontLib.getFont("Tahoma", Font.PLAIN, 12));
            titleLabel.setBackground(Color.WHITE);
            titleLabel.setForeground(Color.BLACK);

            add(titleLabel, BorderLayout.NORTH);
        }
        add(graphDisplay, BorderLayout.CENTER);

    }

    private void resetActions()
    {
        // Clean up
        graphVis.removeAction("animate");
        graphVis.removeAction("filter");
        graphVis.removeAction("subLayout");
        graphVis.removeAction("graphLayout");
        graphVis.removeAction("textColor");
        
        for (Control c : controls)
            graphDisplay.removeControlListener(c);
        controls.clear();

        // Add actions/controls
        if ( g != null && g.getNodeCount() > 0 )
        {
            System.out.println("Resetting actions for graph with " + g.getNodeCount() + " nodes.");

            // colors
            ItemAction nodeColor = new NodeColorAction(graphNodes, groupColors);
            ItemAction edgeColor = new ColorAction(graphEdges,
                    VisualItem.STROKECOLOR, ColorLib.rgb(50, 50, 50));
            ItemAction textColor = new ColorAction(graphNodes,
                    VisualItem.TEXTCOLOR, ColorLib.rgb(0, 0, 0));
            graphVis.putAction("textColor", textColor);
    
            // recolor
            ActionList recolor = new ActionList();
            recolor.add(nodeColor);
            recolor.add(edgeColor);
            recolor.add(textColor);
            graphVis.putAction("recolor", recolor);
            
            // repaint
            ActionList repaint = new ActionList();
            repaint.add(recolor);
            repaint.add(new RepaintAction());
            graphVis.putAction("repaint", repaint);
            
            AutoPanAction autoPan = new AutoPanAction();
            
            /*
            ForceSimulator fsim = new ForceSimulator();
            fsim.addForce(new NBodyForce(0.0f, NBodyForce.DEFAULT_DISTANCE, NBodyForce.DEFAULT_THETA));
            fsim.addForce(new SpringForce(SpringForce.DEFAULT_SPRING_COEFF, 150f));
            fsim.addForce(new DragForce());
            ForceDirectedLayout graphLayout = new ForceDirectedLayout(graph, fsim, false, false);
    
            ActionList animate = new ActionList(Activity.INFINITY);
            animate.add(graphLayout);
            animate.add(new FontAction(graphNodes, FontLib.getFont("Tahoma", 16)));
            animate.add(textColor);
            animate.add(nodeColor);
            animate.add(edgeColor);
            animate.add(autoPan);
            animate.add(new RepaintAction());
            graphVis.putAction("animate", animate);
            */
    
            /*
            NodeLinkTreeLayout graphLayout = new NodeLinkTreeLayout(
                    graph, Constants.ORIENT_TOP_BOTTOM, 50, 40, 8);
            graphLayout.setLayoutAnchor(new Point2D.Double(25, 50));
            */
    
            // create the tree layout action
            RadialTreeLayout graphLayout = new RadialTreeLayout(graph, 150);
            //treeLayout.setAngularBounds(-Math.PI/2, Math.PI);
            graphVis.putAction("graphLayout", graphLayout);
            
            CollapsedSubtreeLayout subLayout = new CollapsedSubtreeLayout(graph);
            graphVis.putAction("subLayout", subLayout);
    
            // create the filtering and layout
            ActionList filter = new ActionList();
            filter.add(new TreeRootAction(graph));
            filter.add(new FontAction(graphNodes, FontLib.getFont("Tahoma", 16)));
            filter.add(graphLayout);
            filter.add(subLayout);
            filter.add(textColor);
            filter.add(nodeColor);
            filter.add(edgeColor);
            graphVis.putAction("filter", filter);
            
            // animated transition
            ActionList animate = new ActionList(1250);
            animate.setPacingFunction(new SlowInSlowOutPacer());
            animate.add(autoPan);
            animate.add(new QualityControlAnimator());
            animate.add(new VisibilityAnimator(graph));
            animate.add(new LocationAnimator(graphNodes));
            animate.add(new ColorAnimator(graphNodes));
            animate.add(new RepaintAction());
            graphVis.putAction("animate", animate);
            graphVis.alwaysRunAfter("filter", "animate");
            
            controls.add(new FocusControl(1, "filter"));
            controls.add(new HoverActionControl("repaint"));

            for (Control c : controls)
                graphDisplay.addControlListener(c);

            // filter graph and perform layout
            graphVis.run("filter");
        }
    }

    public void setDataFromGraphML(String graphData, String label)
    {
        try {
            g = (Graph) new GraphMLReader().readGraph(
                new ByteArrayInputStream(graphData.getBytes()));

        } catch (Exception e) {
            System.err.println("Failed to read GraphML from string data.");
            e.printStackTrace();
        }

        resetVisualization(label);
    }

    public void setData(Graph ng, String label)
    {
        g = ng;
        resetVisualization(label);
    }

    
    private void resetVisualization(String label)
    {
        graphVis.removeGroup(graph);
        graphVis.addGraph(graph, g);

        DefaultRendererFactory drf =
            (DefaultRendererFactory) graphVis.getRendererFactory();
        ((LabelRenderer) drf.getDefaultRenderer()).setTextField(label);

        System.out.println("Redrawing with " + g.getNodeCount() + " nodes.");
        resetActions();
    }

    public void zoomToFit()
    {
        Rectangle2D bounds = graphVis.getBounds(Visualization.ALL_ITEMS);

        System.out.println("bounds: " + bounds);

        GraphicsLib.expand(bounds, 50 + (int) (1 / graphDisplay.getScale()));
        DisplayLib.fitViewToBounds(graphDisplay, bounds, 2000);
    }

    // Helper classes
    public static class TreeRootAction extends GroupAction {
        public TreeRootAction(String graphGroup) {
            super(graphGroup);
        }
        public void run(double frac) {
            TupleSet focus = m_vis.getGroup(Visualization.FOCUS_ITEMS);
            if ( focus==null || focus.getTupleCount() == 0 ) return;
            
            Graph g = (Graph)m_vis.getGroup(m_group);
            Node f = null;
            Iterator<?> tuples = focus.tuples();
            while (tuples.hasNext() )
            {
                Object n = tuples.next();
                if ( n instanceof Node ) {
                    f = (Node) n;
                    if ( g.containsTuple(f) ) break;
                }
            }
            if ( f == null ) return;
            g.getSpanningTree(f);
        }
    }
    
    public class AutoPanAction extends Action
    {
        private Point2D m_start = new Point2D.Double();
        private Point2D m_end = new Point2D.Double();
        private Point2D m_cur = new Point2D.Double();
        private boolean m_init = false;

        public void run(double frac)
        {
            TupleSet ts = m_vis.getFocusGroup(Visualization.FOCUS_ITEMS);
            if (ts.getTupleCount() == 0) return;

            if (frac == 0.0 || !m_init)
            {
                VisualItem vi = (VisualItem) ts.tuples().next();
                m_cur.setLocation(getWidth() / 2, getHeight() / 2);
                m_vis.getDisplay(0).getAbsoluteCoordinate(m_cur, m_start);
                m_end.setLocation(vi.getX(), vi.getY());

                m_init = true;
                // System.out.println("Frac: " + frac +
                // " start: " + m_start + " end: " + m_end + " cur: " + m_cur);
            }
            else
            {
                m_cur.setLocation(m_start.getX() + frac
                        * (m_end.getX() - m_start.getX()), m_start.getY()
                        + frac * (m_end.getY() - m_start.getY()));
                m_vis.getDisplay(0).panToAbs(m_cur);

                // System.out.println("Frac: " + frac +
                // " start: " + m_start + " end: " + m_end + " cur: " + m_cur);
            }

            // Reset for next animation.
            if (frac == 1.0) m_init = false;
        }
    }
    
    public static class NodeColorAction extends ColorAction
    {
        GroupColors groupColors;

        public NodeColorAction(String group, GroupColors gc)
        {
            super(group, VisualItem.FILLCOLOR);
            groupColors = gc;
        }

        public int getColor(VisualItem item)
        {
            for (Map.Entry<String, GroupColors.KeyAndColor> gc :
                    groupColors.colorings.entrySet())
            {
                TupleSet g = m_vis.getGroup(gc.getKey());
                for (Iterator<?> it = g.tuples(); it.hasNext();)
                {
                    String key = gc.getValue().key;
                    Tuple t = (Tuple) it.next();
                    if (t.get(key).equals(item.get(key))) { return gc
                            .getValue().color; }
                }
            }

            if (m_vis.isInGroup(item, Visualization.SEARCH_ITEMS))
                return ColorLib.rgb(255, 190, 190);
            
            else if (m_vis.isInGroup(item, Visualization.FOCUS_ITEMS))
                return ColorLib.rgb(198, 229, 229);

            else if (item.getDOI() > -1) return ColorLib.rgb(164, 193, 193);
            
            else return ColorLib.rgba(255, 255, 255, 0);
        }
    }
}
