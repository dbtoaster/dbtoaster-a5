package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.AbstractAction;
import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.KeyStroke;
import javax.swing.SwingConstants;

import prefuse.Constants;
import prefuse.Display;
import prefuse.Visualization;
import prefuse.action.Action;
import prefuse.action.ActionList;
import prefuse.action.ItemAction;
import prefuse.action.RepaintAction;
import prefuse.action.animate.ColorAnimator;
import prefuse.action.animate.LocationAnimator;
import prefuse.action.animate.QualityControlAnimator;
import prefuse.action.animate.VisibilityAnimator;
import prefuse.action.assignment.ColorAction;
import prefuse.action.assignment.FontAction;
import prefuse.action.filter.FisheyeTreeFilter;
import prefuse.action.layout.CollapsedSubtreeLayout;
import prefuse.action.layout.graph.NodeLinkTreeLayout;
import prefuse.activity.SlowInSlowOutPacer;
import prefuse.controls.DragControl;
import prefuse.controls.FocusControl;
import prefuse.controls.PanControl;
import prefuse.controls.WheelZoomControl;
import prefuse.controls.ZoomControl;
import prefuse.controls.ZoomToFitControl;
import prefuse.data.Table;
import prefuse.data.Tree;
import prefuse.data.Tuple;
import prefuse.data.expression.Predicate;
import prefuse.data.expression.parser.ExpressionParser;
import prefuse.data.io.TreeMLReader;
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
import prefuse.visual.sort.TreeDepthItemSorter;

public class TreeVisPanel extends JPanel {

	private static final long serialVersionUID = 6608462775015056030L;

    private static final String tree = "tree";
    private static final String treeNodes = "tree.nodes";
    private static final String treeEdges = "tree.edges";
    
	Visualization treeVis;
	Display treeDisplay;
	Tree t;
	
    private LabelRenderer nodeRenderer;
    private EdgeRenderer edgeRenderer;
    
    private int tOrientation = Constants.ORIENT_TOP_BOTTOM;
    
    class GroupColors {
    	public class KeyAndColor {
    		public String key; public Integer color;
    		public KeyAndColor(String k, Integer c) { 
    			key = k; color = c;
    		}
    	};
    	
    	public HashMap<String, KeyAndColor> colorings;
    	
    	public GroupColors() {
    		colorings = new HashMap<String, KeyAndColor>();
		}
    	
    	public void put(String group, String key, Integer color) {
    		colorings.put(group, new KeyAndColor(key, color));
    	}
    	
    	public KeyAndColor get(String group) {
    		return colorings.get(group);
    	}
    };
    
    GroupColors groupColors;

    public TreeVisPanel(String title) {
    	this(new Tree(), "name", title, Constants.ORIENT_TOP_BOTTOM);
    }

	public TreeVisPanel(Tree t, String labelField, String title) {
		this(t, labelField, title, Constants.ORIENT_TOP_BOTTOM);
	}
	
	public TreeVisPanel(Tree t, String labelField, String title,
			int orientation)
	{
		setLayout(new BorderLayout());
		treeVis = new Visualization();
		groupColors = new GroupColors();
		
		this.t = t;
		treeVis.add(tree, t);
		tOrientation = orientation;

        nodeRenderer = new LabelRenderer(labelField);
        nodeRenderer.setRenderType(AbstractShapeRenderer.RENDER_TYPE_FILL);
        nodeRenderer.setHorizontalAlignment(Constants.CENTER);
        nodeRenderer.setRoundedCorner(8,8);

        edgeRenderer = new EdgeRenderer(Constants.EDGE_TYPE_CURVE);
        edgeRenderer.setHorizontalAlignment1(Constants.CENTER);
        edgeRenderer.setHorizontalAlignment2(Constants.CENTER);
        edgeRenderer.setVerticalAlignment1(Constants.BOTTOM);
        edgeRenderer.setVerticalAlignment2(Constants.TOP);
        
        DefaultRendererFactory rf = new DefaultRendererFactory(nodeRenderer);
        rf.add(new InGroupPredicate(treeEdges), edgeRenderer);
        treeVis.setRendererFactory(rf);
        
        // colors
        ItemAction nodeColor = new NodeColorAction(treeNodes, groupColors);
        ItemAction edgeColor = new ColorAction(treeEdges,
            VisualItem.STROKECOLOR, ColorLib.rgb(200,200,200));
        ItemAction textColor = new ColorAction(treeNodes,
            VisualItem.TEXTCOLOR, ColorLib.rgb(0,0,0));
        treeVis.putAction("textColor", textColor);

        // quick repaint
        ActionList repaint = new ActionList();
        repaint.add(nodeColor);
        repaint.add(new RepaintAction());
        treeVis.putAction("repaint", repaint);
        
        // create the tree layout action
        NodeLinkTreeLayout treeLayout = new NodeLinkTreeLayout(tree,
            tOrientation, 50, 40, 8);
        treeLayout.setLayoutAnchor(new Point2D.Double(25,50));
        treeVis.putAction("treeLayout", treeLayout);
        
        CollapsedSubtreeLayout subLayout = 
            new CollapsedSubtreeLayout(tree, tOrientation);
        treeVis.putAction("subLayout", subLayout);

        treeLayout.setOrientation(tOrientation);
        subLayout.setOrientation(tOrientation);
        
        AutoPanAction autoPan = new AutoPanAction();

        // create the filtering and layout
        ActionList filter = new ActionList();
        filter.add(new FisheyeTreeFilter(tree, 10));
        filter.add(new FontAction(treeNodes, FontLib.getFont("Tahoma", 16)));
        filter.add(treeLayout);
        filter.add(subLayout);
        filter.add(textColor);
        filter.add(nodeColor);
        filter.add(edgeColor);
        treeVis.putAction("filter", filter);
        
        // animated transition
        ActionList animate = new ActionList(1000);
        animate.setPacingFunction(new SlowInSlowOutPacer());
        animate.add(autoPan);
        animate.add(new QualityControlAnimator());
        animate.add(new VisibilityAnimator(tree));
        animate.add(new LocationAnimator(treeNodes));
        animate.add(new ColorAnimator(treeNodes));
        animate.add(new RepaintAction());
        treeVis.putAction("animate", animate);
        treeVis.alwaysRunAfter("filter", "animate");
        
        treeDisplay = new Display(treeVis);
        treeDisplay.setForeground(Color.BLACK);
        treeDisplay.setBackground(Color.WHITE);

        treeDisplay.setItemSorter(new TreeDepthItemSorter());
        treeDisplay.addControlListener(new ZoomControl());
        treeDisplay.addControlListener(new ZoomToFitControl());
        treeDisplay.addControlListener(new WheelZoomControl());
        treeDisplay.addControlListener(new PanControl());
        treeDisplay.addControlListener(new DragControl());
        treeDisplay.addControlListener(new FocusControl(1, "filter"));
        
        /*
        display.registerKeyboardAction(
            new OrientAction(treeVis, Constants.ORIENT_LEFT_RIGHT),
            "left-to-right", KeyStroke.getKeyStroke("ctrl 1"), WHEN_FOCUSED);
        display.registerKeyboardAction(
            new OrientAction(treeVis, Constants.ORIENT_TOP_BOTTOM),
            "top-to-bottom", KeyStroke.getKeyStroke("ctrl 2"), WHEN_FOCUSED);
        display.registerKeyboardAction(
            new OrientAction(treeVis, Constants.ORIENT_RIGHT_LEFT),
            "right-to-left", KeyStroke.getKeyStroke("ctrl 3"), WHEN_FOCUSED);
        display.registerKeyboardAction(
            new OrientAction(treeVis, Constants.ORIENT_BOTTOM_TOP),
            "bottom-to-top", KeyStroke.getKeyStroke("ctrl 4"), WHEN_FOCUSED);
        */
        
        setOrientation(tOrientation);
        treeVis.run("filter");

        if ( title != null ) {
	        final JFastLabel titleLabel = new JFastLabel(title);
	        titleLabel.setPreferredSize(new Dimension(400, 20));
	        titleLabel.setHorizontalAlignment(SwingConstants.EAST);
	        titleLabel.setVerticalAlignment(SwingConstants.TOP);
	        titleLabel.setBorder(BorderFactory.createEmptyBorder(3,0,0,0));
	        titleLabel.setFont(FontLib.getFont("Tahoma", Font.PLAIN, 12));
	        titleLabel.setBackground(Color.WHITE);
	        titleLabel.setForeground(Color.BLACK);
	        
	        add(titleLabel, BorderLayout.NORTH);
        }
        add(treeDisplay, BorderLayout.CENTER);
	}

    public void setOrientation(int orientation) {
        NodeLinkTreeLayout rtl 
            = (NodeLinkTreeLayout)treeVis.getAction("treeLayout");
        CollapsedSubtreeLayout stl
            = (CollapsedSubtreeLayout)treeVis.getAction("subLayout");
        switch ( orientation ) {
        case Constants.ORIENT_LEFT_RIGHT:
            nodeRenderer.setHorizontalAlignment(Constants.LEFT);
            edgeRenderer.setHorizontalAlignment1(Constants.RIGHT);
            edgeRenderer.setHorizontalAlignment2(Constants.LEFT);
            edgeRenderer.setVerticalAlignment1(Constants.CENTER);
            edgeRenderer.setVerticalAlignment2(Constants.CENTER);
            break;
        case Constants.ORIENT_RIGHT_LEFT:
            nodeRenderer.setHorizontalAlignment(Constants.RIGHT);
            edgeRenderer.setHorizontalAlignment1(Constants.LEFT);
            edgeRenderer.setHorizontalAlignment2(Constants.RIGHT);
            edgeRenderer.setVerticalAlignment1(Constants.CENTER);
            edgeRenderer.setVerticalAlignment2(Constants.CENTER);
            break;
        case Constants.ORIENT_TOP_BOTTOM:
            nodeRenderer.setHorizontalAlignment(Constants.CENTER);
            edgeRenderer.setHorizontalAlignment1(Constants.CENTER);
            edgeRenderer.setHorizontalAlignment2(Constants.CENTER);
            edgeRenderer.setVerticalAlignment1(Constants.BOTTOM);
            edgeRenderer.setVerticalAlignment2(Constants.TOP);
            break;
        case Constants.ORIENT_BOTTOM_TOP:
            nodeRenderer.setHorizontalAlignment(Constants.CENTER);
            edgeRenderer.setHorizontalAlignment1(Constants.CENTER);
            edgeRenderer.setHorizontalAlignment2(Constants.CENTER);
            edgeRenderer.setVerticalAlignment1(Constants.TOP);
            edgeRenderer.setVerticalAlignment2(Constants.BOTTOM);
            break;
        default:
            throw new IllegalArgumentException(
                "Unrecognized orientation value: "+orientation);
        }
        tOrientation = orientation;
        rtl.setOrientation(orientation);
        stl.setOrientation(orientation);
    }
    
    public void setDataFromTreeML(String treeData, String label)
    {
    	t.clear();
        try {
            t = (Tree)new TreeMLReader().readGraph(
        		new ByteArrayInputStream(treeData.getBytes()));
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        
        treeVis.removeGroup(tree);
        treeVis.addTree(tree, t);
        
        DefaultRendererFactory drf =
        	(DefaultRendererFactory) treeVis.getRendererFactory();
        ((LabelRenderer) drf.getDefaultRenderer()).setTextField(label);
        
        treeVis.run("filter");
    }

    public void selectNodes(String group, String key, String pred, int color) {
    	if ( groupColors.colorings.containsKey(group) ) {
    		System.out.println("WARN: TreeVisPanel found group " + group);
    	}
    	else {
			Predicate selectPredicate =
				(Predicate)ExpressionParser.parse(pred);
			
			Table selectedNodes = 
				t.getNodeTable().select(selectPredicate, null);
			
			System.out.println("Selected " + selectedNodes.getTupleCount() + " in group " + group);
			
			treeVis.addFocusGroup(group, selectedNodes);
			
			groupColors.put(group, key, color);
    	}
    }

    public void zoomToFit() {
        Rectangle2D bounds = treeVis.getBounds(Visualization.ALL_ITEMS);
        
        System.out.println("bounds: " + bounds);
        
        GraphicsLib.expand(bounds, 50 + (int)(1/treeDisplay.getScale()));
        DisplayLib.fitViewToBounds(treeDisplay, bounds, 2000);
    }
 
	// Helper classes
    /*
    public class OrientAction extends AbstractAction {
        private static final long serialVersionUID = -4600096370605598712L;
		
        private int orientation;
        Visualization vis;
        
        public OrientAction(Visualization v, int orientation) {
            this.orientation = orientation;
            this.vis = v;
        }
        public void actionPerformed(ActionEvent evt) {
            setOrientation(orientation);
            vis.cancel("orient");
            vis.run("treeLayout");
            vis.run("orient");
        }
    }
    */
    
    
    public class AutoPanAction extends Action {
        private Point2D m_start = new Point2D.Double();
        private Point2D m_end   = new Point2D.Double();
        private Point2D m_cur   = new Point2D.Double();
        private int     m_bias  = 50;
        private boolean m_init  = false;
        
        public void run(double frac) {
            TupleSet ts = m_vis.getFocusGroup(Visualization.FOCUS_ITEMS);
            if ( ts.getTupleCount() == 0 )
                return;
            
            if ( frac == 0.0 || !m_init ) {
                int xbias=0, ybias=0;
                switch ( tOrientation ) {
                case Constants.ORIENT_LEFT_RIGHT:
                    xbias = m_bias;
                    break;
                case Constants.ORIENT_RIGHT_LEFT:
                    xbias = -m_bias;
                    break;
                case Constants.ORIENT_TOP_BOTTOM:
                    ybias = m_bias;
                    break;
                case Constants.ORIENT_BOTTOM_TOP:
                    ybias = -m_bias;
                    break;
                }

                VisualItem vi = (VisualItem)ts.tuples().next();
                m_cur.setLocation(getWidth()/2, getHeight()/2);
                m_vis.getDisplay(0).getAbsoluteCoordinate(m_cur, m_start);
                m_end.setLocation(vi.getX()+xbias, vi.getY()+ybias);
                
                m_init = true;
                //System.out.println("Frac: " + frac +
            	//		" start: " + m_start + " end: " + m_end + " cur: " + m_cur);
            } else {
                m_cur.setLocation(m_start.getX() + frac*(m_end.getX()-m_start.getX()),
                                  m_start.getY() + frac*(m_end.getY()-m_start.getY()));
                m_vis.getDisplay(0).panToAbs(m_cur);

                //System.out.println("Frac: " + frac +
            	//		" start: " + m_start + " end: " + m_end + " cur: " + m_cur);
            }
            
            // Reset for next animation.
            if ( frac == 1.0 )
            	m_init = false;
        }
    }

    public static class NodeColorAction extends ColorAction {
        
    	GroupColors groupColors;
        public NodeColorAction(String group, GroupColors gc) {
            super(group, VisualItem.FILLCOLOR);
            groupColors = gc;
        }
        
        public int getColor(VisualItem item) {
        	for (Map.Entry<String, GroupColors.KeyAndColor> gc :
        		groupColors.colorings.entrySet())
        	{
        		TupleSet g = m_vis.getGroup(gc.getKey());
        		for (Iterator it = g.tuples(); it.hasNext(); ) {
        			String key = gc.getValue().key;
        			Tuple t = (Tuple) it.next();
        			if ( t.get(key).equals(item.get(key)) ) {
        				return gc.getValue().color;
        			}
        		}
        	}
        	
            if ( m_vis.isInGroup(item, Visualization.SEARCH_ITEMS) )
                return ColorLib.rgb(255,190,190);
            else if ( m_vis.isInGroup(item, Visualization.FOCUS_ITEMS) )
                return ColorLib.rgb(198,229,229);
            else if ( item.getDOI() > -1 )
                return ColorLib.rgb(164,193,193);
            else
                return ColorLib.rgba(255,255,255,0);
        }
    }
}
