package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Panel;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.geom.Point2D;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.management.Query;
import javax.swing.AbstractAction;
import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.JRootPane;
import javax.swing.JSplitPane;
import javax.swing.KeyStroke;
import javax.swing.SwingConstants;
import javax.swing.border.Border;
import javax.swing.event.MouseInputAdapter;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IProjectDescription;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.resources.IWorkspace;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.ide.IDE;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.part.ViewPart;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.internal.image.GIFFileFormat;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;

import org.eclipse.swt.SWT;

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
import prefuse.data.Node;
import prefuse.data.Tree;
import prefuse.data.io.TreeMLReader;
import prefuse.data.tuple.TupleSet;
import prefuse.render.AbstractShapeRenderer;
import prefuse.render.DefaultRendererFactory;
import prefuse.render.EdgeRenderer;
import prefuse.render.LabelRenderer;
import prefuse.util.ColorLib;
import prefuse.util.FontLib;
import prefuse.util.ui.JFastLabel;
import prefuse.visual.VisualItem;
import prefuse.visual.expression.InGroupPredicate;
import prefuse.visual.sort.TreeDepthItemSorter;


public class CodeEditorView extends ViewPart {

	public static final String ID = "dbtoaster_gui.codeeditorview";
	
	private TreeVisPanel exprPanel;
	private JPanel workflowPanel;
	private TreeVisPanel mapPanel;
	private TreeVisPanel handlerPanel;

	public void createPartControl(Composite parent) {
	
		IWorkspace ws = ResourcesPlugin.getWorkspace();
		IProjectDescription projDesc = ws.newProjectDescription("DBToasterCode");
		projDesc.setLocation(new Path("/Users/yanif/tmp/dbtcode"));
		//projDesc.setComment("DBToaster compiled code");
		IProject p = ws.getRoot().getProject("DBToasterCode");
		
		try {
			if ( !p.exists() )
				p.create(projDesc, null);
			p.open(null);
			
			IFile file = p.getFile(new Path("vwap.cc"));
			if ( !file.exists() )
				file.create(new ByteArrayInputStream(new byte[0]), IResource.NONE, null);
			
			IDE.openEditor(getViewSite().getPage(), file, true);

		} catch (PartInitException e) {
			e.printStackTrace();
		} catch (CoreException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Composite top = new Composite(parent, SWT.EMBEDDED);
		GridLayout layout = new GridLayout();
		layout.marginWidth = 0;
		layout.marginHeight = 0;
		top.setLayout(layout);
		top.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		// Initialize default trees.
        Tree exprTree = null;
        try {
            exprTree = (Tree)new TreeMLReader().readGraph("/Users/yanif/tmp/dbtcode/vwap.tml");
        } catch ( Exception e ) {
            e.printStackTrace();
            System.exit(1);
        }
        
		Tree mapTree = new Tree();
		mapTree.getNodeTable().addColumn("name", String.class);
		Node root = mapTree.addRoot();
		root.set("name", "q[pmin]");
		
		Node mp2 = mapTree.addChild(root);
		mp2.set("name", "m[p2]");
		
		Node sv0 = mapTree.addChild(mp2);
		sv0.set("name", "sv0");
		
		Node sv1 = mapTree.addChild(mp2);
		sv1.set("name", "sv1[p]");

		Tree handlerTree = new Tree();
		handlerTree.getNodeTable().addColumn("name", String.class);
		Node droot = handlerTree.addRoot();
		droot.set("name", "result");
		
		Node ib = handlerTree.addChild(droot);
		ib.set("name", "insert(bids)");
		
		Node db = handlerTree.addChild(droot);
		db.set("name", "delete(bids)");
		
		exprPanel = new TreeVisPanel(exprTree, "op", "Map expression", Constants.ORIENT_TOP_BOTTOM);
		workflowPanel = buildWorkflowTree();
		mapPanel = new TreeVisPanel(mapTree, "name", "Data structures", Constants.ORIENT_LEFT_RIGHT);
		handlerPanel = new TreeVisPanel(handlerTree, "name", "Handler functions", Constants.ORIENT_LEFT_RIGHT);
		
		Frame codeFrame = SWT_AWT.new_Frame(top);
		codeFrame.setLayout(new BorderLayout());
        Panel codePanel = new Panel();
        codeFrame.add(codePanel, BorderLayout.CENTER);
        
        JRootPane codeRootPane = new JRootPane();
        codePanel.setLayout(new BorderLayout());
 		codePanel.add(codeRootPane, BorderLayout.CENTER);
		
 		JSplitPane split1 = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        split1.setLeftComponent(mapPanel);
        split1.setRightComponent(handlerPanel);
        split1.setOneTouchExpandable(false);
        split1.setContinuousLayout(false);
        split1.setDividerSize(2);
        split1.setResizeWeight(0.5);
 		
        JSplitPane split2 = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        split2.setLeftComponent(exprPanel);
        split2.setRightComponent(workflowPanel);
        split2.setOneTouchExpandable(false);
        split2.setContinuousLayout(false);
        split2.setDividerSize(2);
        split2.setResizeWeight(0.75);
        split2.setDividerLocation(0.75);
        
        JSplitPane split3 = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        split3.setLeftComponent(split2);
        split3.setRightComponent(split1);
        split3.setOneTouchExpandable(false);
        split3.setContinuousLayout(false);
        split3.setDividerSize(1);
        split3.setResizeWeight(0.6);
        
        Container codeContentPane = codeRootPane.getContentPane();
        codeContentPane.setLayout(new BorderLayout());
        codeContentPane.add(split3, BorderLayout.CENTER);

        codeFrame.pack();
        codeFrame.setVisible(true);
 	}
	
	// Workflow panel helpers.
	
	// stage name -> data group.
	static class WorkflowStages {
		static LinkedHashMap<String, String> workflowStages;
		public static LinkedHashMap<String, String> get() {
			if ( workflowStages == null ) {
				workflowStages = new LinkedHashMap<String, String>();
				workflowStages.put("SQL Query", null);
				workflowStages.put("Parse", null);
				workflowStages.put("Extract bindings", "extract");
				workflowStages.put("Find delta frontier", "incr");
				workflowStages.put("Recur over inputs", null);
				workflowStages.put("   Simplify deltas", "deltas");
				workflowStages.put("   Simplify aggregates", "aggs");
				workflowStages.put("   Code generation", "generate");
				workflowStages.put("C++ code", null);
			}
			return workflowStages;
		}
	};
	
	JFastLabel buildWorkflowLabel(String group, String text,
		Border b, int fontSize)
	{
        JFastLabel label;
    	if ( group == null )
    		label = new JFastLabel(text);
    	else {
    		WorkflowLabel r = new WorkflowLabel(group, text);
            r.addMouseListener(
        		new WorkflowStageMouseListener(r, exprPanel));
            label = (JFastLabel) r;
    	}

        label.setPreferredSize(new Dimension(135, 20));
        label.setHorizontalAlignment(SwingConstants.EAST);
        label.setVerticalAlignment(SwingConstants.TOP);
        label.setBorder(b);
        label.setFont(FontLib.getFont("Tahoma", Font.PLAIN, fontSize));
        label.setBackground(Color.WHITE);
        label.setForeground(Color.BLACK);
        
        return label;
    }
	
	JFastLabel buildWorkflowStage(String group, String stage) {
		return
			buildWorkflowLabel(group, stage,
				BorderFactory.createEmptyBorder(1,0,0,0), 14);
	}

	JPanel buildWorkflowTree() {
		
		JPanel r = new JPanel();
		FlowLayout l = new FlowLayout();
		l.setHgap(0);
		l.setVgap(0);
		r.setLayout(l);
		r.setBackground(Color.WHITE);
		r.setForeground(Color.BLACK);

		r.add(buildWorkflowLabel(null, "Workflow",
			BorderFactory.createMatteBorder(0, 0, 3, 0, Color.DARK_GRAY), 16));

		// Add stages.
		for (Map.Entry<String, String> stage :
				WorkflowStages.get().entrySet())
		{
			r.add(buildWorkflowStage(stage.getValue(), stage.getKey()));
		}
        
        return r;
	}
	
	public void setFocus() {}
	
	// Helper classes
	class WorkflowLabel extends JFastLabel {
		private static final long serialVersionUID = -7295730416631024647L;

		String group;
		public WorkflowLabel(String group, String text) {
			super(text);
			this.group = group;
		}
	}
	
	class WorkflowStageMouseListener extends MouseInputAdapter {
		WorkflowLabel label;
		TreeVisPanel mapExprPanel;
		
		public WorkflowStageMouseListener(WorkflowLabel l, TreeVisPanel tp) {
			label = l;
			mapExprPanel = tp;
		}
		
		public void mouseClicked(MouseEvent e) {
			System.out.println("Clicked: " + label.getText());
		}
		
		public void mouseEntered(MouseEvent e) {
			label.setBackground(Color.LIGHT_GRAY);
		}
		
		public void mouseExited(MouseEvent e) {
			label.setBackground(Color.WHITE);
		}
	}
}