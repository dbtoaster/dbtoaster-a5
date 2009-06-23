package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Panel;

import javax.swing.JRootPane;

import org.eclipse.swt.SWT;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.part.ViewPart;

import prefuse.data.Node;
import prefuse.data.Tree;

public class CodeView extends ViewPart {

	public static final String ID = "dbtoaster_gui.codeview";
	
	TreeVisPanel codeTreePanel;

	public void createPartControl(Composite parent) {
		Composite top = new Composite(parent, SWT.EMBEDDED);
		GridLayout layout = new GridLayout();
		layout.marginWidth = 0;
		layout.marginHeight = 0;
		top.setLayout(layout);
		top.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
		
		Frame codeFrame = SWT_AWT.new_Frame(top);
		codeFrame.setLayout(new BorderLayout());
        Panel codePanel = new Panel();
        codeFrame.add(codePanel, BorderLayout.CENTER);

        JRootPane codeRootPane = new JRootPane();
        codePanel.setLayout(new BorderLayout());
 		codePanel.add(codeRootPane, BorderLayout.CENTER);

 		Tree codeTree = buildVwapCodeTree();
        codeTreePanel = new TreeVisPanel(codeTree, "code", "C++ QP");
	
        Container codeContentPane = codeRootPane.getContentPane();
        codeContentPane.setLayout(new BorderLayout());
        codeContentPane.add(codeTreePanel, BorderLayout.CENTER);

        codeFrame.pack();
        codeFrame.setVisible(true);
	}

	public void setFocus() {
	}


	Tree buildVwapCodeTree()
	{
		Tree codeTree = new Tree();
 		codeTree.getNodeTable().addColumn("code", String.class);

 		Node codeRoot = codeTree.addRoot();
 		codeRoot.set("code", "<global scope>");

 		Node onInsertBids = codeTree.addChild(codeRoot);
 		onInsertBids.set("code", "on_insert_B(p,v)");

 		// maintain q[pmin]
 		Node maintainQPmin = codeTree.addChild(onInsertBids);
 		maintainQPmin.set("code", "<maintain q[pmin]>");
 		
 		// update q[pmin]
 		Node qpminForLoop = codeTree.addChild(maintainQPmin);
 		qpminForLoop.set("code", "foreach(pmin in q.keys())");
 		
 		Node qpminIncr = codeTree.addChild(qpminForLoop);
 		qpminIncr.set("code", "q[pmin] += (pmin > p? p * v : 0);");
 		
 		// insert q[pmin]
 		Node qpminMissing = codeTree.addChild(maintainQPmin);
 		qpminMissing.set("code", "if (!q.hasKey(pmin))");
 		
 		Node newQPmin = codeTree.addChild(qpminMissing);
 		newQPmin.set("code", "pmin_lb = q.lower_bound(pmin);\nq[pmin] = q[pmin_ln] + (p*v);");
 		
 		// maintain m[p2]
 		Node maintainMP2 = codeTree.addChild(onInsertBids);
 		maintainMP2.set("code", "<maintain m[p2]>");
 		
 		Node mp2ForLoop = codeTree.addChild(maintainMP2);
 		mp2ForLoop.set("code", "foreach(p2 in m.keys())");
 		
 		Node mp2Incr = codeTree.addChild(mp2ForLoop);
 		mp2Incr.set("code", "m[p2] += 0.25*v - (p > p2? v : 0)");

 		// insert m[p2]
 		Node mp2Missing = codeTree.addChild(maintainMP2);
 		mp2Missing.set("code", "if (!m.hasKey(p2))");
 		
 		Node newMP2 = codeTree.addChild(mp2Missing);
 		newMP2.set("code", "p2_ub = m.upper_bound(p2);\nm[p2] = m[p2_ub] + (0.25*v - v);");

 		// compute result pmin
 		Node computePmin = codeTree.addChild(onInsertBids);
 		computePmin.set("code", "<compute result pmin>");
 		
 		Node pminForLoop = codeTree.addChild(computePmin);
 		pminForLoop.set("code", "pmin = infinity; foreach(m[p2])");
 		
 		Node pminIncr = codeTree.addChild(pminForLoop);
 		pminIncr.set("code", "pmin = m[p2] > 0? min(pmin, p2) : pmin;");
 		
 		// compute result
 		Node computeResult = codeTree.addChild(onInsertBids);
 		computeResult.set("code", "return q[pmin]");
 		
 		return codeTree;
	}

}
