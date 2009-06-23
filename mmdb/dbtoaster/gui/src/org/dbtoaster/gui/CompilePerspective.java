package org.dbtoaster.gui;

import org.eclipse.ui.IPageLayout;
import org.eclipse.ui.IPerspectiveFactory;

public class CompilePerspective implements IPerspectiveFactory {

	public void createInitialLayout(IPageLayout layout) {
		String editorArea = layout.getEditorArea();
		//layout.setEditorAreaVisible(false);
		layout.setFixed(true);
		
		layout.addView(QueryEditor.ID, IPageLayout.LEFT, 0.5f, editorArea);
		layout.addView(CodeEditorView.ID, IPageLayout.BOTTOM, 0.5f, editorArea);
	}

}
