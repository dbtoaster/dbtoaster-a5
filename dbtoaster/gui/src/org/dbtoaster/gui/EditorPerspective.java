package org.dbtoaster.gui;

import org.eclipse.ui.IPageLayout;
import org.eclipse.ui.IPerspectiveFactory;

public class EditorPerspective implements IPerspectiveFactory
{

    public static final String ID = "dbtoaster_gui.editor_perspective";

    public void createInitialLayout(IPageLayout layout)
    {
        String editorArea = layout.getEditorArea();
        layout.setEditorAreaVisible(false);
        layout.setFixed(true);

        layout.addPerspectiveShortcut(CompilationViewerPerspective.ID);
        // TODO: add a dataset/schema+relation viewer
        layout.addView(QueryEditor.ID, IPageLayout.LEFT, 0.5f, editorArea);
        // layout.addView(CodeEditorView.ID, IPageLayout.BOTTOM, 0.5f,
        // editorArea);
    }

}
