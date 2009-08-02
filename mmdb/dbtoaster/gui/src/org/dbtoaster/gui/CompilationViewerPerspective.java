package org.dbtoaster.gui;

import org.eclipse.ui.IPageLayout;
import org.eclipse.ui.IPerspectiveFactory;

public class CompilationViewerPerspective implements IPerspectiveFactory
{

    public static final String ID = "dbtoaster_gui.compilation_viewer_perspective";

    public void createInitialLayout(IPageLayout layout)
    {
        String editorArea = layout.getEditorArea();
        layout.setFixed(true);

        layout.addPerspectiveShortcut(EditorPerspective.ID);
        layout.addView(CodeEditorView.ID, IPageLayout.LEFT, 0.65f, editorArea);
    }

}
