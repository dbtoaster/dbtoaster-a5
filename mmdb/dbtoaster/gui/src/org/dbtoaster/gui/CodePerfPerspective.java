package org.dbtoaster.gui;

import org.eclipse.ui.IFolderLayout;
import org.eclipse.ui.IPageLayout;
import org.eclipse.ui.IPerspectiveFactory;

public class CodePerfPerspective implements IPerspectiveFactory
{

    public static final String ID = "dbtoaster_gui.codeperf_perspective";

    public void createInitialLayout(IPageLayout layout)
    {
        String editorArea = layout.getEditorArea();
        layout.setEditorAreaVisible(false);
        layout.setFixed(true);

        layout.addPerspectiveShortcut(PerformancePerspective.ID);
        layout.addView(CodeView.ID, IPageLayout.LEFT, 0.5f, editorArea);

        IFolderLayout bottomFolder = layout.createFolder("BottomFolder",
                IPageLayout.BOTTOM, 0.5f, CodeView.ID);
        bottomFolder.addView(CodePerfView.ID);
        bottomFolder.addView(DebugView.ID);
    }

}
