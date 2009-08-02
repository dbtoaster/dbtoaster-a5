package org.dbtoaster.gui;

import org.eclipse.ui.IPageLayout;
import org.eclipse.ui.IPerspectiveFactory;

public class PerformancePerspective implements IPerspectiveFactory
{

    public static final String ID = "dbtoaster_gui.performance_perspective";

    public void createInitialLayout(IPageLayout layout)
    {
        String editorArea = layout.getEditorArea();
        layout.setEditorAreaVisible(false);
        layout.setFixed(true);

        layout.addPerspectiveShortcut(CodePerfPerspective.ID);
        layout.addView(PerfView.ID, IPageLayout.BOTTOM, 0.5f, editorArea);
    }

}
