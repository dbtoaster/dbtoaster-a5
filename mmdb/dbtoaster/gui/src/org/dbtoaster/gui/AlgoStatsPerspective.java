package org.dbtoaster.gui;

import org.eclipse.ui.IFolderLayout;
import org.eclipse.ui.IPageLayout;
import org.eclipse.ui.IPerspectiveFactory;

public class AlgoStatsPerspective implements IPerspectiveFactory
{

	public static final String ID = "dbtoaster_gui.algo_stats_perspective";

    public void createInitialLayout(IPageLayout layout)
    {
    	String editorArea = layout.getEditorArea();
        layout.setEditorAreaVisible(false);
        layout.setFixed(true);

        layout.addPerspectiveShortcut(PerformancePerspective.ID);
        layout.addPerspectiveShortcut(CodePerfPerspective.ID);
        layout.addView(AlgoDataView.ID, IPageLayout.LEFT, 0.5f, editorArea);
    }
}
