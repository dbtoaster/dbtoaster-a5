package org.dbtoaster.gui;

import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.WorkbenchException;
import org.eclipse.ui.application.IWorkbenchConfigurer;
import org.eclipse.ui.application.IWorkbenchWindowConfigurer;
import org.eclipse.ui.application.WorkbenchAdvisor;
import org.eclipse.ui.application.WorkbenchWindowAdvisor;

public class ApplicationWorkbenchAdvisor extends WorkbenchAdvisor
{

    public WorkbenchWindowAdvisor createWorkbenchWindowAdvisor(
            IWorkbenchWindowConfigurer configurer)
    {
        return new ApplicationWorkbenchWindowAdvisor(configurer);
    }

    public String getInitialWindowPerspectiveId()
    {
        // Open up the query editor perspective.
        return EditorPerspective.ID;
    }

    public void postStartup()
    {
        // Open up the performance perspective
        try
        {
            IWorkbenchConfigurer wc = getWorkbenchConfigurer();
            getWorkbenchConfigurer().getWorkbench().openWorkbenchWindow(
                    PerformancePerspective.ID, null);
        } catch (WorkbenchException e)
        {
            e.printStackTrace();
        }
    }

}
