package org.dbtoaster.gui;

import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.WorkbenchException;
import org.eclipse.ui.application.IWorkbenchConfigurer;
import org.eclipse.ui.application.IWorkbenchWindowConfigurer;
import org.eclipse.ui.application.WorkbenchAdvisor;
import org.eclipse.ui.application.WorkbenchWindowAdvisor;

public class ApplicationWorkbenchAdvisor extends WorkbenchAdvisor {

	private static final String PERSPECTIVE_ID = "dbtoaster_gui.compile_perspective";

	public WorkbenchWindowAdvisor createWorkbenchWindowAdvisor(
			IWorkbenchWindowConfigurer configurer)
	{
		return new ApplicationWorkbenchWindowAdvisor(configurer);
	}

	public String getInitialWindowPerspectiveId() {
		// Open up the compilation perspective.
		return PERSPECTIVE_ID;
	}
	
	public void postStartup()
	{
		// Open up the performance perspective
		try {
			IWorkbenchConfigurer wc = getWorkbenchConfigurer();
			wc.setData("perspective.id", PerformancePerspective.ID);
			getWorkbenchConfigurer().getWorkbench().
				openWorkbenchWindow(PerformancePerspective.ID, null);
		} catch (WorkbenchException e) {
			e.printStackTrace();
		}
	}

}
