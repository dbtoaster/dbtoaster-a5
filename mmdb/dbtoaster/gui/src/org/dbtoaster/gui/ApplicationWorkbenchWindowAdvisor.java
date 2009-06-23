package org.dbtoaster.gui;

import org.eclipse.swt.graphics.Point;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.application.ActionBarAdvisor;
import org.eclipse.ui.application.IActionBarConfigurer;
import org.eclipse.ui.application.IWorkbenchWindowConfigurer;
import org.eclipse.ui.application.WorkbenchWindowAdvisor;

public class ApplicationWorkbenchWindowAdvisor extends WorkbenchWindowAdvisor {

	public ApplicationWorkbenchWindowAdvisor(IWorkbenchWindowConfigurer configurer) {
		super(configurer);
	}

	public ActionBarAdvisor createActionBarAdvisor(
			IActionBarConfigurer configurer) {
		return new ApplicationActionBarAdvisor(configurer);
	}

	public void preWindowOpen() {
		IWorkbenchWindowConfigurer configurer = getWindowConfigurer();
		configurer.setInitialSize(new Point(700, 600));
		configurer.setShowCoolBar(false);
		configurer.setShowStatusLine(false);
		configurer.setTitle("DBToaster");
		
		// Additional setup for performance perspective.
		Object perspObj = configurer.getWorkbenchConfigurer().getData("perspective.id");
		if ( perspObj instanceof String ) {
			String perspectiveId = (String) perspObj;
			if (perspectiveId.equals(PerformancePerspective.ID)) {
				configurer.setShowPerspectiveBar(true);
			}
		}
	}
}
