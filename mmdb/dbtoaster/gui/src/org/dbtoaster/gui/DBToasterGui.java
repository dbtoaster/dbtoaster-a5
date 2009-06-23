package org.dbtoaster.gui;

import org.eclipse.equinox.app.IApplication;
import org.eclipse.equinox.app.IApplicationContext;
import org.eclipse.osgi.baseadaptor.loader.ClasspathEntry;
import org.eclipse.osgi.internal.baseadaptor.DefaultClassLoader;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.WorkbenchException;

/**
 * This class controls all aspects of the application's execution
 */
public class DBToasterGui implements IApplication {

	/* (non-Javadoc)
	 * @see org.eclipse.equinox.app.IApplication#start(org.eclipse.equinox.app.IApplicationContext)
	 */
	public Object start(IApplicationContext context) {
		ClassLoader applicationClassLoader = this.getClass().getClassLoader();
		if (applicationClassLoader == null) {
			applicationClassLoader = ClassLoader.getSystemClassLoader();
		}
		ClasspathEntry[] cp = ((DefaultClassLoader) applicationClassLoader)
	                     .getClasspathManager().getHostClasspathEntries();
		for (ClasspathEntry i : cp)
			System.out.println(i.getBundleFile().getBaseFile().getName());
		
		Display display = PlatformUI.createDisplay();
		try {
			int returnCode = PlatformUI.createAndRunWorkbench(display, new ApplicationWorkbenchAdvisor());
			
			if (returnCode == PlatformUI.RETURN_RESTART) {
				return IApplication.EXIT_RESTART;
			}
			return IApplication.EXIT_OK;
		} finally {
			display.dispose();
		}
	}

	/* (non-Javadoc)
	 * @see org.eclipse.equinox.app.IApplication#stop()
	 */
	public void stop() {
		final IWorkbench workbench = PlatformUI.getWorkbench();
		if (workbench == null)
			return;
		final Display display = workbench.getDisplay();
		display.syncExec(new Runnable() {
			public void run() {
				if (!display.isDisposed())
					workbench.close();
			}
		});
	}
}
