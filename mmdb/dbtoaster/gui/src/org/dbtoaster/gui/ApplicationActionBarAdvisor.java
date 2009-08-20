package org.dbtoaster.gui;

import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Action;
import org.eclipse.ui.IWorkbenchActionConstants;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.IViewPart;
import org.eclipse.ui.actions.ActionFactory;
import org.eclipse.ui.actions.ActionFactory.IWorkbenchAction;
import org.eclipse.ui.application.ActionBarAdvisor;
import org.eclipse.ui.application.IActionBarConfigurer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;

/**
 * An action bar advisor is responsible for creating, adding, and disposing of
 * the actions added to a workbench window. Each window will be populated with
 * new actions.
 */
public class ApplicationActionBarAdvisor extends ActionBarAdvisor
{

    // Actions - important to allocate these only in makeActions, and then use
    // them
    // in the fill methods. This ensures that the actions aren't recreated
    // when fillActionBars is called with FILL_PROXY.
    private IWorkbenchAction exitAction;
    private IWorkbenchWindow win;
    
    Action openAction = new Action ("Open")
    { 
    	public void run() {
    		FileDialog dialog = new FileDialog(new Shell(), SWT.OPEN);
    		final String file= dialog.open();
    		if(file!= null) {
    			try {
    				String content = readFileAsAString(new File(file));
    				System.out.println("Opening " + content);
    				IWorkbenchPage pages[] = win.getPages();
    				for (IWorkbenchPage p: pages) {
    					IViewPart view;
    					if((view = p.findView("dbtoaster_gui.queryeditor")) != null) {
    						QueryEditor qe = (QueryEditor) view;
    						qe.getQTAV().setText(content);
    					}
    				}
    				
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
    	public void dispose() {
    		
    	}
    };
    
    public ApplicationActionBarAdvisor(IActionBarConfigurer configurer)
    {
        super(configurer);
    }

    protected void makeActions(final IWorkbenchWindow window)
    {
        // Creates the actions and registers them.
        // Registering is needed to ensure that key bindings work.
        // The corresponding commands keybindings are defined in the plugin.xml
        // file.
        // Registering also provides automatic disposal of the actions when
        // the window is closed.

        exitAction = ActionFactory.QUIT.create(window);
        register(exitAction);      
        win = window;
    }

    protected void fillMenuBar(IMenuManager menuBar)
    {
    	MenuManager fileMenu = new MenuManager("&File",
                //IWorkbenchActionConstants.M_FILE);
    			"temp");
    	fileMenu.add(openAction);
        fileMenu.add(exitAction);
        menuBar.add(fileMenu);        
    }
    
    /* from http://www.java2s.com/Code/Java/SWT-JFace-Eclipse/FileViewer.htm */
    public static String readFileAsAString(File file) throws IOException {
        return new String(getBytesFromFile(file));
    }
    
    /* from http://www.java2s.com/Code/Java/SWT-JFace-Eclipse/FileViewer.htm */
    public static byte[] getBytesFromFile(File file) throws IOException {
        InputStream is = new FileInputStream(file);

        // Get the size of the file
        long length = file.length();

        // You cannot create an array using a long type.
        // It needs to be an int type.
        // Before converting to an int type, check
        // to ensure that file is not larger than Integer.MAX_VALUE.
        if (length > Integer.MAX_VALUE) {
          // File is too large
          throw new IllegalArgumentException("File is too large! (larger or equal to 2G)");
        }

        // Create the byte array to hold the data
        byte[] bytes = new byte[(int) length];

        // Read in the bytes
        int offset = 0;
        int numRead = 0;
        while (offset < bytes.length
          && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
          offset += numRead;
        }

        // Ensure all the bytes have been read in
        if (offset < bytes.length) {
          throw new IOException(
            "Could not completely read file " + file.getName());
        }

        // Close the input stream and return bytes
        is.close();
        return bytes;
    }
}
