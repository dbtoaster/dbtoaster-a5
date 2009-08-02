package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Panel;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JRootPane;

import org.eclipse.swt.SWT;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.ControlListener;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.part.ViewPart;

import prefuse.controls.ControlAdapter;

public class PerfView extends ViewPart
{

    public static final String ID = "dbtoaster_gui.perfview";

    private Vector<DBPerfPanel> databasePanels;
    private Text perfStatus;

    public void createPartControl(Composite parent)
    {
        Composite top = new Composite(parent, SWT.NONE);
        GridLayout layout = new GridLayout();
        layout.marginWidth = 0;
        layout.marginHeight = 0;
        top.setLayout(layout);

        int numDatabases = 5;
        databasePanels = new Vector<DBPerfPanel>();

        Composite dbcomp = new Composite(top, SWT.EMBEDDED);
        dbcomp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
        GridLayout dbcLayout = new GridLayout(4, false);
        dbcomp.setLayout(dbcLayout);

        Label yCpuLabel = new Label(dbcomp, SWT.BORDER);
        yCpuLabel.setText("CPU");
        GridData yCpuData = new GridData(SWT.FILL, SWT.CENTER, false, true);
        yCpuData.horizontalSpan = 1;
        yCpuData.verticalSpan = 6;
        yCpuLabel.setLayoutData(yCpuData);

        boolean rightLabelAdded = false;

        for (int i = 0; i < numDatabases; ++i)
        {
            /*
             * Composite framecomp = new Composite(dbcomp, SWT.EMBEDDED);
             * GridData frameData = new GridData(SWT.FILL, SWT.FILL, true,
             * true); frameData.widthHint = 700; frameData.heightHint = 150;
             * frameData.horizontalSpan = 2; framecomp.setLayoutData(frameData);
             * 
             * Frame dbframe = SWT_AWT.new_Frame(framecomp);
             * dbframe.setLayout(new BorderLayout()); Panel dbPanel = new
             * Panel(); dbframe.add(dbPanel, BorderLayout.CENTER); JRootPane
             * dbroot = new JRootPane(); dbPanel.setLayout(new BorderLayout());
             * dbPanel.add(dbroot, BorderLayout.CENTER);
             * 
             * databasePanels.add(new DBSwingPerfPanel());
             * dbroot.setContentPane(databasePanels.lastElement());
             * 
             * dbframe.pack(); dbframe.setVisible(true);
             */

            databasePanels.add(new DBPerfPanel(dbcomp, SWT.NO_TRIM));
            GridData frameData = new GridData(SWT.FILL, SWT.FILL, true, true);
            frameData.widthHint = 700;
            frameData.heightHint = 150;
            frameData.horizontalSpan = 2;
            databasePanels.lastElement().setLayoutData(frameData);

            if (!rightLabelAdded)
            {
                Label yMemLabel = new Label(dbcomp, SWT.BORDER);
                yMemLabel.setText("Mem");
                GridData yMemData = new GridData(SWT.FILL, SWT.CENTER, false,
                        true);
                yMemData.horizontalSpan = 1;
                yMemData.verticalSpan = 6;
                yMemLabel.setLayoutData(yMemData);

                rightLabelAdded = true;
            }
        }

        Label timeLabel1 = new Label(dbcomp, SWT.BORDER);
        timeLabel1.setText("Time");
        GridData t1Data = new GridData(SWT.CENTER, SWT.FILL, true, false);
        timeLabel1.setLayoutData(t1Data);

        Label timeLabel2 = new Label(dbcomp, SWT.BORDER);
        timeLabel2.setText("Time");
        GridData t2Data = new GridData(SWT.CENTER, SWT.FILL, true, false);
        timeLabel2.setLayoutData(t2Data);

        perfStatus = new Text(top, SWT.BORDER | SWT.WRAP);
        GridData psData = new GridData(SWT.FILL, SWT.FILL, true, false);
        psData.heightHint = perfStatus.getLineHeight();
        perfStatus.setLayoutData(psData);

        Display d = perfStatus.getDisplay();
        perfStatus.setBackground(d
                .getSystemColor(SWT.COLOR_TITLE_INACTIVE_BACKGROUND));
        perfStatus.setForeground(d
                .getSystemColor(SWT.COLOR_TITLE_INACTIVE_FOREGROUND));

        perfStatus.setText("No databases running.");
    }

    public void setFocus()
    {

    }

}
