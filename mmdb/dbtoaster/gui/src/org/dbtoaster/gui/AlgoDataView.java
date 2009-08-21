
package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.Panel;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.swing.JRootPane;

import org.dbtoaster.gui.PerfView.ExecuteQueryPanel;
import org.dbtoaster.gui.QueryEditor.QueryHistoryPanel;
import org.dbtoaster.model.DBToasterWorkspace;
import org.dbtoaster.model.Query;
import org.eclipse.swt.SWT;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.IPerspectiveDescriptor;
import org.eclipse.ui.IPerspectiveListener;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.part.ViewPart;


import org.dbtoaster.model.AlgoDataExtractor;
//import org.dbtoaster.model.Compiler.CompileMode;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import prefuse.Constants;
import prefuse.data.io.DataIOException;
import prefuse.data.io.TreeMLReader;
import prefuse.util.ColorLib;


public class AlgoDataView extends ViewPart
{
    public static final String ID = "dbtoaster_gui.algo_data_view";

    DBToasterWorkspace dbtWorkspace;
    TSocket s;
    
    // TODO: Move constants to appropriate places
	private String currentHost="127.0.0.1";
	private int currentPort=5502;
	
	private String [] serverCommands = {"java", "ExchangeServer.ExchangeServer", "20081201.csv"};
	private String ServerDir ="/home/anton/software/ExchangeSimulatorServer/src";
	
	private String ToasterDir = "/home/anton/software/vwapConnection";
	private String toasterCommand = "/home/anton/software/vwapConnection/test";
	
	private String AlgoEngineDir = "/home/anton/software/TradingClient";
	private String algoEngineCommand = "/home/anton/software/TradingClient/test";
	
    private LinkedHashMap<String,AlgoGraphsPanel> databasePanels;
    private TBinaryProtocol.Factory debuggerProtocolFactory;
    
    
    class RunServer extends Composite
    {
        DBToasterWorkspace dbtWorkspace;
        
        ProcessBuilder exchangeServer;
        ProcessBuilder toasterRuntime;
        ProcessBuilder algoEngine;
        
        Process currentServer = null;
        Process currentToaster = null;
        Process currentAlgos = null;
        
        AlgoGraphsPanel graph1;
        AlgoGraphsPanel graph2;
        AlgoGraphsPanel graph3;
        
        
        Text queryText;

        public RunServer(Composite parent, int style)
        {
            super(parent, style);

            dbtWorkspace = DBToasterWorkspace.getWorkspace();
          //  GridLayout dbcLayout = new GridLayout(1, false);
           // algoGraphs.setLayout(dbcLayout);
           // this.getLayout()
                      
            queryText = new Text(this, style);
            queryText.setLayoutData(new GridData(GridData.FILL, GridData.FILL,
                    true, true));
            queryText.setText("Welcome to Trading Toaster v0.01");
        }
        
        public void RunExchangeServer()
        {
        	exchangeServer = new ProcessBuilder(serverCommands);
            
            File execDir=new File(ServerDir);

            exchangeServer.directory(execDir);
            
            exchangeServer.redirectErrorStream(true);
            
            String test;
            
            try {
				currentServer=exchangeServer.start();
				test="Started Exchange Server!";
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Couldn't start an ExchangeServer!");
				test="Couldn't Start Exchange Server";
			}
			          
            queryText.setText(test);
        }  
        
        public void StopEchangeServer()
        {
        	currentServer.destroy();
        }
        
        public void RunToasterRuntime()
        {

        	toasterRuntime = new ProcessBuilder(toasterCommand);
            
            File execDir=new File(ToasterDir);

            toasterRuntime.directory(execDir);
            
            toasterRuntime.redirectErrorStream(true);
            
            String test;
            
            try {
            	currentToaster = toasterRuntime.start();
				test="Started Toaster!";
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Couldn't start an ExchangeServer!");
				test = "Couldn't Start Toaster";
			}
            
            queryText.setText(test);
        }
        
        public void StopToasterRuntime()
        {
        	currentToaster.destroy();
        }
        
        public void RunAlgoEngine()
        {
        	algoEngine = new ProcessBuilder(algoEngineCommand);
            
            File execDir=new File(AlgoEngineDir);

            algoEngine.directory(execDir);
            
            algoEngine.redirectErrorStream(true);
            
            String test;
            
            try {
            	currentAlgos = algoEngine.start();
				test="Started AlgoEngine!";
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Couldn't start an ExchangeServer!");
				test="Couldn't Start AlgoEngine";
			}
            
            queryText.setText(test);
        }
        
        public void StopAlgoEngine()
        {
        	currentAlgos.destroy();
        }
        
        public void SetPanels(Composite parent, AlgoDataExtractor.Client dataClient)
        {
        	graph1= new AlgoGraphsPanel(parent, dataClient, 1, SWT.NO_TRIM);
            GridData frameData = new GridData(SWT.FILL, SWT.FILL, true, true);
            frameData.widthHint = 700;
            frameData.heightHint = 150;
            frameData.horizontalSpan = 2;
            graph1.setLayoutData(frameData);
            
            graph2= new AlgoGraphsPanel(parent, dataClient, 2, SWT.NO_TRIM);
     //       GridData frameData = new GridData(SWT.FILL, SWT.FILL, true, true);
            frameData.widthHint = 700;
            frameData.heightHint = 150;
            frameData.horizontalSpan = 2;
            graph2.setLayoutData(frameData);
            
            graph3= new AlgoGraphsPanel(parent, dataClient, 3, SWT.NO_TRIM);
     //       GridData frameData = new GridData(SWT.FILL, SWT.FILL, true, true);
            frameData.widthHint = 700;
            frameData.heightHint = 150;
            frameData.horizontalSpan = 2;
            graph3.setLayoutData(frameData);
        }
        
        public void RemovePanels(Composite parent)
        {
        	graph1.dispose();
        	graph2.dispose();
        	graph3.dispose();
    
      
        }
    };
    
    class ExecuteTradingPanel
    {
        DBToasterWorkspace dbtWorkspace;
        
        Composite parent;
        Button runButton;
        boolean buttonOff=true;
        
        Tree eqTree;

        public ExecuteTradingPanel(Composite parent, DBToasterWorkspace dbtw)
        {
            dbtWorkspace = dbtw;
            this.parent=parent;

            parent.addDisposeListener(new DisposeListener()
            	{

            		public void widgetDisposed(DisposeEvent e){
            			
            		}
            	});

        }
        
        void addStartButton()
        {
        	
        	debuggerProtocolFactory = new TBinaryProtocol.Factory();
            
            s = new TSocket(currentHost, currentPort);

        	final RunServer btBarComp = new RunServer(parent, SWT.NONE);
            GridLayout btBarLayout = new GridLayout(2, false);
            btBarLayout.marginWidth = 0;
            btBarLayout.marginHeight = 0;
            btBarComp.setLayout(btBarLayout);
            GridData btBarLD = new GridData(SWT.FILL, SWT.FILL, true, false);
            btBarLD.horizontalSpan = 2;
            btBarComp.setLayoutData(btBarLD);
            
 /*           Text statusText = new Text(btBarComp, SWT.BORDER | SWT.WRAP);
            GridData gridData = new GridData(SWT.FILL, SWT.FILL, true, false);
            statusText.setBackground(statusText.getDisplay().getSystemColor(
                    SWT.COLOR_TITLE_INACTIVE_BACKGROUND));
            statusText.setForeground(statusText.getDisplay().getSystemColor(
                    SWT.COLOR_TITLE_INACTIVE_FOREGROUND));
            gridData.heightHint = statusText.getLineHeight() * 2;
            statusText.setLayoutData(gridData);
            statusText.setText("Welcome to DBToaster v0.1.");
   */         
            // Add run button.
            runButton = new Button(btBarComp, SWT.PUSH);
            runButton.setBackground(runButton.getDisplay().getSystemColor(
                    SWT.COLOR_DARK_GREEN));
            runButton.setForeground(runButton.getDisplay().getSystemColor(
                    SWT.COLOR_GREEN));
            runButton.setText("TRADE!");
            
            GridData toastGridData = new GridData(SWT.END, SWT.FILL, false, false);
            toastGridData.minimumWidth = 50;
 //           toastGridData.heightHint = statusText.getLineHeight() * 2;
            runButton.setLayoutData(toastGridData);
            buttonOff=true;
            


            runButton.addSelectionListener(new SelectionListener()
            {
                public void widgetSelected(SelectionEvent e)
                {
                	GridData toastGridData;
                	if (buttonOff)
                	{
	                    btBarComp.RunExchangeServer();
	                    System.out.println(btBarComp.currentServer);
	                    try {
							Thread.sleep(200);
						} catch (InterruptedException e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
	                    btBarComp.RunToasterRuntime();	
	                    System.out.println(btBarComp.currentToaster);
	                    try {
							Thread.sleep(200);
						} catch (InterruptedException e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
	                    btBarComp.RunAlgoEngine();
	                    System.out.println(btBarComp.currentAlgos);
	                    
	                    try {
							Thread.sleep(200);
						} catch (InterruptedException e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
	                    
	                   
	                    try {
	            			s.open();
	            		} catch (TTransportException error) {
	            			// TODO Auto-generated catch block
	            			error.printStackTrace();
	            		}
	            		
	                    TProtocol protocol = debuggerProtocolFactory.getProtocol(s);
	                    AlgoDataExtractor.Client dataClient=new AlgoDataExtractor.Client(protocol);
	                    

	                    
	                    btBarComp.SetPanels(parent, dataClient);	                    
	                    parent.layout();
	                    
	                    runButton.setBackground(runButton.getDisplay().getSystemColor(
	                            SWT.COLOR_DARK_RED));
	                    runButton.setForeground(runButton.getDisplay().getSystemColor(
	                            SWT.COLOR_RED));
	                    runButton.setText("STOP!");
	                    
	                    toastGridData = new GridData(SWT.END, SWT.FILL, false, false);
	                    toastGridData.minimumWidth = 50;
	         //           toastGridData.heightHint = statusText.getLineHeight() * 2;
	                    runButton.setLayoutData(toastGridData);
	                    buttonOff=false;
                	}
                	else
                	{
                		btBarComp.StopAlgoEngine();
                		btBarComp.StopToasterRuntime();
                		btBarComp.StopEchangeServer();
                		
                		s.close();
                		
                		btBarComp.RemovePanels(parent);	
                		parent.redraw();
 	                    parent.layout();
                		
                		runButton.setBackground(runButton.getDisplay().getSystemColor(
	                            SWT.COLOR_DARK_GREEN));
	                    runButton.setForeground(runButton.getDisplay().getSystemColor(
	                            SWT.COLOR_GREEN));
	                    runButton.setText("TRADE!");
	                    
	                    toastGridData = new GridData(SWT.END, SWT.FILL, false, false);
	                    toastGridData.minimumWidth = 50;
	         //           toastGridData.heightHint = statusText.getLineHeight() * 2;
	                    runButton.setLayoutData(toastGridData);
	                    buttonOff=true;
                	}
                    
                }

                public void widgetDefaultSelected(SelectionEvent e)
                {
                }
            });
            
            
        }
    }

    public void createPartControl(Composite parent)
    {

        dbtWorkspace = DBToasterWorkspace.getWorkspace();
        
        
        

        
 //       AlgoDataExtractor.Client dataClient=new AlgoDataExtractor.Client(protocol);
        
        Composite top = new Composite(parent, SWT.NONE);
        GridLayout layout = new GridLayout(2, false);
        layout.marginWidth = 0;
        layout.marginHeight = 0;
        top.setLayout(layout);
 
        
        Composite algoGraphs = new Composite(top, SWT.NONE);
        algoGraphs.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
        GridLayout dbcLayout = new GridLayout(1, false);
        algoGraphs.setLayout(dbcLayout);
/*
        AlgoGraphsPanel graph1= new AlgoGraphsPanel(algoGraphs, dataClient, 1, SWT.NO_TRIM);
        GridData frameData = new GridData(SWT.FILL, SWT.FILL, true, true);
        frameData.widthHint = 700;
        frameData.heightHint = 150;
        frameData.horizontalSpan = 2;
        graph1.setLayoutData(frameData);
        
        AlgoGraphsPanel graph2= new AlgoGraphsPanel(algoGraphs, dataClient, 2, SWT.NO_TRIM);
 //       GridData frameData = new GridData(SWT.FILL, SWT.FILL, true, true);
        frameData.widthHint = 700;
        frameData.heightHint = 150;
        frameData.horizontalSpan = 2;
        graph2.setLayoutData(frameData);
        
        AlgoGraphsPanel graph3= new AlgoGraphsPanel(algoGraphs, dataClient, 3, SWT.NO_TRIM);
 //       GridData frameData = new GridData(SWT.FILL, SWT.FILL, true, true);
        frameData.widthHint = 700;
        frameData.heightHint = 150;
        frameData.horizontalSpan = 2;
        graph3.setLayoutData(frameData);
        
*/       
        ExecuteTradingPanel tradingPanel = new ExecuteTradingPanel(algoGraphs, dbtWorkspace);
        tradingPanel.addStartButton();

    }

    public void setFocus()
    {

    }

}

