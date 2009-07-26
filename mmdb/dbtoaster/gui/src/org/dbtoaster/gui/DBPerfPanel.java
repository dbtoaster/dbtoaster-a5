package org.dbtoaster.gui;

import java.awt.Color;
import java.awt.Paint;
import java.awt.Stroke;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;

import javax.swing.SwingUtilities;
import javax.swing.Timer;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Slider;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.internal.dnd.SwtUtil;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.experimental.chart.swt.ChartComposite;
import org.swtchart.Chart;
import org.swtchart.ILineSeries;
import org.swtchart.ISeries.SeriesType;

public class DBPerfPanel extends Composite {

	private static final long serialVersionUID = 4339624058150476495L;

	/*
	class RandomFeed extends Thread {

		TimeSeries s;
		double y = ((Math.random() - 0.5) * 200);

		RandomFeed(TimeSeries s) {
		        this.s = s;
		}

		public void run() {
	        double yFeed = Math.pow((Math.random() - 0.5) * 2.5, 3.0);
	        y += yFeed;
	        s.add(new Millisecond(), y);
		}
	};
	*/

    class DataGenerator extends Timer implements ActionListener {

		TimeSeries s;
		XYPlot plot;
		double y = Math.random() * 100;

    	DataGenerator(TimeSeries s, XYPlot plot, int i) {
    		super(i, null);
	        this.s = s;
	        this.plot = plot;
    		addActionListener(this);
        }
      
        public void actionPerformed(ActionEvent actionevent) {
        	/*
        	getDisplay().asyncExec(new Runnable() {
    			public void run() {
    				if ( !getDisplay().isDisposed() ) {
	    				double yFeed = Math.pow((Math.random() - 0.5) * 2.5, 3.0);
	    				y += yFeed;
	    				Millisecond x = new Millisecond();
	    				s.add(x, y);
	    				
	    				ValueAxis xAxis = plot.getDomainAxis();
	    				xAxis.setRange(xAxis.getUpperBound() - 10000,
    						Math.max(x.getEnd().getTime(), xAxis.getUpperBound()));
    				}
    			}
        	});
        	*/
        }
    }
   
	class StatPanel extends ChartComposite {

		TimeSeriesCollection statsData;
		TimeSeries statsSeries;
		JFreeChart chart;
		XYPlot plot;
				
		public StatPanel(String name, Composite comp, int style, Paint p) {
			super(comp, style);
			statsData = new TimeSeriesCollection();
			statsSeries = new TimeSeries(name);
			//statsSeries.setMaximumItemAge(10000);
			statsData.addSeries(statsSeries);
			chart = ChartFactory.createTimeSeriesChart(
				null, null, null, statsData, false, false, false);
			
			plot = (XYPlot) chart.getPlot();
	        plot.setBackgroundPaint(Color.LIGHT_GRAY);
	        plot.setDomainGridlinePaint(Color.WHITE);
	        plot.setRangeGridlinePaint(Color.WHITE);
	        plot.setDomainCrosshairVisible(true);
	        plot.setRangeCrosshairVisible(true);
	        
	        XYItemRenderer r = plot.getRenderer();
	        if (r instanceof XYLineAndShapeRenderer) {
	            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
	            renderer.setBaseShapesVisible(true);
	            renderer.setBaseShapesFilled(true);
	            renderer.setBaseFillPaint(p);
	            renderer.setBaseOutlinePaint(p);
	            renderer.setSeriesPaint(0, p);
	            renderer.setUseFillPaint(true);
	            renderer.setUseOutlinePaint(true);
	        }
	        
	        DateAxis axis = (DateAxis) plot.getDomainAxis();
	        axis.setDateFormatOverride(new SimpleDateFormat("H:m:s.S"));

			setChart(chart);

			DataGenerator feed = new DataGenerator(statsSeries, plot, 100);
			feed.start();
		}
	};

	/*
	class XYSeries {
		LinkedList<Double> xseries;
		LinkedList<Double> yseries;
		
		public XYSeries() {
			xseries = new LinkedList<Double>();
			yseries = new LinkedList<Double>();
		}
		
		public void add(double x, double y) {
			if ( xseries.getLast() > x ) {
				int idx = xseries.indexOf(x);
				xseries.add(idx, x);
				yseries.add(idx, y);
			}
			else {
				xseries.add(x); yseries.add(y);
			}
		}

		public double getXRange() {
			return xseries.getLast() - xseries.getFirst();
		}
		
		public double[] getXSeries() { return xseries.toArray(); }
	}

	class RandomFeed extends TimerTask {

		double x = 0.0;
		double y = ((Math.random() - 0.5) * 200);

		XYSeries s;
		
		RandomFeed(XYSeries s) {
		        this.s = s;
		}

		public void run() {
			double xFeed = Math.random() * 10.0;
	        double yFeed = Math.pow((Math.random() - 0.5) * 2.5, 3.0);
	        x += xFeed;
	        y += yFeed;
	        s.add(x, y);
		}
	};

	class StatPanel extends Chart {

		private final double[] ySeries = {
			0.0, 0.38, 0.71, 0.92, 1.0, 0.92,
			0.71, 0.38, 0.0, -0.38, -0.71, -0.92,
			-1.0, -0.92, -0.71, -0.38 };

		Timer t;

		public StatPanel(Composite parent, int style) {
			super(parent, style);

			// create line series
			ILineSeries lineSeries = (ILineSeries)
				getSeriesSet().createSeries(SeriesType.LINE, "line series");
			
			lineSeries.setYSeries(ySeries);

			// adjust the axis range
			getAxisSet().adjustRange();	

			t = new Timer("Feed timer");
	        t.schedule(new RandomFeed(statsSeries), 100L, 800L);
		}
	};
	*/


	StatPanel cpuPanel;
	StatPanel memPanel;
	
	Slider cpuSlider;
	Slider memSlider;
	
	public DBPerfPanel(Composite parent, int style) {
		super(parent, style);
		setLayout(new GridLayout(4, false));
		
		//cpuPanel = new StatPanel(this, SWT.NO_TRIM);
		//memPanel = new StatPanel(this, SWT.NO_TRIM);
		
		cpuSlider = new Slider(this, SWT.VERTICAL);
		GridData csData = new GridData(SWT.FILL, SWT.FILL, false, true);
		cpuSlider.setLayoutData(csData);
		
		cpuPanel = new StatPanel("cpu", this, SWT.NO_TRIM, Color.RED);
		GridData cpData = new GridData(SWT.FILL, SWT.FILL, true, true);
		//cpData.widthHint = 400;
		cpuPanel.setLayoutData(cpData);
		
		memSlider = new Slider(this, SWT.VERTICAL);
		GridData msData = new GridData(SWT.FILL, SWT.FILL, false, true);
		memSlider.setLayoutData(msData);

		memPanel = new StatPanel("mem", this, SWT.NO_TRIM, Color.GREEN);
		GridData mpData = new GridData(SWT.FILL, SWT.FILL, true, true);
		//mpData.widthHint = 400;
		memPanel.setLayoutData(mpData);
    }
}