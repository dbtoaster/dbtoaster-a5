package org.dbtoaster.gui;

import java.awt.Color;
import java.awt.Paint;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.SimpleDateFormat;

import javax.swing.Timer;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Slider;
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

public class DBPerfPanel extends Composite
{

    private static final long serialVersionUID = 4339624058150476495L;

    /*
    class DataGenerator extends Timer implements ActionListener
    {

        private static final long serialVersionUID = -2743247589626778216L;

        TimeSeries s;
        XYPlot plot;
        double y = Math.random() * 100;

        DataGenerator(TimeSeries s, XYPlot plot, int i)
        {
            super(i, null);
            this.s = s;
            this.plot = plot;
            addActionListener(this);
        }

        public void actionPerformed(ActionEvent actionevent)
        {

            
            getDisplay().asyncExec(new Runnable()
            {
                public void run()
                {
                    if (!getDisplay().isDisposed())
                    {
                        double yFeed = Math.pow((Math.random() - 0.5) * 2.5, 3.0);
                        y += yFeed;
                        Millisecond x = new Millisecond();
                        s.add(x, y);

                        ValueAxis xAxis = plot.getDomainAxis();
                        xAxis.setRange(xAxis.getUpperBound() - 10000, Math.max(
                            x.getEnd().getTime(), xAxis.getUpperBound()));
                    }
                }
            });
            
        }
    }
    */

    class StatPanel extends ChartComposite
    {

        TimeSeriesCollection statsData;
        TimeSeries statsSeries;
        JFreeChart chart;
        XYPlot plot;

        public StatPanel(String name, Composite comp, int style, Paint p)
        {
            super(comp, style);
            statsData = new TimeSeriesCollection();
            statsSeries = new TimeSeries(name);
            // statsSeries.setMaximumItemAge(10000);
            statsData.addSeries(statsSeries);
            chart = ChartFactory.createTimeSeriesChart(null, null, null,
                    statsData, false, false, false);

            plot = (XYPlot) chart.getPlot();
            plot.setBackgroundPaint(Color.LIGHT_GRAY);
            plot.setDomainGridlinePaint(Color.WHITE);
            plot.setRangeGridlinePaint(Color.WHITE);
            plot.setDomainCrosshairVisible(true);
            plot.setRangeCrosshairVisible(true);

            XYItemRenderer r = plot.getRenderer();
            if (r instanceof XYLineAndShapeRenderer)
            {
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

            //DataGenerator feed = new DataGenerator(statsSeries, plot, 100);
            //feed.start();
        }
    };

    StatPanel cpuPanel;
//    StatPanel memPanel;

//    Slider cpuSlider;
//    Slider memSlider;

    public DBPerfPanel(Composite parent, int style)
    {
        super(parent, style);
        setLayout(new GridLayout(2, false));

//        cpuSlider = new Slider(this, SWT.VERTICAL);
//        GridData csData = new GridData(SWT.FILL, SWT.FILL, false, true);
//        cpuSlider.setLayoutData(csData);

        cpuPanel = new StatPanel("cpu", this, SWT.NO_TRIM, Color.RED);
        GridData cpData = new GridData(SWT.FILL, SWT.FILL, true, true);
        // cpData.widthHint = 400;
        cpuPanel.setLayoutData(cpData);

//        memSlider = new Slider(this, SWT.VERTICAL);
//        GridData msData = new GridData(SWT.FILL, SWT.FILL, false, true);
//        memSlider.setLayoutData(msData);

//        memPanel = new StatPanel("mem", this, SWT.NO_TRIM, Color.GREEN);
//        GridData mpData = new GridData(SWT.FILL, SWT.FILL, true, true);
//        // mpData.widthHint = 400;
//        memPanel.setLayoutData(mpData);
    }
    
    public TimeSeries getCpuTimeSeries() {
    	return cpuPanel.statsSeries;
    }
    
    public ChartComposite getCpuChart() {
        return cpuPanel;
    }
}