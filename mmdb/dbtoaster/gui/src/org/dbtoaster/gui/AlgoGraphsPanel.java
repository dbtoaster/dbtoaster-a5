
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

import org.apache.thrift.TException;
import org.dbtoaster.model.AlgoDataExtractor;

public class AlgoGraphsPanel extends Composite
{

    private static final long serialVersionUID = 4339624794240476495L;

    class DataGenerator extends Timer implements ActionListener
    {

        private static final long serialVersionUID = -2743247589626778216L;

        TimeSeries s1;
        TimeSeries s2;
        XYPlot plot;
        double y = Math.random() * 100;

        DataGenerator(TimeSeries s1,TimeSeries s2, XYPlot plot, int i)
        {
            super(i, null);
            this.s1 = s1;
            this.s2 = s2;
            this.plot = plot;
            addActionListener(this);
        }

        public void actionPerformed(ActionEvent actionevent)
        {
            
             getDisplay().asyncExec(new Runnable() { public void run() { if (
             !getDisplay().isDisposed() ) { double yFeed =
             Math.pow((Math.random() - 0.5) * 2.5, 3.0); y += yFeed;
             Millisecond x = new Millisecond(); s1.add(x, y);
              
             ValueAxis xAxis = plot.getDomainAxis();
             xAxis.setRange(xAxis.getUpperBound() - 10000,
             Math.max(x.getEnd().getTime(), xAxis.getUpperBound())); } } });
             
             getDisplay().asyncExec(new Runnable() { public void run() { if (
                     !getDisplay().isDisposed() ) { double yFeed =
                     Math.pow((Math.random() - 0.5) * 2.5, 3.0); y += yFeed;
                     Millisecond x = new Millisecond(); s2.add(x, y);
                      
                     ValueAxis xAxis = plot.getDomainAxis();
                     xAxis.setRange(xAxis.getUpperBound() - 10000,
                     Math.max(x.getEnd().getTime(), xAxis.getUpperBound())); } } });
             
        }
    }

    class StatPanel extends ChartComposite
    {

        TimeSeriesCollection statsData;
        TimeSeries statsSeries1;
        TimeSeries statsSeries2;
        JFreeChart chart;
        XYPlot plot;

        public StatPanel(String name, Composite comp, int style, Paint p)
        {
            super(comp, style);
            statsData = new TimeSeriesCollection();
            statsSeries1 = new TimeSeries(name);
            statsSeries2 = new TimeSeries(name+2);
            // statsSeries.setMaximumItemAge(10000);
            statsData.addSeries(statsSeries1);
            statsData.addSeries(statsSeries2);
            
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
                renderer.setSeriesPaint(0, Color.GREEN);
                renderer.setUseFillPaint(true);
                renderer.setUseOutlinePaint(true);
            }

            DateAxis axis = (DateAxis) plot.getDomainAxis();
            axis.setDateFormatOverride(new SimpleDateFormat("H:m:s.S"));

            setChart(chart);

            DataGenerator feed = new DataGenerator(statsSeries1, statsSeries2, plot, 100);
            feed.start();
        }
    };

    class DiffsDataExtractor extends Timer implements ActionListener
    {

        private static final long serialVersionUID = 2309247589626778216L;

        TimeSeries s1;
        TimeSeries s2;
        AlgoDataExtractor.Client dataClient;
        XYPlot plot;
        double y = Math.random() * 100;

        DiffsDataExtractor(TimeSeries s1,TimeSeries s2, AlgoDataExtractor.Client dClient, XYPlot plot, int i)
        {
            super(i, null);
            this.s1 = s1;
            this.s2 = s2;
            this.plot = plot;
            this.dataClient=dClient;
            addActionListener(this);
        }

        public void actionPerformed(ActionEvent actionevent)
        {
            
			getDisplay().asyncExec(new Runnable() {
				public void run() {
					if (!getDisplay().isDisposed()) {
//						double yFeed = Math.pow((Math.random() - 0.5) * 2.5, 3.0);
						double priceDiff;
						double timeDifference;
						try {
							priceDiff=dataClient.getAsksDiff() - dataClient.getBidsDiff();
							timeDifference=dataClient.getAsksTime() - dataClient.getBidsTime();
							
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							priceDiff=0;
							timeDifference=0;
						}
						Millisecond x = new Millisecond();
						s1.add(x, priceDiff);
						s2.add(x, timeDifference);

						ValueAxis xAxis = plot.getDomainAxis();
						xAxis.setRange(xAxis.getUpperBound() - 10000, Math.max(x.getEnd().getTime(), xAxis.getUpperBound()));
					}
				}
			});            
        }
    }
    
    class DifferencesStartPanel extends ChartComposite
    {

        TimeSeriesCollection statsData;
        TimeSeries statsSeries1;
        TimeSeries statsSeries2;
        JFreeChart chart;
        XYPlot plot;

        public DifferencesStartPanel(String priceDiffs, String timeDiffs, AlgoDataExtractor.Client dClient, Composite comp, int style, Paint pPrices, Paint pTimes)
        {
            super(comp, style);
            statsData = new TimeSeriesCollection();
            statsSeries1 = new TimeSeries(priceDiffs);
            statsSeries2 = new TimeSeries(timeDiffs);
            // statsSeries.setMaximumItemAge(10000);
            statsData.addSeries(statsSeries1);
            statsData.addSeries(statsSeries2);
            
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
 //               renderer.setBaseFillPaint(pPrices);
 //               renderer.setBaseOutlinePaint(pPrices);
                renderer.setSeriesPaint(0, pPrices);
                renderer.setSeriesPaint(1, pTimes);
 //               renderer.setUseFillPaint(true);
 //               renderer.setUseOutlinePaint(true);
            }

            DateAxis axis = (DateAxis) plot.getDomainAxis();
            axis.setDateFormatOverride(new SimpleDateFormat("H:m:s.S"));

            setChart(chart);

            DiffsDataExtractor feed = new DiffsDataExtractor(statsSeries1, statsSeries2, dClient, plot, 100);
            feed.start();
        }
    };
    
    class VolatilityDataExtractor extends Timer implements ActionListener
    {

        private static final long serialVersionUID = -1743247539626751216L;

        TimeSeries s1;
        TimeSeries s2;
        TimeSeries s3;
        AlgoDataExtractor.Client dataClient;
        XYPlot plot;

        double currentPrice=0;
		double mean=0;
		double SD=0;
		double meanPlusSD=0;
		double meanMinusSD=0;

        VolatilityDataExtractor(TimeSeries s1,TimeSeries s2, TimeSeries s3, AlgoDataExtractor.Client dClient, XYPlot plot, int i)
        {
            super(i, null);
            this.s1 = s1;
            this.s2 = s2;
            this.s3 = s3;
            this.plot = plot;
            this.dataClient=dClient;
            addActionListener(this);
        }

        public void actionPerformed(ActionEvent actionevent)
        {
            
			getDisplay().asyncExec(new Runnable() {
				public void run() {
					if (!getDisplay().isDisposed()) {
//						double yFeed = Math.pow((Math.random() - 0.5) * 2.5, 3.0);

						try {
							currentPrice=dataClient.getPrice()/10000;
							mean=dataClient.getMeanPrice();
							SD=dataClient.getVariance();
							meanPlusSD=mean+SD;
							meanMinusSD=mean-SD;
							
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							currentPrice=0;
							mean=0;
							SD=0;
						}
						Millisecond x = new Millisecond();
						s1.add(x, currentPrice);
						s2.add(x, meanPlusSD);
						s3.add(x, meanMinusSD);

						ValueAxis xAxis = plot.getDomainAxis();
						xAxis.setRange(xAxis.getUpperBound() - 10000, Math.max(x.getEnd().getTime(), xAxis.getUpperBound()));
					}
				}
			});            
        }
    }
    
    class VolatilityStartPanel extends ChartComposite
    {

        TimeSeriesCollection statsData;
        TimeSeries statsSeries1;
        TimeSeries statsSeries2;
        TimeSeries statsSeries3;
        JFreeChart chart;
        XYPlot plot;

        public VolatilityStartPanel(String currentPrice, String meanPrice, AlgoDataExtractor.Client dClient, Composite comp, int style, Paint currentP, Paint sdP )
        {
            super(comp, style);
            statsData = new TimeSeriesCollection();
            statsSeries1 = new TimeSeries(currentPrice);
            statsSeries2 = new TimeSeries(meanPrice+"_plusSD");
            statsSeries3 = new TimeSeries(meanPrice+"_minusSD");
            // statsSeries.setMaximumItemAge(10000);
            statsData.addSeries(statsSeries1);
            statsData.addSeries(statsSeries2);
            statsData.addSeries(statsSeries3);
            
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
 //               renderer.setBaseFillPaint(pPrices);
 //               renderer.setBaseOutlinePaint(pPrices);
                renderer.setSeriesPaint(0, currentP);
                renderer.setSeriesPaint(1, sdP);
                renderer.setSeriesPaint(2, sdP);
 //               renderer.setUseFillPaint(true);
 //               renderer.setUseOutlinePaint(true);
            }

            DateAxis axis = (DateAxis) plot.getDomainAxis();
            axis.setDateFormatOverride(new SimpleDateFormat("H:m:s.S"));

            setChart(chart);

            VolatilityDataExtractor feed = new VolatilityDataExtractor(statsSeries1, statsSeries2, statsSeries3, dClient, plot, 100);
            feed.start();
        }
    };
    
    class ProfitsDataExtractor extends Timer implements ActionListener
    {

        private static final long serialVersionUID = -2743247585526238216L;

        TimeSeries s1;
        TimeSeries s2;
        AlgoDataExtractor.Client dataClient;
        XYPlot plot;
        double y = Math.random() * 100;

        ProfitsDataExtractor(TimeSeries s1,TimeSeries s2, AlgoDataExtractor.Client dClient, XYPlot plot, int i)
        {
            super(i, null);
            this.s1 = s1;
            this.s2 = s2;
            this.plot = plot;
            this.dataClient=dClient;
            addActionListener(this);
        }

        public void actionPerformed(ActionEvent actionevent)
        {
            
			getDisplay().asyncExec(new Runnable() {
				public void run() {
					if (!getDisplay().isDisposed()) {
//						double yFeed = Math.pow((Math.random() - 0.5) * 2.5, 3.0);
						double stocksValue;
						double curretFunds;
						try {
							stocksValue=dataClient.getPrice()*dataClient.getAmountStocks();
							curretFunds=dataClient.getMoney();
							
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							stocksValue=0;
							curretFunds=0;
						}
						Millisecond x = new Millisecond();
						s1.add(x, stocksValue);
						s2.add(x, curretFunds);

						ValueAxis xAxis = plot.getDomainAxis();
						xAxis.setRange(xAxis.getUpperBound() - 10000, Math.max(x.getEnd().getTime(), xAxis.getUpperBound()));
					}
				}
			});            
        }
    }
    
    class ProfitStartPanel extends ChartComposite
    {

        TimeSeriesCollection statsData;
        TimeSeries statsSeries1;
        TimeSeries statsSeries2;
        JFreeChart chart;
        XYPlot plot;

        public ProfitStartPanel(String stocks, String money, AlgoDataExtractor.Client dClient, Composite comp, int style, Paint pPrices, Paint pTimes)
        {
            super(comp, style);
            statsData = new TimeSeriesCollection();
            statsSeries1 = new TimeSeries(stocks);
            statsSeries2 = new TimeSeries(money);
            // statsSeries.setMaximumItemAge(10000);
            statsData.addSeries(statsSeries1);
            statsData.addSeries(statsSeries2);
            
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
 //               renderer.setBaseFillPaint(pPrices);
 //               renderer.setBaseOutlinePaint(pPrices);
                renderer.setSeriesPaint(0, pPrices);
                renderer.setSeriesPaint(1, pTimes);
 //               renderer.setUseFillPaint(true);
 //               renderer.setUseOutlinePaint(true);
            }

            DateAxis axis = (DateAxis) plot.getDomainAxis();
            axis.setDateFormatOverride(new SimpleDateFormat("H:m:s.S"));

            setChart(chart);

            DiffsDataExtractor feed = new DiffsDataExtractor(statsSeries1, statsSeries2, dClient, plot, 100);
            feed.start();
        }
    };
    
    DifferencesStartPanel diffsPanel;
    VolatilityStartPanel  volatilityPanel;
    ProfitStartPanel      profitsPanel;

    Slider diffsSlider;
    Slider volatilitySlider;
    Slider moneySlider;

    public AlgoGraphsPanel(Composite parent, AlgoDataExtractor.Client dClient, int type, int style)
    {
        super(parent, style);
        setLayout(new GridLayout(4, false));

        if (type == 1)
        {
	        diffsSlider = new Slider(this, SWT.VERTICAL);
	        GridData csData = new GridData(SWT.FILL, SWT.FILL, false, true);
	        diffsSlider.setLayoutData(csData);
	
	        diffsPanel = new DifferencesStartPanel("Price", "Time", dClient, this, SWT.NO_TRIM, Color.RED, Color.GREEN);
	        GridData cpData = new GridData(SWT.FILL, SWT.FILL, true, true);
	        // cpData.widthHint = 400;
	        diffsPanel.setLayoutData(cpData);
        }
        else if (type == 2)
        {
        	volatilitySlider = new Slider(this, SWT.VERTICAL);
	        GridData csData = new GridData(SWT.FILL, SWT.FILL, false, true);
	        volatilitySlider.setLayoutData(csData);
	
	        volatilityPanel = new VolatilityStartPanel("Current Price", "Avg. Price ", dClient, this, SWT.NO_TRIM, Color.RED, Color.GREEN);
	        GridData cpData = new GridData(SWT.FILL, SWT.FILL, true, true);
	        // cpData.widthHint = 400;
	        volatilityPanel.setLayoutData(cpData);
        } 
        else if (type == 3)
        {
        	moneySlider = new Slider(this, SWT.VERTICAL);
	        GridData csData = new GridData(SWT.FILL, SWT.FILL, false, true);
	        moneySlider.setLayoutData(csData);
	
	        profitsPanel = new ProfitStartPanel("StocksValue", "Liquid Funds", dClient, this, SWT.NO_TRIM, Color.RED, Color.GREEN);
	        GridData cpData = new GridData(SWT.FILL, SWT.FILL, true, true);
	        // cpData.widthHint = 400;
	        profitsPanel.setLayoutData(cpData);
        }
        else
        {
        	System.out.println("AlgoGraphPanell needs to be extended to handle additional type: "+type);
        }
    }
}
