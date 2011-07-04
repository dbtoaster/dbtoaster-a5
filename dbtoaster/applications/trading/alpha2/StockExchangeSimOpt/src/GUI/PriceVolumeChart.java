/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package GUI;

import java.awt.BorderLayout;
import java.util.Date;

import javax.swing.JPanel;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CombinedDomainXYPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import state.StockPrice;

/**
 *
 * @author kunal
 */
public class PriceVolumeChart extends ApplicationFrame {

    /** The time series data. */
    private TimeSeries priceSeries;
    private TimeSeries volumeSeries;

    public PriceVolumeChart(String title) {
        super(title);
        priceSeries = new TimeSeries("Price", Millisecond.class);
        volumeSeries = new TimeSeries("Volume", Millisecond.class);

        final TimeSeriesCollection dataset1 = new TimeSeriesCollection(priceSeries);
        final TimeSeriesCollection dataset2 = new TimeSeriesCollection(volumeSeries);
        final JFreeChart chart = createChart(dataset1, dataset2);

        final ChartPanel chartPanel = new ChartPanel(chart);


        final JPanel content = new JPanel(new BorderLayout());
        content.add(chartPanel);
        chartPanel.setPreferredSize(new java.awt.Dimension(2000, 1000));
        setContentPane(content);

    }

    private JFreeChart createChart(TimeSeriesCollection dataset1, TimeSeriesCollection dataset2) {
        final XYItemRenderer renderer1 = new StandardXYItemRenderer();
        final NumberAxis rangeAxis1 = new NumberAxis("Price");
        final XYPlot subplot1 = new XYPlot(dataset1, null, rangeAxis1, renderer1);
        subplot1.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);
        
        final XYItemRenderer renderer2 = new XYBarRenderer();
        final NumberAxis rangeAxis2 = new NumberAxis("Volume");
//        rangeAxis2.setAutoRangeIncludesZero(false);
        final XYPlot subplot2 = new XYPlot(dataset2, null, rangeAxis2, renderer2);
        subplot2.setRangeAxisLocation(AxisLocation.TOP_OR_LEFT);
        
        // parent plot...
        final CombinedDomainXYPlot plot = new CombinedDomainXYPlot(new NumberAxis("Domain"));
        plot.setGap(10.0);
        
        // add the subplots...
        plot.add(subplot1, 1);
        plot.add(subplot2, 1);
        plot.setOrientation(PlotOrientation.VERTICAL);
        
        ValueAxis axis = plot.getDomainAxis();
        axis.setAutoRange(true);
        axis.setFixedAutoRange(60000.0);  // 60 seconds
        
        return new JFreeChart("Stock 10101",
                              JFreeChart.DEFAULT_TITLE_FONT, plot, true);
    }
    
    public void add(Double price, Integer volume){
        priceSeries.addOrUpdate(new Millisecond(new Date()), price);
        volumeSeries.addOrUpdate(new Millisecond(new Date()), volume);
    }
    
    public void run() throws InterruptedException {

        final PriceVolumeChart demo = new PriceVolumeChart("Dynamic Data Demo");
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);
        StockPrice.setStockVolume(10101, 0);
        while (true) {
            final Millisecond now = new Millisecond();
            System.out.println("Now = " + now.toString());
            demo.add(StockPrice.getStockPrice(10101), StockPrice.getStockVolume(10101));
            StockPrice.setStockVolume(10101, 0);
            Thread.sleep(1000);
        }

    }
}
