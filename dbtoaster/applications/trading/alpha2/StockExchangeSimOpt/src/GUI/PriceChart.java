/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package GUI;

import java.awt.BorderLayout;
import java.util.Date;

import javax.swing.JPanel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import state.StockPrice;

/**
 *
 * @author kunal
 */
public class PriceChart extends ApplicationFrame {

    /** The time series data. */
    private TimeSeries series;
    

    public PriceChart(String title) {
        super(title);
        series = new TimeSeries("Random Data", Millisecond.class);
        final TimeSeriesCollection dataset = new TimeSeriesCollection(series);
        final JFreeChart chart = createChart(dataset);

        final ChartPanel chartPanel = new ChartPanel(chart);


        final JPanel content = new JPanel(new BorderLayout());
        content.add(chartPanel);
        chartPanel.setPreferredSize(new java.awt.Dimension(2000, 1000));
        setContentPane(content);
    }

    private JFreeChart createChart(final XYDataset dataset) {
        
        final JFreeChart result = ChartFactory.createTimeSeriesChart(
                "Dynamic Data Demo",
                "Time",
                "Value",
                dataset,
                true,
                true,
                false);
        final XYPlot plot = result.getXYPlot();
        ValueAxis axis = plot.getDomainAxis();
        axis.setAutoRange(true);
        axis.setFixedAutoRange(60000.0);  // 60 seconds
        axis = plot.getRangeAxis();
        axis.setRange(0.0, 200.0);
        return result;
    }

    public void add(Double price) {
        series.addOrUpdate(new Millisecond(new Date()), price);

    }

    public void run() throws InterruptedException {

        final PriceChart demo = new PriceChart("Dynamic Data Demo");
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);
        
        while (true) {
            final Millisecond now = new Millisecond();
            System.out.println("Now = " + now.toString());
            demo.add(StockPrice.getStockPrice(10101));
            Thread.sleep(1000);
        }

    }
}