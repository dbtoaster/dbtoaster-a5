package org.dbtoaster.gui;

import java.util.Iterator;

import org.dbtoaster.model.DBToasterWorkspace;
import org.dbtoaster.model.Query;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.part.ViewPart;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.LegendItem;
import org.jfree.chart.LegendItemCollection;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.experimental.chart.swt.ChartComposite;

public class CodePerfView extends ViewPart
{

    public static final String ID = "dbtoaster_gui.codeperfview";

    DBToasterWorkspace dbtWorkspace;
    ChartComposite chartComp;

    public void createPartControl(Composite parent)
    {
        dbtWorkspace = DBToasterWorkspace.getWorkspace();
        //Query q = dbtWorkspace.getExecutedQuery();
        //CategoryDataset cd = buildStatisticsCategories(
        //    q.getExecutor().getStatisticsProfile());

        Composite top = new Composite(parent, SWT.NONE);
        GridLayout layout = new GridLayout();
        layout.marginWidth = 0;
        layout.marginHeight = 0;
        top.setLayout(layout);
        top.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

        JFreeChart handlerChart = ChartFactory.createStackedBarChart(
                "Handler execution breakdown", "Handler function",
                "Exec time (ms)", null, PlotOrientation.HORIZONTAL, true,
                false, false);

        chartComp = new ChartComposite(top, SWT.NONE, handlerChart, true);
        chartComp.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

        CategoryDataset chartData = buildVwapHandlerDataset();
        CategoryPlot plot = handlerChart.getCategoryPlot();
        plot.setDataset(chartData);
        LegendItemCollection litems = new LegendItemCollection();
        for (Iterator<?> it = chartData.getRowKeys().iterator(); it.hasNext();)
        {
            litems.add(new LegendItem((String) it.next()));
        }

        plot.setFixedLegendItems(litems);
        
        dbtWorkspace.setCategoryPlot(plot);
        
    }

    CategoryDataset buildVwapHandlerDataset()
    {
        DefaultCategoryDataset r = new DefaultCategoryDataset();
        r.addValue(30, "<update q[pmin]>", "on_insert_B(p,v)");
        r.addValue(10, "<insert q[pmin]>", "on_insert_B(p,v)");
        r.addValue(41, "<update m[p2]>", "on_insert_B(p,v)");
        r.addValue(15, "<insert m[p2]>", "on_insert_B(p,v)");
        r.addValue(4, "<compute pmin>", "on_insert_B(p,v)");
        r.addValue(0.5, "<compute result>", "on_insert_B(p,v)");

        r.addValue(23, "<update q[pmin]>", "on_delete_B(p,v)");
        r.addValue(17, "<insert q[pmin]>", "on_delete_B(p,v)");
        r.addValue(45, "<update m[p2]>", "on_delete_B(p,v)");
        r.addValue(11, "<insert m[p2]>", "on_delete_B(p,v)");
        r.addValue(2, "<compute pmin>", "on_delete_B(p,v)");
        r.addValue(2, "<compute result>", "on_delete_B(p,v)");
        return r;
    }

    public void setFocus() {}

}
