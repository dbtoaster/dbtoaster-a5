package org.dbtoaster.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.math.BigDecimal;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.JSplitPane;

import org.dbtoaster.gui.DBPerfPanel.StatPanel;

import net.quies.math.plot.ChartStyle;
import net.quies.math.plot.Function;
import net.quies.math.plot.Graph;
import net.quies.math.plot.InteractiveGraph;

public class DBSwingPerfPanel extends JPanel
{

    private static final long serialVersionUID = -4537920835189413132L;

    /*
     * class StatPanel { public Visualization vis; public Display display;
     * public Rectangle2D dlb; public Rectangle2D xb; public Rectangle2D yb;
     * 
     * public StatPanel() { vis = new Visualization(); display = new
     * Display(vis); dlb = new Rectangle2D.Double(); xb = new
     * Rectangle2D.Double(); yb = new Rectangle2D.Double(); }
     * 
     * public StatPanel(Visualization vis, Display display, Rectangle2D dlb,
     * Rectangle2D xb, Rectangle2D yb) { this.vis = vis; this.display = display;
     * this.dlb = dlb; this.xb = xb; this.yb = yb; } };
     * 
     * StatPanel cpuPanel; StatPanel memPanel;
     * 
     * private static final String group = "data";
     * 
     * public DBPerfPanel() { setLayout(new BorderLayout());
     * 
     * cpuPanel = new StatPanel(); memPanel = new StatPanel();
     * 
     * //cpuPanel.display.setSize(500,400); //cpuPanel.display.pan(50, 50);
     * cpuPanel.display.setForeground(Color.GRAY);
     * cpuPanel.display.setBackground(Color.WHITE);
     * cpuPanel.display.setBorder(BorderFactory.createLineBorder(Color.BLUE));
     * 
     * //memPanel.display.setSize(500,400); //memPanel.display.pan(50, 50);
     * memPanel.display.setForeground(Color.GRAY);
     * memPanel.display.setBackground(Color.BLACK);
     * memPanel.display.setBorder(BorderFactory.createLineBorder(Color.RED));
     * 
     * setupPlot(cpuPanel, "t", "cpu usage", "cpu"); setupPlot(memPanel, "t",
     * "mem usage", "mem");
     * 
     * JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
     * split.setLeftComponent(cpuPanel.display);
     * split.setRightComponent(memPanel.display);
     * split.setOneTouchExpandable(true); split.setContinuousLayout(false);
     * split.setDividerSize(3); split.setResizeWeight(0.5);
     * 
     * add(split, BorderLayout.CENTER);
     * 
     * cpuPanel.vis.run("draw"); memPanel.vis.run("draw"); }
     * 
     * private void setupPlot(StatPanel p, String xfield, String yfield, String
     * sfield) { //
     * -------------------------------------------------------------------- //
     * STEP 1: setup the visualized data Table t = new Table();
     * t.addColumn(xfield, double.class); t.addColumn(yfield, double.class);
     * t.addColumn(sfield, double.class); for (int i=0; i < 100; ++i) {
     * t.addRow(); t.setDouble(i, 0, i*0.2); t.setDouble(i, 1,
     * 10+i*Math.random()); t.setDouble(i, 2, 20+i*Math.random()); }
     * 
     * p.vis.addTable(group, t);
     * 
     * p.vis.setRendererFactory(new RendererFactory() { AbstractShapeRenderer sr
     * = new ShapeRenderer(); Renderer arY = new AxisRenderer(Constants.RIGHT,
     * Constants.TOP); Renderer arX = new AxisRenderer(Constants.CENTER,
     * Constants.FAR_BOTTOM);
     * 
     * public Renderer getRenderer(VisualItem item) { return
     * item.isInGroup("ylab") ? arY : item.isInGroup("xlab") ? arX : sr; } });
     * 
     * // --------------------------------------------------------------------
     * // STEP 2: create actions to process the visual data
     * 
     * // set up the actions AxisLayout x_axis = new AxisLayout(group, xfield,
     * Constants.X_AXIS, VisiblePredicate.TRUE); p.vis.putAction("x", x_axis);
     * 
     * AxisLabelLayout x_label = new AxisLabelLayout("xlab", x_axis, p.xb, 25);
     * 
     * AxisLayout y_axis = new AxisLayout(group, yfield, Constants.Y_AXIS,
     * VisiblePredicate.TRUE); p.vis.putAction("y", y_axis);
     * 
     * AxisLabelLayout y_label = new AxisLabelLayout("ylab", y_axis, p.yb, 18);
     * 
     * x_axis.setLayoutBounds(p.dlb); y_axis.setLayoutBounds(p.dlb);
     * 
     * ColorAction color = new ColorAction(group, VisualItem.STROKECOLOR,
     * ColorLib.rgb(100,100,255)); p.vis.putAction("color", color);
     * 
     * int[] shapes = new int[] { Constants.SHAPE_DIAMOND }; DataShapeAction
     * shape = new DataShapeAction(group, sfield, shapes);
     * p.vis.putAction("shape", shape);
     * 
     * ActionList draw = new ActionList(); draw.add(x_axis); draw.add(y_axis);
     * draw.add(x_label); draw.add(y_label); if ( sfield != null )
     * draw.add(shape); draw.add(color); draw.add(new RepaintAction());
     * p.vis.putAction("draw", draw);
     * 
     * ActionList update = new ActionList(); update.add(x_axis);
     * update.add(y_axis); update.add(x_label); update.add(y_label);
     * update.add(new RepaintAction()); p.vis.putAction("update", update);
     * 
     * p.display.setBorder(BorderFactory.createEmptyBorder(10,10,10,10));
     * p.display.setSize(700,450); p.display.setHighQuality(true);
     * p.display.addComponentListener(new ComponentAdapter() { public void
     * componentResized(ComponentEvent e) { System.out.println("Resizing");
     * displayLayout(cpuPanel); displayLayout(memPanel); } });
     * displayLayout(cpuPanel); displayLayout(memPanel); }
     * 
     * public void displayLayout(StatPanel p) { Insets i =
     * p.display.getInsets(); int w = p.display.getWidth(); int h =
     * p.display.getHeight(); int iw = i.left+i.right; int ih = i.top+i.bottom;
     * int aw = 85; int ah = 15;
     * 
     * System.out.println(Integer.toString(w) +", "+ Integer.toString(h));
     * p.dlb.setRect(i.left, i.top, w-iw-aw, h-ih-ah); p.xb.setRect(i.left,
     * h-ah-i.bottom, w-iw-aw, ah-10); p.yb.setRect(i.left, i.top, w-iw,
     * h-ih-ah);
     * 
     * p.vis.run("update"); }
     */

    class RandomFeed extends TimerTask
    {

        final Function function;
        BigDecimal x = new BigDecimal((Math.random() - 0.5) * 200);
        BigDecimal y = new BigDecimal((Math.random() - 0.5) * 200);

        RandomFeed(Function f)
        {
            function = f;
        }

        public void run()
        {
            double xFeed = Math.random() * 10;
            double yFeed = Math.pow((Math.random() - 0.5) * 2.5, 3.0);
            x = x.add(new BigDecimal(xFeed));
            y = y.add(new BigDecimal(yFeed));
            function.addPoint(x, y);
        }
    };

    class StatPanel extends JPanel
    {
        public Graph plot;
        public Timer t;

        public StatPanel(Color stroke)
        {
            setLayout(new BorderLayout());
            setBorder(BorderFactory.createLineBorder(Color.GRAY, 2));

            plot = new InteractiveGraph();
            t = new Timer("Feed timer");
            Function f = new Function("Stats");
            ChartStyle s = new ChartStyle();
            s.setPaint(stroke);

            plot.getXAxis().setZigZaginess(BigDecimal.valueOf(7L, 1));
            plot.getYAxis().setZigZaginess(BigDecimal.valueOf(7L, 1));
            plot.getXAxis().setPreferredSteps(10);
            plot.getYAxis().setPreferredSteps(2);
            plot.setBackground(Color.BLACK);
            plot.addFunction(f, s);

            t.schedule(new RandomFeed(f), 100L, 800L);
            t.schedule(new TimerTask()
            {

                public void run()
                {
                    plot.render();
                    plot.repaint();
                }

            }, 200L, 1000L);

            plot.render();
            plot.repaint();
            add(plot, BorderLayout.CENTER);
        }
    };

    StatPanel cpuPanel;
    StatPanel memPanel;

    public DBSwingPerfPanel()
    {
        setLayout(new BorderLayout());

        // SplitPane for Swing panels
        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        split.setLeftComponent(cpuPanel);
        split.setRightComponent(memPanel);
        split.setOneTouchExpandable(true);
        split.setContinuousLayout(false);
        split.setDividerSize(10);
        split.setResizeWeight(0.5);

        add(split, BorderLayout.CENTER);
    }

}
