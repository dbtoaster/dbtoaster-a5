/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dynamicordergen.traderthreads;

import GUI.PriceVolumeChart;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jfree.ui.RefineryUtilities;

/**
 *
 * A thread to start the graph display of price and volume
 * @author kunal
 */
public class PriceThread extends Thread {

    PriceVolumeChart p;

    public PriceThread() {
        p = new PriceVolumeChart("Dynamic Data Demo");
        p.pack();
        RefineryUtilities.centerFrameOnScreen(p);
        p.setVisible(true);
    }

    @Override
    public void run() {
        try {
            p.run();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
}
