/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import GUI.PriceChart;
import algotraders.HistoricTrader;
import dynamicordergen.traderthreads.OperatorThread;
import dynamicordergen.traderthreads.PriceThread;
import dynamicordergen.traderthreads.TrendFollowerThread;
import dynamicordergen.traderthreads.TrendSkepticThread;
import dynamicordergen.utils.OrderGenClient;
import java.io.IOException;
import java.util.Date;
import org.jfree.ui.RefineryUtilities;
import stockexchangesim.StockMarketServer;

/**
 *
 * @author kunal
 */
public class TradeGenTest {

    public static void main(String args[]) throws IOException, InterruptedException {
        StockMarketServer s = new StockMarketServer();
        
        PriceThread p = new PriceThread();
        p.start();
        
        OrderGenClient client = new OrderGenClient();
        
        System.out.println("Starting operator thread");
        OperatorThread optThread = new OperatorThread(10101, 5, 500, 6, 3, client);
        optThread.start();
        
        System.out.println("Starting follower thread");
        TrendFollowerThread followerThread = new TrendFollowerThread(10101, 10, 100, 5, 100, client);
        followerThread.start();
        
        System.out.println("Starting skeptic thread");
        TrendSkepticThread skeptThread = new TrendSkepticThread(10101, 5, 70, 10, 75, client);
        skeptThread.start();
        
        long timestamp = new Date().getTime();
        
        while(new Date().getTime() < timestamp+100000){}
        
        optThread.stop();
        followerThread.stop();
        skeptThread.stop();
        client.close();
    }
}
