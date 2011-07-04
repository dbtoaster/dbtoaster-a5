/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import algotraders.HistoricTrader;
import java.io.IOException;
import nasdaq.MarketMaker;
import stockexchangesim.StockMarketServer;

/**
 *
 * @author kunal
 */
public class MarketMakerTest {

    public static void main(String args[]) throws IOException {
        StockMarketServer s = new StockMarketServer();
        
        HistoricTrader ht = new HistoricTrader();
        ht.run();
        
        MarketMaker mm = new MarketMaker(0.1, 1000, 1000, 10101);
        mm.execute();
        
    }
}
