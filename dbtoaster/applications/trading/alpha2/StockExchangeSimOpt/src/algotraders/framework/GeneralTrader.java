/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.framework;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jboss.netty.channel.Channel;
import rules.Matcher;
import state.OrderBook;
import state.StockState;

/**
 *
 * General model for algorithmic trader. Implement to complete a trader.
 * @author kunal
 */
public abstract class GeneralTrader {
    
    public OrderBook simOrderBook;
    public Matcher matchMaker;
    public WatchList watchList;
    public Map<Integer, GeneralStockPropts> stockInfo;
    public StockState stockState;
    public Channel ch;
    
    /**
     * Initialize standard global variables.
     * @throws IOException 
     */
    public void generalInit() throws IOException{
        Terminal t = new Terminal();
        this.matchMaker = t.matchmaker;
        this.stockState = t.stockState;
        simOrderBook = t.orderBook;
        this.watchList = WatchList.createDefaultList();
        this.stockInfo = new HashMap<Integer, GeneralStockPropts>();
    }
    
    public void generalInit(List<Integer> watchList) throws IOException{
        Terminal t = new Terminal();
        this.matchMaker = t.matchmaker;
        this.stockState = t.stockState;
        simOrderBook = t.orderBook;
        this.watchList = new WatchList(watchList);
        this.stockInfo = new HashMap<Integer, GeneralStockPropts>();
    }
    
}
