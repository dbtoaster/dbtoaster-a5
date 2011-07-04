/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.basicsobitrader;

import algotraders.framework.GeneralStockPropts;
import algotraders.framework.GeneralTraderHandler;
import algotraders.framework.WatchList;
import codecs.TupleDecoder;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import rules.Matcher;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;
import state.StockPrice;
import state.StockState;

/**
 *
 * The handler for the BasicSobi trader channel to the market
 * @author kunal
 */
public class BasicSobiTraderHandler extends GeneralTraderHandler {
    
    //Variables local to implementation
    Map<Integer, Long> pendingOrders;
    
    BasicSobiTraderHandler(OrderBook simOrderBook, WatchList watchList, Map<Integer, GeneralStockPropts> stockInfo, 
            Matcher matchMaker, StockState stockState, TupleDecoder t) throws IOException {
        
        //General initialisation
        generalInit(simOrderBook, watchList, stockInfo, matchMaker, stockState, t);
        
        //Local initialisations
        pendingOrders = new HashMap<Integer, Long>();
    }
    
    /**
     * Implemented method from parent class {@link GeneralTraderHandler}
     * 
     * @param payload
     * @param ch
     * @throws IOException 
     */
    @Override
    public void runSim(String payload, Channel ch) throws IOException {
        String[] contents = payload.split(";");
        
        //Parse the tuple
        Map<String, Object> decodedLoad = parser.createTuples(contents[1]);
        
        
        if (decodedLoad != null) {
            Object a[] = new Object[schema.size()];
            for (int i = 0; i < schema.size(); i++) {
                a[i] = decodedLoad.get(schema.get(i));
            }

            //Create a new order book entry
            OrderBookEntry newEntry = null;            
            try {
                newEntry = orderBook.createEntry(a);
                handlerLog.write(String.format("Added order: stock:%s price:%s volume:%s order_id:%s timestamp:%s trader:%s\n",
                        newEntry.stockId,
                        newEntry.price,
                        newEntry.volume,
                        newEntry.order_id,
                        newEntry.timestamp,
                        (newEntry.traderId == -1) ? "historic" : newEntry.traderId));
                
                orderBook.executeCommand(contents[0], newEntry);
            } catch (IOException ex) {
                System.err.println(ex.getMessage());
                return;
            }
            
            //Run matching algorithm
            matchMaker.match(contents[0], newEntry);
            
            
            //Code for generation of new trade
            BasicSobiPropts oldPropts = (BasicSobiPropts) stockInfo.get(newEntry.stockId);
            oldPropts.updatePrice(StockPrice.getStockPrice(newEntry.stockId));
            oldPropts.updatePending();
            
            String trade = null;
            Integer trader = 0;
            if (a[5] != null) {
                trader = (Integer) a[5];
            }
            if (trader == this.hashCode()) {
                trade = null;
                long endtimestamp = new Date().getTime();
                Integer order_id = (Integer)a[3];
                long startTimestamp = pendingOrders.get(order_id);
                long latency = endtimestamp - startTimestamp;
                System.err.println("Order No. "+order_id+" took "+latency+" ms.");
                pendingOrders.remove(order_id);
                
            } else {
                trade = oldPropts.getTrade();
            }
            if (trade != null) {
                trade = String.format("%s stock_id:%s trader:%s order_id:%s", trade, newEntry.stockId, this.hashCode(), orderId++);
                pendingOrders.put(orderId-1, new Date().getTime());
                ChannelFuture cf = ch.write(trade + "\n");
                System.out.println("Adding new order: " + trade);
                
                cf.awaitUninterruptibly();
            }
            
            
        }
    }
}
