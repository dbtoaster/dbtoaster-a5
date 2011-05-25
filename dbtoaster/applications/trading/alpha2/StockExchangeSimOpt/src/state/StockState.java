/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package state;

import algotraders.utils.WatchList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

/**
 *
 * @author kunal
 */
public class StockState{
        Map<Integer, Double> stockPriceState;
        ChannelGroup subscribers;
         
        public Double getStockPrice(Integer stockId){
            return stockPriceState.get(stockId);
        }
        
        public Double setStockPrice(Integer stockId, Double price){
            return stockPriceState.put(stockId, price);
        }
        
        public void addSubscriber(Channel ch){
            subscribers.add(ch);
        }
        
        public void removeSubscriber(Channel ch){
            subscribers.remove(ch);
        }
        
        public ChannelGroup getSubscribers(){
            return subscribers;
        }
        
        public void init(){
            //TODO: complete this to initialise stock market state
            subscribers = new DefaultChannelGroup();
            this.stockPriceState = new HashMap<Integer, Double>();
            List<Integer> stockList = WatchList.createDefaultList().getList();
            for(Integer i : stockList){
                this.stockPriceState.put(i, 0.);
            }
        }
    }
