/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package stockexchangesim;

import java.util.Map;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

/**
 *
 * @author kunal
 */
public class StockState{
        Map<Integer, Integer> stockPriceState;
        ChannelGroup subscribers;
        
        public Integer getStockPrice(Integer stockId){
            return stockPriceState.get(stockId);
        }
        
        public void setStockPrice(Integer stockId, Integer price){
            stockPriceState.put(stockId, price);
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
        }
    }
