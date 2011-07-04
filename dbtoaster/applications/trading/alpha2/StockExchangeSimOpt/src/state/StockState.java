/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package state;

import java.util.HashMap;
import java.util.Map;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

/**
 *
 * Records the connection state of each user and the channels corresponding to the users.
 * @author kunal
 */
public class StockState {

    //static final Map<Integer, Double> stockPriceState;
    ChannelGroup subscribers;
    Map<Integer, Integer> traderToChannelIdMap;
    Map<Integer, Channel> channelIdToChannelMap;
    public static final Integer tickLength = 1000;

    public void addSubscriber(Channel ch) {
        subscribers.add(ch);
        channelIdToChannelMap.put(ch.hashCode(), ch);
    }

    public void removeSubscriber(Channel ch) {
        subscribers.remove(ch);
    }

    public void addToMap(Integer traderId, Integer channelId) {
        traderToChannelIdMap.put(traderId, channelId);
    }

    public Integer getFromMap(Integer traderId) {
        return traderToChannelIdMap.get(traderId);
    }

    public Channel getChannel(Integer channelId) {
        return channelIdToChannelMap.get(channelId);
    }

    public ChannelGroup getSubscribers() {
        return subscribers;
    }
    

    public void init() {
        //TODO: complete this to initialise stock market state
        subscribers = new DefaultChannelGroup();
        channelIdToChannelMap = new HashMap<Integer, Channel>();
        traderToChannelIdMap = new HashMap<Integer, Integer>();
        
    }
}
