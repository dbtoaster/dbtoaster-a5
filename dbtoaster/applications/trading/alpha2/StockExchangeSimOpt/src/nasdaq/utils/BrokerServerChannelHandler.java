/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package nasdaq.utils;

import nasdaq.Broker.StateInfo;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 *
 * Again left open, but essentially this may be the same as {@link MarketMakerServerHandler}. When order is received, just parse out the details, find the best channel for that order,
 * (from {@link StateInfo} object in {@link Broker} and send the order to that channel.
 * @author Kunal Shah
 */
public class BrokerServerChannelHandler extends SimpleChannelHandler{

}
