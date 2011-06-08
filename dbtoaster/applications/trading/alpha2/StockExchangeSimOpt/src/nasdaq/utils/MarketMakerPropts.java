/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nasdaq.utils;

import org.jboss.netty.channel.Channel;
import rules.Matcher;
import state.OrderBook;
import state.StockState;

/**
 *
 * @author kunal
 */
public class MarketMakerPropts {
    public OrderBook simOrderBook;
    public Double bidBaseSpread;
    public Double askBaseSpread;
    public Double curBidSpread;
    public Double curAskSpread;
    public Double bidQuote;
    public Double askQuote;
    public Integer maxVolTradeable;
    public Integer marketMakerStock;
    public Double portfolioValue;
    public Integer stockHeld;
    public Integer windowLength;
    public StockState stockState;
    public Matcher matchMaker;
    public Channel ch;
    //Window based Properties here
    public WindowPropts curWindowPropts;
    
    
}
