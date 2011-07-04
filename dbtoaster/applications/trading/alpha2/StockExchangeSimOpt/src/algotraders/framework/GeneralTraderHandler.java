/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.framework;

import codecs.TupleDecoder;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import rules.Matcher;
import state.OrderBook;
import state.StockState;

/**
 *
 * Abstract definition of a generic handler for trader. 
 * @author kunal
 */
public abstract class GeneralTraderHandler extends SimpleChannelHandler{
    //Generic handler variables
    public OrderBook orderBook;
    public TupleDecoder parser;
    public List<String> schema;
    public Matcher matchMaker;
    public StockState stockState;
    public Map<Integer, GeneralStockPropts> stockInfo;
    public WatchList watchList;
    public FileWriter handlerLog;
    public int orderId = 0;
    public static final Logger logger = Logger.getLogger("handler_log");
    
    /**
     * 
     * Initialize global variables.
     * @param simOrderBook The orderbook of for the trader this handler handles
     * @param watchList The list of stocks to keep an eye on
     * @param stockInfo List of {@link GeneralStockPropts} objects for each stock
     * @param matchMaker the matcher object
     * @param stockState The connection state
     * @param t The parser
     * @throws IOException 
     */
    public void generalInit(OrderBook simOrderBook, WatchList watchList, Map<Integer, GeneralStockPropts> stockInfo, 
            Matcher matchMaker, StockState stockState, TupleDecoder t) throws IOException{
        //General initialisation
        this.matchMaker = matchMaker;
        this.orderBook = simOrderBook;
        this.parser = t;
        this.schema = OrderBook.getSchemaKeys();
        this.stockState = stockState;
        this.watchList = watchList;
        this.stockInfo = stockInfo;
        handlerLog = new FileWriter("basic_sobi_handler_log");
    }
    
    /**
     * 
     * Overriden method from simpleChannelHandler.
     * @param ctx
     * @param e
     * @throws IOException 
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws IOException {
        String buffer = (String) e.getMessage();
        String[] payloads = buffer.split("\n");
        for (String payload : payloads) {
            try {
                System.out.println("MarketUpdate: " + payload);
                runSim(payload, e.getChannel());
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
            
        }
    }
    
    /**
     * 
     * Abstract method to describe action on receiving a message. Complete to create a handler.
     * @param payload
     * @param ch
     * @throws Exception 
     */
    public abstract void runSim(String payload, Channel ch)throws Exception;
}
