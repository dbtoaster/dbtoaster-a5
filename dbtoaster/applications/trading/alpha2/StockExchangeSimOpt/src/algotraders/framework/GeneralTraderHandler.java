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
import rules.impl.BasicMatcher;
import state.OrderBook;
import state.StockState;

/**
 *
 * @author kunal
 */
public abstract class GeneralTraderHandler extends SimpleChannelHandler{
    //Generic handler variables
    public OrderBook orderBook;
    public TupleDecoder parser;
    public List<String> schema;
    public BasicMatcher matchMaker;
    public StockState stockState;
    public Map<Integer, GeneralStockPropts> stockInfo;
    public WatchList watchList;
    public FileWriter handlerLog;
    public int orderId = 0;
    public static final Logger logger = Logger.getLogger("handler_log");
    
    public void generalInit(OrderBook simOrderBook, WatchList watchList, Map<Integer, GeneralStockPropts> stockInfo, 
            BasicMatcher matchMaker, StockState stockState, TupleDecoder t) throws IOException{
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
    
    public abstract void runSim(String payload, Channel ch)throws Exception;
}
