/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package handlers;

import java.io.IOException;
import state.StockState;
import rules.impl.BasicMatcher;
import codecs.TupleDecoder;
import java.io.FileWriter;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;

import java.net.SocketAddress;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import rules.Matcher;

/**
 *
 * Form of input tuples assumed is buy/sell;key:value key:value key:value....
 * Also if price field is 0 in the input then order is assumed to be a market order
 * 
 * @author kunal
 */
public class OrderMatchingHandler extends SimpleChannelHandler {

    OrderBook orderBook;
    Semaphore orderBookLock;
    Semaphore streamLock;
    TupleDecoder parser;
    List<String> schema;
    Matcher matchMaker;
    StockState stockState;
    static final Logger logger = Logger.getLogger("handler_log");
    FileWriter handlerLog;
    String pending = "";

    public OrderMatchingHandler(OrderBook conn, Semaphore obLock, Semaphore sLock, TupleDecoder t, Matcher m, StockState stockState) throws IOException {
        orderBook = conn;
        orderBookLock = obLock;
        streamLock = sLock;
        parser = t;
        schema = OrderBook.getSchemaKeys();
        matchMaker = m;
        this.stockState = stockState;
        handlerLog = new FileWriter("handler_log");
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws IOException {
        String buffer = (String) e.getMessage();
        String[] payloads = buffer.split("\n");
        
        try {
            for (String payload : payloads) {
                //String retMsg = payload;
                String[] contents = payload.split(";");
                System.out.println("Order received: " + payload);
                Map<String, Object> decodedLoad = parser.createTuples(contents[1]);

                if (decodedLoad != null) {
                    Object a[] = new Object[schema.size() ];
                    for (int i = 0; i < schema.size(); i++) {
                        a[i] = decodedLoad.get(schema.get(i));
                    }
                    //a[4] = e.getChannel().hashCode();
                    OrderBookEntry newEntry = orderBook.createEntry(a);
                    String msg = String.format("%s;stock_id:%s price:%s volume:%s order_id:%s timestamp:%s trader:%s\n",
                            contents[0],
                            newEntry.stockId,
                            newEntry.price,
                            newEntry.volume,
                            newEntry.order_id,
                            newEntry.timestamp,
                            newEntry.traderId);
                    //handlerLog.write(msg);
                    System.out.println(msg);
                    orderBookLock.acquireUninterruptibly();
                    orderBook.executeCommand(contents[0], newEntry);
                    orderBookLock.release();
                    
                    stockState.addToMap(newEntry.traderId, e.getChannel().hashCode());

                    if (!contents[0].equals(OrderBook.DELETECOMMANDSYMBOL)) {
                        orderBookLock.acquireUninterruptibly();
                        matchMaker.match(contents[0], newEntry);
                        orderBookLock.release();
                    }
                    streamLock.acquireUninterruptibly();
                    for (Channel ch : stockState.getSubscribers()) {
                        ChannelFuture c = ch.write(msg);
                        //c.await();
                    }
                    streamLock.release();
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
            handlerLog.write("The order format had an error. Skipping\n");
            //System.out.println("The order format had an error. Skipping\n");
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        e.getCause().printStackTrace();
        Channel ch = e.getChannel();
        stockState.removeSubscriber(e.getChannel());
        ch.close();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        SocketAddress address = e.getChannel().getRemoteAddress();
        stockState.addSubscriber(e.getChannel());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        stockState.removeSubscriber(e.getChannel());
        
    }
}
