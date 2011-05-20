/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package handlers;

import state.StockState;
import rules.impl.BasicMatcher;
import codecs.TupleDecoder;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;

import java.net.SocketAddress;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

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
    BasicMatcher matchMaker;
    StockState stockState;
    static final Logger logger = Logger.getLogger("handler_log");

    public OrderMatchingHandler(OrderBook conn, Semaphore obLock,Semaphore sLock,  TupleDecoder t, BasicMatcher m, StockState stockState) {
        orderBook = conn;
        orderBookLock = obLock;
        streamLock = sLock;
        parser = t;
        schema = OrderBook.getSchemaKeys();
        matchMaker = m;
        this.stockState = stockState;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        String buffer = (String) e.getMessage();
        String[] payloads = buffer.split("\n");
        try {
            for (String payload : payloads) {
                //String retMsg = payload;
                String[] contents = payload.split(";");
                //System.out.println("Order received: " + contents[1]);
                Map<String, Object> decodedLoad = parser.createTuples(contents[1]);

                if (decodedLoad != null) {
                    Object a[] = new Object[schema.size()-1];
                    for (int i = 0; i < schema.size()-1; i++) {
                        a[i] = decodedLoad.get(schema.get(i));
                    }
                    //a[4] = e.getChannel().hashCode();
                    OrderBookEntry newEntry = orderBook.createEntry((Integer)a[0],
                            (Integer) a[1],
                            (Integer) a[2],
                            new Date().getTime(),
                            e.getChannel().hashCode());
                    orderBookLock.acquireUninterruptibly();
                    orderBook.executeCommand(contents[0], newEntry);
                    orderBookLock.release();

                    orderBookLock.acquireUninterruptibly();
                    matchMaker.match(contents[0], newEntry);
                    orderBookLock.release();
                    
                    streamLock.acquireUninterruptibly();
                    for(Channel ch : stockState.getSubscribers()){
                        ChannelFuture c = ch.write(payload+"\n");
                        //c.await();
                    }
                    streamLock.release();
                }
            }
            //Send update on all existing streams
//            for (Channel c : stockState.getSubscribers()) {
//                c.write(e.getMessage());
//            }
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        e.getCause().printStackTrace();
        Channel ch = e.getChannel();
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
