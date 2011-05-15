/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package stockexchangesim;

import codecs.TupleDecoder;
import connections.OrderBook;
import java.io.IOException;
import rules.MatchRules;
import handlers.OrderMatchingHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
/**
 *
 * @author kunal
 */
public class StockMarketServer {
    
    OrderBook orderBook;
    Semaphore lock;
    MatchRules m;
    StockState stockState;
    
    public StockMarketServer() throws IOException{
        Terminal t = new Terminal();
        this.orderBook = t.orderBook;
        this.lock = t.dbLock;
        this.m = t.matchmaker;
        this.stockState = t.stockState;
        //logger = LoggerFactory.getLogger("stream_logger");
    }
    
    public class StockMarketChannelFactory implements ChannelPipelineFactory{

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(
                    new StringDecoder(), new StringEncoder(),
                    new OrderMatchingHandler(orderBook, lock, new TupleDecoder(OrderBook.getSchema()), m, stockState));
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        StockMarketServer s = new StockMarketServer();
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(s.new StockMarketChannelFactory());
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.bind(new InetSocketAddress(8080));
    }
}
