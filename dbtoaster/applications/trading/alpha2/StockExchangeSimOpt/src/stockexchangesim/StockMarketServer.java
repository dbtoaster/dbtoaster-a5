/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package stockexchangesim;

import state.StockState;
import codecs.TupleDecoder;
import state.OrderBook;
import java.io.IOException;
import handlers.OrderMatchingHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import rules.Matcher;
/**
 *
 * The stock market server.
 * @author kunal
 */
public class StockMarketServer {
    
    OrderBook orderBook;
    Semaphore obLock,sLock;
    Matcher m;
    StockState stockState;
    
    public StockMarketServer() throws IOException{
        Terminal t = new Terminal();
        this.orderBook = t.orderBook;
        this.obLock = t.dbLock;
        this.sLock = t.sLock;
        this.m = t.matchmaker;
        this.stockState = t.stockState;
        //logger = LoggerFactory.getLogger("stream_logger");
        
        //Initialise the market
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new StockMarketChannelFactory());
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.bind(new InetSocketAddress(8080));
    }
    
    public class StockMarketChannelFactory implements ChannelPipelineFactory{

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(
                    new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                    new StringDecoder(), new StringEncoder(),
                    new OrderMatchingHandler(orderBook, obLock, sLock, new TupleDecoder(OrderBook.getSchema()), m, stockState));
        }
        
    }
}
