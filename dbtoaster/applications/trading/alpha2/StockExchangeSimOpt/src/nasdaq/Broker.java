/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nasdaq;

import algotraders.basicsobitrader.BasicSobiPropts;
import algotraders.framework.GeneralStockPropts;
import codecs.TupleDecoder;
import handlers.DefaultHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import nasdaq.utils.BrokerClientChannelHandler;
import nasdaq.utils.BrokerServerChannelHandler;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import rules.Matcher;
import state.OrderBook;

/**
 *
 * Simulation of a broker interface. This interface acts as a client as well as a server. It is a client to the market as well as other market maker feeds that this 
 * broker subscribes to. It is a server interface for many private persons that may want to trade in the market. The broker's function is simple. When one of his clients 
 * wants to trade in the market, he finds the channel which is providing the best offer for the same. This channel may be a market maker channel or the market itself. 
 * The order is subsequently sent to the best source.
 * 
 * Note: This is an implementation for one stock only. Use map of {@link Integer}, {@link StateInfo} Objects for multi stock
 * 
 * @author Kunal Shah
 */
public class Broker {

    List<Channel> marketMakers;
    OrderBook orderBook;
    Matcher matcher;
    GeneralStockPropts stockPropts;
    
    //This holds the information about what is best bid/ask price in the market currently and which channel is offering it.
    public class StateInfo {

        Channel bestBidChannel;
        Double bestBidPrice;
        Channel bestAskChannel;
        Double bestAskPrice;
    }
    public static StateInfo stateInfo;

    /**
     * Replaces the best bid channel if needed.
     * 
     * @param price
     * @param ch 
     */
    public synchronized static void setBestBid(Double price, Channel ch) {
        if (stateInfo.bestBidPrice < price) {
            stateInfo.bestBidPrice = price;
            stateInfo.bestBidChannel = ch;
        }
    }

    /**
     * Replaces the best ask channel if needed.
     * 
     * @param price
     * @param ch 
     */
    public synchronized static void setBestAsk(Double price, Channel ch) {
        if (stateInfo.bestAskPrice > price) {
            stateInfo.bestAskPrice = price;
            stateInfo.bestAskChannel = ch;
        }
    }

    public Broker() {
        orderBook = new OrderBook();
        stateInfo.bestAskPrice = Double.MAX_VALUE;
        stateInfo.bestBidPrice = 0.;
        stockPropts = new BasicSobiPropts(0., 0, 0., orderBook);
        marketMakers = new ArrayList<Channel>();
        this.init();

    }

    /**
     * 
     * Channel factory for client connections TO the broker
     */
    public class BrokerServerChannelFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(
                    new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                    new StringDecoder(), new StringEncoder(),
                    new BrokerServerChannelHandler());
        }
    }

    /**
     * 
     * Channel factory for broker's connections to the {@link MarketMaker}
     */
    public class BrokerClientChannelFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(
                    new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                    new StringDecoder(), new StringEncoder(),
                    new BrokerClientChannelHandler());
        }
    }
    
    /**
     * 
     * Channel factory for the broker connection to the Market.
     */
    public class MarketChannelFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(
                    new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                    new StringDecoder(), new StringEncoder(),
                    new DefaultHandler(matcher, new TupleDecoder(OrderBook.getSchema()), orderBook, stockPropts));
        }

        
    }

    /**
     * 
     * Initialize a connection to the market, and start up a server clients can connect to. This does not connect to any market makers.
     */
    private void init() {

        //Create a server for traders to place their orders.
        //Create server
        ChannelFactory serverFactory =
                new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ServerBootstrap bootstrap = new ServerBootstrap(serverFactory);

        bootstrap.setPipelineFactory(new BrokerServerChannelFactory());
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.bind(new InetSocketAddress(8081));

        //Create a connection to the market
        ChannelFactory clientFactory =
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ClientBootstrap bootstrap2 = new ClientBootstrap(clientFactory);
        bootstrap2.setPipelineFactory(new MarketChannelFactory());

        bootstrap2.setOption("tcpNoDelay", true);
        bootstrap2.setOption("keepAlive", true);

        ChannelFuture cf = bootstrap2.connect(new InetSocketAddress("localhost", 8080));
    }

    /**
     * 
     * Add a market maker connection
     * @param port The port where the market maker is hosted (assuming server is at localhost). Modify if server variable.
     */
    public void addMarketMakerFeed(Integer port) {
        //Create a new client for the given server
        //Create client
        ChannelFactory clientFactory =
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ClientBootstrap bootstrap2 = new ClientBootstrap(clientFactory);
        bootstrap2.setPipelineFactory(new BrokerClientChannelFactory());

        bootstrap2.setOption("tcpNoDelay", true);
        bootstrap2.setOption("keepAlive", true);

        ChannelFuture cf = bootstrap2.connect(new InetSocketAddress("localhost", port));
        Channel ch = cf.awaitUninterruptibly().getChannel();
        marketMakers.add(ch);
    }
}
