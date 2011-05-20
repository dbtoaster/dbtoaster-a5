/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.basicsobitrader;

import algotraders.utils.SobiTerminal;
import algotraders.utils.GeneralStockPropts;
import algotraders.utils.WatchList;
import codecs.TupleDecoder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import rules.Matcher;
import rules.impl.BasicMatcher;
import state.OrderBook;
import state.StockState;

/**
 *
 * Algorithm: 
 * 1) If the volume weighted price for bids or asks is 0, then dont put in any trades. The market is building up.
 * 2) If the volume weighted price for bids is farther from market price than the volume weighted price for asks, then it means the market is going bullish,
 *          and the prices will rise. Hence it is wise to go for an ask at a price higher than the current market price.
 * 3) If the volume weighted price for asks is farther from market price than the volume weighted price for bids, then it means the market is going bearish,
 *          and the prices will fall. Hence it is wise to go for a bid at a price lower than the current market price.
 * 
 * @author kunal
 * 
 */
public class BasicSobiTrader {

    OrderBook simOrderBook;
    Matcher matchMaker;
    WatchList watchList;
    Map<Integer, GeneralStockPropts> stockInfo;
    StockState stockState;
    Integer theta, volToTrade, margin;
    Channel ch;

    public BasicSobiTrader(Integer param, Integer volToTrade, Integer margin) throws IOException {
        SobiTerminal t = new SobiTerminal();
        this.matchMaker = t.matchmaker;
        this.stockState = t.stockState;
        simOrderBook = t.orderBook;
        this.theta = param;
        this.margin = margin;
        this.volToTrade = volToTrade;

        this.watchList = WatchList.createDefaultList();
        this.stockInfo = new HashMap<Integer, GeneralStockPropts>();
        this.init();

    }

    public BasicSobiTrader(List<Integer> watchList, Integer param, Integer volToTrade, Integer margin) throws IOException {
        SobiTerminal t = new SobiTerminal();
        this.matchMaker = t.matchmaker;
        this.stockState = t.stockState;
        simOrderBook = t.orderBook;
        this.theta = param;
        this.margin = margin;
        this.volToTrade = volToTrade;

        this.watchList = new WatchList(watchList);
        this.stockInfo = new HashMap<Integer, GeneralStockPropts>();
        this.init();

    }

    public class SobiTraderChannelPipelineFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(new StringDecoder(), new StringEncoder(), new BasicSobiTraderHandler(simOrderBook, watchList, stockInfo,(BasicMatcher) matchMaker, stockState,
                    new TupleDecoder(OrderBook.getSchema())));
        }
    }

    private void init() {
        for (Integer i : watchList.getList()) {
            BasicSobiPropts newPropts = new BasicSobiPropts(this.theta, this.volToTrade, this.margin);
            stockInfo.put(i, newPropts);
        }

        ChannelFactory factory =
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new SobiTraderChannelPipelineFactory());

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        ChannelFuture cf = bootstrap.connect(new InetSocketAddress("localhost", 8080));

        ch = cf.awaitUninterruptibly().getChannel();
    }
    
    public static void main(String args[]) throws IOException{
        BasicSobiTrader b= new BasicSobiTrader(10, 100, 1);
        System.out.println("Sobi trader up");
    }
}
