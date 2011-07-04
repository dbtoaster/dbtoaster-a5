/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.basicsobitrader;

import algotraders.framework.GeneralTrader;
import codecs.TupleDecoder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import rules.impl.BasicMatcher;
import state.OrderBook;

/**
 *
 * Generates trades according to an algorithm.
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
public class BasicSobiTrader extends GeneralTrader{

    
    Double theta, margin;
    Integer volToTrade;
    
    /**
     * Constructor
     * 
     * @param param Parameter for deviation of volume weighted prices that trigger trade. |(bidvolwt+askvolwt-2*mp)|>param
     * @param volToTrade Vol to trade on trigger
     * @param margin Margin for limit order when trade is triggered.
     * @throws IOException 
     */
    public BasicSobiTrader(Double param, Integer volToTrade, Double margin) throws IOException {
        generalInit();
        
        //Local variables init
        this.theta = param;
        this.margin = margin;
        this.volToTrade = volToTrade;
        this.init();

    }

    /**
     * Constructor
     * 
     * @param watchList List of stocks this trader will trade in.
     * @param param Same as above
     * @param volToTrade
     * @param margin
     * @throws IOException 
     */
    public BasicSobiTrader(List<Integer> watchList, Double param, Integer volToTrade, Double margin) throws IOException {
        generalInit(watchList);
        
        //Local vairables init
        this.theta = param;
        this.margin = margin;
        this.volToTrade = volToTrade;
        this.init();

    }

    /**
     * Pipeline factory for connection to market.
     */
    private class SobiTraderChannelPipelineFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                    new StringDecoder(), new StringEncoder(),
                    new BasicSobiTraderHandler(simOrderBook, watchList, stockInfo,(BasicMatcher) matchMaker, stockState,
                    new TupleDecoder(OrderBook.getSchema())));
        }
    }
    
    /**
     * 
     * Initialization of local variables
     */
    private void init() {
        for (Integer i : watchList.getList()) {
            BasicSobiPropts newPropts = new BasicSobiPropts(this.theta, this.volToTrade, this.margin, this.simOrderBook);
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
        BasicSobiTrader b= new BasicSobiTrader(10., 100, 1.);
        System.out.println("Sobi trader up");
    }
}
