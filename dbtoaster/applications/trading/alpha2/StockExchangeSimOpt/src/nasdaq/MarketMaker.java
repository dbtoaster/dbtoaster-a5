/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nasdaq;

import codecs.TupleDecoder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.Executors;
import nasdaq.utils.MarketMakerClientHandler;
import nasdaq.utils.MarketMakerPropts;
import nasdaq.utils.MarketMakerServerHandler;
import nasdaq.utils.NasdaqTerminal;
import nasdaq.utils.WindowPropts;
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
import state.OrderBook;

/**
 *
 * @author kunal
 * Market maker parameters :
 * 1)Max volume tradeable
 * 2) bid quote
 * 3) ask quote
 * 4) 
 * 
 */
public class MarketMaker {

    MarketMakerPropts marketPropts;

    public MarketMaker(Double spread, Integer maxVolTradeable, Integer windowLength, Integer stock) throws IOException {
        marketPropts = new MarketMakerPropts();

        marketPropts.bidBaseSpread = this.marketPropts.askBaseSpread = spread / 2;
        this.marketPropts.curAskSpread=this.marketPropts.curBidSpread = spread/2;
        this.marketPropts.maxVolTradeable = maxVolTradeable;
        this.marketPropts.portfolioValue = 0.;
        this.marketPropts.stockHeld = 0;
        this.marketPropts.marketMakerStock = stock;
        this.marketPropts.windowLength = windowLength;

        this.init();
    }

    public class MarketMakerServerChannelFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(
                    new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                    new StringDecoder(), new StringEncoder(),
                    new MarketMakerServerHandler(marketPropts, new TupleDecoder(OrderBook.getSchema())));
        }
    }

    public class MarketMakerClientChannelFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(
                    new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                    new StringDecoder(), new StringEncoder(),
                    new MarketMakerClientHandler(marketPropts, new TupleDecoder(OrderBook.getSchema())));
        }
    }

    public void execute() throws IOException {
        long startTimeStamp = new Date().getTime() / this.marketPropts.windowLength;

        while (true) {
            Double stockPrice = this.marketPropts.stockState.getStockPrice(this.marketPropts.marketMakerStock);
            this.marketPropts.askQuote = stockPrice - this.marketPropts.curAskSpread;
            this.marketPropts.bidQuote = stockPrice + this.marketPropts.curBidSpread;
            System.out.println(String.format("Timestamp:%s bidquote:%s askquote:%s", startTimeStamp, this.marketPropts.bidQuote, this.marketPropts.askQuote));

            this.marketPropts.curWindowPropts = new WindowPropts(this.marketPropts.portfolioValue + this.marketPropts.stockHeld * this.marketPropts.stockState.getStockPrice(this.marketPropts.marketMakerStock));
            while ((new Date().getTime() / this.marketPropts.windowLength) == startTimeStamp) {
            }

            if (this.marketPropts.curWindowPropts.initPortfolioValue < (this.marketPropts.portfolioValue+ this.marketPropts.stockHeld*this.marketPropts.stockState.getStockPrice(this.marketPropts.marketMakerStock))+0.1) {
                //Made a profit. Restore bid ask spreads to normal
                this.marketPropts.curAskSpread = this.marketPropts.bidBaseSpread;
                this.marketPropts.curBidSpread = this.marketPropts.askBaseSpread;
            } else {
                //Increase spread uniformly for now
                this.marketPropts.curBidSpread += 0.1;
                this.marketPropts.curAskSpread += 0.1;
            }
            System.out.println("New portfolio value is : " + this.marketPropts.portfolioValue);
            startTimeStamp = new Date().getTime() / this.marketPropts.windowLength;
        }

    }

    private void init() throws IOException {

        //Initialise local variables
        NasdaqTerminal nt = new NasdaqTerminal();
        this.marketPropts.stockState = nt.stockState;
        this.marketPropts.matchMaker = nt.matchmaker;
        this.marketPropts.simOrderBook = nt.orderBook;

        //Create server
        ChannelFactory serverFactory =
                new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ServerBootstrap bootstrap = new ServerBootstrap(serverFactory);

        bootstrap.setPipelineFactory(new MarketMakerServerChannelFactory());
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.bind(new InetSocketAddress(8081));

        //Create client
        ChannelFactory clientFactory =
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ClientBootstrap bootstrap2 = new ClientBootstrap(clientFactory);
        bootstrap2.setPipelineFactory(new MarketMakerClientChannelFactory());

        bootstrap2.setOption("tcpNoDelay", true);
        bootstrap2.setOption("keepAlive", true);

        ChannelFuture cf = bootstrap2.connect(new InetSocketAddress("localhost", 8080));

        this.marketPropts.ch = cf.awaitUninterruptibly().getChannel();

        //call execute

    }

    public Channel getNasdaqChannel() {
        return this.marketPropts.ch;
    }
    
    public static void main(String args[]) throws IOException{
        MarketMaker mm = new MarketMaker(0.1, 1000, 1000, 10101);
        mm.execute();
    }
}
