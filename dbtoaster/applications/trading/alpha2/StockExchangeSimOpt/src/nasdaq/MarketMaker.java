/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nasdaq;

import algotraders.basicsobitrader.BasicSobiPropts;
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
import state.StockPrice;

/**
 *
 * Interface for market making. Edit generateCase and generateOrder to define marketMaker algorithm
 * @author kunal
 * Market maker parameters :
 * 1)Max volume tradeable
 * 2) bid quote
 * 3) ask quote
 * 4) 
 * 
 */
public class MarketMaker {

    final static Integer BULLISH = 1;
    final static Integer BEARISH = 2;
    MarketMakerPropts marketPropts;
    Integer trendLength;
    String currentTrend;
    Double currentPrice;
    
    Integer orderId = 0;

    public MarketMaker(Double spread, Integer maxVolTradeable, Integer windowLength, Integer stock) throws IOException {
        marketPropts = new MarketMakerPropts();

        marketPropts.bidBaseSpread = this.marketPropts.askBaseSpread = spread / 2;
        this.marketPropts.curAskSpread = this.marketPropts.curBidSpread = spread / 2;
        this.marketPropts.maxVolTradeable = maxVolTradeable;
        this.marketPropts.portfolioValue = 0.;
        this.marketPropts.stockHeld = 0;
        this.marketPropts.marketMakerStock = stock;
        this.marketPropts.windowLength = windowLength;

        this.init();
    }

    private Integer generateCase() {
        Double newPrice = StockPrice.getStockPrice(marketPropts.marketMakerStock);
        if ((currentTrend.equals(OrderBook.ASKCOMMANDSYMBOL) && newPrice < currentPrice) || (currentTrend.equals(OrderBook.BIDCOMMANDSYMBOL) && newPrice > currentPrice)) {
            trendLength++;
        } else {
            trendLength = 1;
            currentTrend = (currentTrend.equals(OrderBook.ASKCOMMANDSYMBOL)) ? OrderBook.BIDCOMMANDSYMBOL : OrderBook.ASKCOMMANDSYMBOL;
        }
        currentPrice = newPrice;
        Integer scenario = 0;
        if (trendLength > 1) {
            Double bidVolWt = (Double) marketPropts.stockPropts.getPropt("bidVolWeightAvg");
            Double askVolWt = (Double) marketPropts.stockPropts.getPropt("askVolWeightAvg");


            if (askVolWt + bidVolWt - (2 * currentPrice) > 0 && currentTrend.equals(OrderBook.BIDCOMMANDSYMBOL)) {
                scenario = BULLISH;
            } else if (askVolWt + bidVolWt - (2 * currentPrice) < 0 && currentTrend.equals(OrderBook.ASKCOMMANDSYMBOL)) {
                scenario = BEARISH;
            }
        }

        return scenario;
    }

    private Double generateOrder(Integer scenario) {
        String orderFormat = OrderBook.ORDERFORMAT;
        Double highestBid = (Double) marketPropts.stockPropts.getPropt("highestBid");
        Double lowestAsk = (Double) marketPropts.stockPropts.getPropt("lowestAsk");
        Double theta = (Double) marketPropts.stockPropts.getPropt("theta");
        Double vol = (Double) marketPropts.stockPropts.getPropt("orderVol");
        if (marketPropts.pendingOrder != null) {
            String deleteMessage = String.format(orderFormat, OrderBook.DELETECOMMANDSYMBOL, marketPropts.marketMakerStock, 0, 0, marketPropts.pendingOrder.order_id, 0, marketPropts.pendingOrder.traderId);
            marketPropts.ch.write(deleteMessage);
        }
        String order;
        Double price;
        if (scenario == 0) {
            marketPropts.pendingOrder = null;
            return StockPrice.getStockPrice(marketPropts.marketMakerStock);
        } else if (scenario == BULLISH) {
            if (lowestAsk == OrderBook.MARKETORDER) {
                lowestAsk = StockPrice.getStockPrice(marketPropts.marketMakerStock);
            }
            price = lowestAsk - theta;
            orderId++;
            order = String.format(orderFormat, OrderBook.ASKCOMMANDSYMBOL, marketPropts.marketMakerStock, lowestAsk - theta, vol, orderId, 0, this.marketPropts.hashCode());
            marketPropts.pendingOrder = marketPropts.simOrderBook.createEntry(marketPropts.marketMakerStock, lowestAsk - theta , vol.intValue() , orderId,  0, this.marketPropts.hashCode());
        } else {
            if (highestBid == OrderBook.MARKETORDER) {
                highestBid = StockPrice.getStockPrice(marketPropts.marketMakerStock);
            }
            price = highestBid + theta;
            orderId++;
            order = String.format(orderFormat, OrderBook.BIDCOMMANDSYMBOL, marketPropts.marketMakerStock, highestBid + theta, vol, orderId, 0, this.marketPropts.hashCode());
            marketPropts.pendingOrder = marketPropts.simOrderBook.createEntry(marketPropts.marketMakerStock, highestBid + theta, vol.intValue(), orderId, 0, this.marketPropts.hashCode());
        }
        marketPropts.ch.write(order);
        return price;

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
            //Generate some trade if the market maker wants to
            Integer scenario = generateCase();
            Double price = generateOrder(scenario);


            Double stockPrice = StockPrice.getStockPrice(this.marketPropts.marketMakerStock);
            this.marketPropts.askQuote = price - this.marketPropts.curAskSpread;
            this.marketPropts.bidQuote = price + this.marketPropts.curBidSpread;
            System.out.println(String.format("Timestamp:%s bidquote:%s askquote:%s", startTimeStamp, this.marketPropts.bidQuote, this.marketPropts.askQuote));

            this.marketPropts.curWindowPropts = new WindowPropts(this.marketPropts.portfolioValue + this.marketPropts.stockHeld * stockPrice);
            while ((new Date().getTime() / this.marketPropts.windowLength) == startTimeStamp) {
            }

            if (this.marketPropts.curWindowPropts.initPortfolioValue < (this.marketPropts.portfolioValue + this.marketPropts.stockHeld * stockPrice) + 0.1) {
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
        this.marketPropts.stockPropts = new BasicSobiPropts(0., 0, 0., this.marketPropts.simOrderBook);

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
    
    public static void main(String args[]){
        if(args.length!=4){
            System.out.println("Usage: java MarketMaker spread maxVolTradeable windowLength stock");
        }
        else{
            try{
                MarketMaker mm = new MarketMaker(Double.parseDouble(args[0]),
                        Integer.parseInt(args[1]),
                        Integer.parseInt(args[2]),
                        Integer.parseInt(args[3]));
            }
            catch(Exception e){
                System.out.println("Error with input: "+e.getMessage());
            }
        }
        
    }
}
