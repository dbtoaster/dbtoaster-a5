/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.profitbasedtrader;

import algotraders.basicsobitrader.BasicSobiTrader;
import algotraders.framework.GeneralTrader;
import codecs.TupleDecoder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import state.OrderBook;

/**
 *
 * Everything in this class is almost same as {@link BasicSobiTrader} except this includes the concept of a portfolio and profit/loss.
 * @author kunal
 */
public class ProfitTrader extends GeneralTrader {
    Double theta, margin, portfolio;
    Integer volToTrade;


    public ProfitTrader(Double param, Integer volToTrade, Double margin, Double portfolio) throws IOException {
        generalInit();

        //Local variables init
        this.theta = param;
        this.margin = margin;
        this.volToTrade = volToTrade;
        this.portfolio = portfolio;
        this.init();

    }

    public ProfitTrader(List<Integer> watchList, Double param, Integer volToTrade, Double margin, Double portfolio) throws IOException {
        generalInit(watchList);

        //Local vairables init
        this.theta = param;
        this.margin = margin;
        this.volToTrade = volToTrade;
        this.portfolio = portfolio;
        this.init();

    }

    private class SobiTraderChannelPipelineFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(new DelimiterBasedFrameDecoder(4096, Delimiters.lineDelimiter()),
                    new StringDecoder(), new StringEncoder(),
                    new ProfitTraderHandler(simOrderBook, watchList, stockInfo,matchMaker, stockState,
                    new TupleDecoder(OrderBook.getSchema())));
        }
    }

    private void init() {
        for (Integer i : watchList.getList()) {
            ProfitTraderPropts newPropts = new ProfitTraderPropts(this.theta, this.volToTrade, this.margin, this.portfolio, this.simOrderBook);
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
        ProfitTrader  b= new ProfitTrader(10., 100, 1., 100000.);
        System.out.println("Profit trader up");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while(true){
            String order = br.readLine();
            b.ch.write(order+"\n");
        }
    }
}
