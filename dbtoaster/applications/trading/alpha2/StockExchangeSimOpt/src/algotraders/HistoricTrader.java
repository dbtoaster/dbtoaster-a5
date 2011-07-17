/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

/**
 * This was class was designed to read lines from a file and recreate 
 * a historic stream by synchronizing the timestamps in the file with the 
 * timestamps in current execution. The final originally provided was 
 * cleanedData4.csv, but it has been improved(with added field per line),
 * in history.csv. If a modified scaling historic trader has to be run (one 
 * which scales the trade it is sending w.r.t. price difference between history 
 * and current simulation) then all that needs to be done is:
 * 1) add another handler to the ChannelPipelineFactory (DefaultHandler will do fine)
 * 2) every time a new line is read, check the stock price in the simulation. Then
 *    do newPrice = f(oldPrice, oldMarketPrice, simMarketPrice), where the file will
 *    provide the oldMarketPrice.
 * 
 * @author kunal
 */
public class HistoricTrader {

    static BufferedReader reader = null;
    
    @Sharable
    public class ClientHandler
            extends SimpleChannelHandler {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            System.out.println("Market update: " + e.getMessage());
        }
    }

    /**
     * Only method. Create an instance and call run, and this instance will flood host market server 
     * with the historic data.
     * 
     * @throws IOException 
     */
    public void run() throws IOException {

        System.out.println("Enter test file name");
        String filename = "/home/kunal/cornell_db_maybms/cornell_db_maybms/dbtoaster/applications/trading/alpha2/StockExchangeSimOpt/cleanedData4.csv";
        reader = new BufferedReader(new FileReader(filename));

        //Establish connection
        ChannelFactory factory =
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new StringDecoder(), new StringEncoder());
            }
        });
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        //Hardcoding the host and port
        ChannelFuture cf = bootstrap.connect(new InetSocketAddress("localhost", 8080));

        Channel ch = cf.awaitUninterruptibly().getChannel();


        String order = null;
        if (reader == null) {
            return;
        }
        int order_id = 0;

        //Process the first entry to synchronise the historic data timestamp.
        order = reader.readLine();
        String fields[] = order.split(",");
        long startHistoricTimestamp = Long.parseLong(fields[0]);
        long startSimTimestamp = new Date().getTime();
        
        do {
            fields = order.split(",");
            int stock_id = 10101;
            double price = (Integer.valueOf(fields[4])) * 1.0 / 10000;
            int volume = (Integer.valueOf(fields[3]));
            String action = fields[2];
            int traderId = -1;
            long timestamp = Long.parseLong(fields[0]);
            if (action.equals("D")) {
                continue;
            }
            String message = String.format("%s;stock_id:%s price:%s volume:%s order_id:%s trader:%s\n", action, stock_id, price, volume, order_id++, traderId);
            //Synchronization loop. Uncomment to synchronize
            //while((new Date().getTime()-startSimTimestamp) < (timestamp - startHistoricTimestamp)){
            //}
            
            cf = ch.write(message);
            
            cf.awaitUninterruptibly();
        } while ((order = reader.readLine()) != null && order_id<5000);
        long endSimTimestamp = new Date().getTime();
        System.out.println("Sent at rate = "+ order_id*1000 / (endSimTimestamp-startSimTimestamp));
        System.out.println("Loop ended");
    }
    
    public static void main(String args[]) throws IOException{
        HistoricTrader ht = new HistoricTrader();
        ht.run();
    }
}
