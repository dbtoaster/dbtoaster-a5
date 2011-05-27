/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Date;
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

/**
 *
 * @author kunal
 */
public class HistoricTrader {

    static BufferedReader reader = null;

    public static void main(String args[]) throws IOException {

        System.out.println("Enter test file name");
        BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
        String filename = b.readLine();

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
            /*while((new Date().getTime()-startSimTimestamp) < (timestamp - startHistoricTimestamp)){
                System.out.println("This is order no. "+order_id+" waiting for "+ (new Date().getTime()-startSimTimestamp) +" to become "+ (timestamp - startHistoricTimestamp));
            }*/
            
            cf = ch.write(message);

            cf.awaitUninterruptibly();
        } while ((order = reader.readLine()) != null && order_id<5000);
        long endSimTimestamp = new Date().getTime();
        System.out.println("Sent at rate = "+ 5000*1000 / (endSimTimestamp-startSimTimestamp));
        System.out.println("Loop ended");
    }
}
