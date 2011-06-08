/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nasdaq.utils;

import codecs.TupleDecoder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;

/**
 *
 * @author kunal
 */
public class MarketMakerClientHandler extends SimpleChannelHandler {

    TupleDecoder parser;
    List<String> schema;
    MarketMakerPropts marketPropts;

    public MarketMakerClientHandler(MarketMakerPropts marketMakerPropts, TupleDecoder t) {
        parser = t;
        schema = OrderBook.getSchemaKeys();
        this.marketPropts = marketMakerPropts;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws IOException {
        String buffer = (String) e.getMessage();
        String[] payloads = buffer.split("\n");

        try {
            for (String payload : payloads) {
                //String retMsg = payload;
                String[] contents = payload.split(";");
                System.out.println("Order received: " + payload);
                Map<String, Object> decodedLoad = parser.createTuples(contents[1]);

                if (decodedLoad != null) {
                    Object a[] = new Object[schema.size()];
                    for (int i = 0; i < schema.size(); i++) {
                        a[i] = decodedLoad.get(schema.get(i));
                    }
                    //a[4] = e.getChannel().hashCode();
                    OrderBookEntry newEntry = marketPropts.simOrderBook.createEntry(a);
                    if (contents[0].equals("B_update")) {
                        marketPropts.portfolioValue = marketPropts.portfolioValue - (newEntry.volume * newEntry.price);
                        marketPropts.stockHeld += newEntry.volume;
                        return;
                    }
                    if (contents[0].equals("S_update")) {
                        marketPropts.portfolioValue = marketPropts.portfolioValue + (newEntry.volume * newEntry.price);
                        marketPropts.stockHeld -= newEntry.volume;
                        return;
                    }
                    marketPropts.simOrderBook.executeCommand(contents[0], newEntry);

                    marketPropts.matchMaker.match(contents[0], newEntry);

                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
