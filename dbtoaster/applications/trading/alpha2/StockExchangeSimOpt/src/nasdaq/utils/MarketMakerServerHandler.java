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
 * Handles connections of clients to the market maker who want to trade in the market through this market maker. 
 * @author kunal
 */
public class MarketMakerServerHandler extends SimpleChannelHandler {

    TupleDecoder parser;
    List<String> schema;
    MarketMakerPropts marketPropts;
    int order_id;

    public MarketMakerServerHandler(MarketMakerPropts marketMakerPropts, TupleDecoder t) throws IOException {
        parser = t;
        schema = OrderBook.getSchemaKeys();
        this.marketPropts = marketMakerPropts;
        order_id = 0;
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
                    String msg = String.format("%s;stock_id:%s price:%s volume:%s order_id:%s timestamp:%s trader:%s\n",
                            contents[0],
                            newEntry.stockId,
                            newEntry.price,
                            newEntry.volume,
                            newEntry.order_id,
                            newEntry.timestamp,
                            newEntry.traderId);

                    if (newEntry.stockId != marketPropts.marketMakerStock || newEntry.volume > marketPropts.maxVolTradeable) {
                        continue;
                    }
                    //New order received, supporting only buy and sell commands at match maker right now
                    String action = contents[0];


                    //Confirm the trade 
                    if (action.equals(OrderBook.ASKCOMMANDSYMBOL)) {

                        this.marketPropts.portfolioValue -= this.marketPropts.askQuote * newEntry.volume;
                        this.marketPropts.stockHeld += newEntry.volume;
                    } else {
                        this.marketPropts.portfolioValue += this.marketPropts.bidQuote * newEntry.volume;
                        this.marketPropts.stockHeld -= newEntry.volume;
                    }
                    e.getChannel().write("Confirmed\n");

                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
}
