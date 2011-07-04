/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package handlers;

import algotraders.framework.GeneralStockPropts;
import codecs.TupleDecoder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import rules.Matcher;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;
import state.StockPrice;

/**
 * A default handler for a connection to the market. Handles all kinds of market updates. This handler ignores order confirmation updates.
 * 
 * @author Kunal Shah
 */
public class DefaultHandler extends SimpleChannelHandler {

    TupleDecoder parser;
    List<String> schema;
    OrderBook orderBook;
    Matcher matcher;
    GeneralStockPropts stockPropts;

    /**
     * 
     * Constructor
     * @param matcher The matching rules to be used.
     * @param parser The parser to be used,
     * @param orderBook The orderBook to work on.
     * @param stockPropts If any kind of property maintenance is to be done the pass a {@link GeneralStockPropt} instance to this, else pass null
     */
    public DefaultHandler(Matcher matcher, TupleDecoder parser, OrderBook orderBook, GeneralStockPropts stockPropts) {
        this.matcher = matcher;
        this.parser = parser;
        this.orderBook = orderBook;
        this.schema = OrderBook.getSchemaKeys();
        this.stockPropts = stockPropts;
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
                    OrderBookEntry newEntry = orderBook.createEntry(a);
                    if (contents[0].equals("B_update")) {

                        return;
                    }
                    if (contents[0].equals("S_update")) {

                        return;
                    }
                    orderBook.executeCommand(contents[0], newEntry);

                    matcher.match(contents[0], newEntry);
                    if (stockPropts != null) {
                        stockPropts.updatePrice(StockPrice.getStockPrice(newEntry.stockId));
                        stockPropts.updatePending();
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
