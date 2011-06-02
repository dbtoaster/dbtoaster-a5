/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.profitbasedtrader;

import algotraders.framework.GeneralStockPropts;
import algotraders.framework.GeneralTraderHandler;
import algotraders.framework.WatchList;
import codecs.TupleDecoder;
import java.io.IOException;
import java.util.Map;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import rules.Matcher;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;
import state.StockState;

/**
 *
 * @author kunal
 */
public class ProfitTraderHandler extends GeneralTraderHandler {

    //Variables local to implementation

    public ProfitTraderHandler(OrderBook simOrderBook, WatchList watchList, Map<Integer, GeneralStockPropts> stockInfo,
            Matcher matchMaker, StockState stockState, TupleDecoder t) throws IOException {

        //General initialisation
        generalInit(simOrderBook, watchList, stockInfo, matchMaker, stockState, t);

        //Local initialisations
    }

    @Override
    public void runSim(String payload, Channel ch) throws Exception {
        String[] contents = payload.split(";");

        //Parse the tuple
        Map<String, Object> decodedLoad = parser.createTuples(contents[1]);


        if (decodedLoad != null) {
            Object a[] = new Object[schema.size()];
            for (int i = 0; i < schema.size(); i++) {
                a[i] = decodedLoad.get(schema.get(i));
            }

            //Create a new order book entry
            OrderBookEntry newEntry = null;
            try {
                newEntry = orderBook.createEntry(a);
                handlerLog.write(String.format("Added order: stock:%s price:%s volume:%s order_id:%s timestamp:%s trader:%s\n",
                        newEntry.stockId,
                        newEntry.price,
                        newEntry.volume,
                        newEntry.order_id,
                        newEntry.timestamp,
                        (newEntry.traderId == -1) ? "historic" : newEntry.traderId));

                if (contents[0].equals("B_update")) {
                    ProfitTraderPropts stockPropts = (ProfitTraderPropts)stockInfo.get(newEntry.stockId);
                    stockPropts.updatePortfolio(-1* newEntry.price*newEntry.volume);
                    System.err.println("New portfolio value is : "+stockPropts.getPropt("portfolio"));
                    return;
                }
                if (contents[0].equals("S_update")) {
                    ProfitTraderPropts stockPropts = (ProfitTraderPropts)stockInfo.get(newEntry.stockId);
                    stockPropts.updatePortfolio( newEntry.price*newEntry.volume);
                    System.err.println("New portfolio value is : "+stockPropts.getPropt("portfolio"));
                    return;
                }


                orderBook.executeCommand(contents[0], newEntry);
            } catch (IOException ex) {
                handlerLog.write(("The order format had an error. Skipping\n"));
            }

            //Run matching algorithm
            matchMaker.match(contents[0], newEntry);

            ProfitTraderPropts oldPropts = (ProfitTraderPropts) stockInfo.get(newEntry.stockId);
            oldPropts.updatePrice(stockState.getStockPrice(newEntry.stockId));
            oldPropts.updatePending(orderBook);

            
        }
    }
}
