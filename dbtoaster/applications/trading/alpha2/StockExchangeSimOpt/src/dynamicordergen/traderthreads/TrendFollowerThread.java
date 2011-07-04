/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dynamicordergen.traderthreads;

import dynamicordergen.utils.OrderGenClient;
import java.util.Date;
import java.util.Random;
import state.OrderBook;
import state.StockPrice;
import state.StockState;

/**
 *
 * @author kunal
 * 
 * @param:
 * 1) stockId: the stock this thread is following.
 * 2) rate: max number of trades per market "tick" this thread will generate.
 * 3) factor: the factor by which rate will increase/decrease per trend consistent "tick"(note that this will be the arithmetico-geometric factor)
 * 4) volume: the maximum volume per order this thread will trade.
 * 
 * Note: Assuming trend followers place market orders for now.
 */
public class TrendFollowerThread extends Thread {

    Integer stockId;
    Integer baseRate;
    Integer maxRate;
    Integer factor;
    Integer volume;
    Integer currentRate;
    Integer trendLength;
    String currentTrend;
    Integer orderId;
    OrderGenClient ogc;
    String orderFormat;
    Double currentTickPrice;

    public TrendFollowerThread(Integer stockId, Integer baseRate, Integer maxRate, Integer factor, Integer volume, OrderGenClient ogc) {
        this.stockId = stockId;
        this.baseRate = baseRate;
        this.maxRate = maxRate;
        this.factor = factor;
        this.volume = volume;
        currentTrend = OrderBook.ASKCOMMANDSYMBOL;
        this.ogc = ogc;
        orderFormat = OrderBook.ORDERFORMAT;
        currentTickPrice = 100.;
        orderId =0;
        currentRate = baseRate;
        trendLength =0;
    }

    private void orderGen() {
        //Delete all pending orders from this trader
        for (int i = 0; i < currentRate; i++) {

            ogc.write(String.format(orderFormat, OrderBook.DELETECOMMANDSYMBOL, stockId, 0, 0, orderId - i - 1, 0, this.hashCode()));
        }

        //Check if the trend has been followed.
        Double newPrice = StockPrice.getStockPrice(stockId);
        //System.out.println("----------Old trend:"+currentTrend+" Old Price:"+currentTickPrice+" New price: "+newPrice);
        if ((newPrice > currentTickPrice+0.01 && currentTrend.equals(OrderBook.BIDCOMMANDSYMBOL)) || (newPrice < currentTickPrice-0.01 && currentTrend.equals(OrderBook.ASKCOMMANDSYMBOL))) {
            //Trend has been followed.
            trendLength++;
            currentRate = currentRate + (trendLength * factor);
            if (currentRate > maxRate) {
                currentRate = maxRate;
            }
        } else {
            trendLength = 0;
            currentRate = baseRate;
            currentTrend = (currentTrend.equals(OrderBook.ASKCOMMANDSYMBOL)) ? OrderBook.BIDCOMMANDSYMBOL : OrderBook.ASKCOMMANDSYMBOL;
        }
        //System.out.println("Trend set to:"+currentTrend+"------------");

        for (int i = 0; i < currentRate; i++) {
            ogc.write(String.format(orderFormat, currentTrend, stockId, OrderBook.MARKETORDER, volume, orderId++, 0, this.hashCode()));
        }
        currentTickPrice = newPrice;
    }
    
    @Override
    public void run() {
        System.out.println("Started running");
        long timestamp;
        while (true) {
            timestamp = new Date().getTime();
            orderGen();
            while(new Date().getTime() < timestamp+StockState.tickLength){}
        }
    }
}
