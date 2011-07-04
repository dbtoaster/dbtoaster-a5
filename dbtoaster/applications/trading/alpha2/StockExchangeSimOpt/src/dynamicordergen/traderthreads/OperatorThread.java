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
 * @param :
 * 1) stockId: The stock this thread is operating.
 * 2) rate : Max number of trades per milli-sec this thread will generate.
 * 3) volume: Max volume this operator will try to trade in one order.
 * 4) maxrange: the maximum market "ticks" up till which the operator will try to pull the market.
 * 5) minrange: the minimum market "ticks" up till which the operator will try to pull the market.
 * 
 * 
 */
public class OperatorThread extends Thread {

    Integer stockId;
    Integer rate;
    Integer volume;
    Integer maxrange;
    Integer minrange;
    Integer currentRange;
    String currentAction;
    String orderFormat;
    Integer orderId = 0;
    Random randomGen = new Random();
    OrderGenClient ogc;

    public OperatorThread(Integer stockId, Integer rate, Integer volume, Integer maxrange, Integer minRange, OrderGenClient ogc) {
        this.stockId = stockId;
        this.rate = rate;
        this.volume = volume;
        this.maxrange = maxrange;
        this.minrange = minRange;
        this.ogc = ogc;

        currentAction = (randomGen.nextInt(2) == 0) ? OrderBook.BIDCOMMANDSYMBOL : OrderBook.ASKCOMMANDSYMBOL;
        currentRange = this.minrange + randomGen.nextInt(this.maxrange - this.minrange);
        orderFormat = OrderBook.ORDERFORMAT;
    }

    private void orderGen() {
        //Delete all pending trades for previous orders
        for (int i = 0; i < rate; i++) {
            //System.out.println("Sending order:" + String.format(orderFormat, OrderBook.DELETECOMMANDSYMBOL, stockId, 0, 0, orderId - i, 0, this.hashCode()));

            ogc.write(String.format(orderFormat, OrderBook.DELETECOMMANDSYMBOL, stockId, 0, 0, orderId - i-1, 0, this.hashCode()));
        }

        //Generate one large market order first...
        Double price = StockPrice.getStockPrice(stockId);
        Double factor = (currentAction.equals(OrderBook.ASKCOMMANDSYMBOL)) ? -0.01 : 0.01;
        //System.out.println("Sending order:" + String.format(orderFormat, currentAction, stockId, OrderBook.MARKETORDER, volume, orderId++, 0, this.hashCode()));

        //Generate more trades for higher prices
        for (int i = 1; i <= rate; i++) {
            //System.out.println("Sending order:" + String.format(orderFormat, currentAction, stockId, price + (factor * i * price), volume, orderId++, 0, this.hashCode()));
            ogc.write(String.format(orderFormat, currentAction, stockId, price + (factor * i * price), volume, orderId++, 0, this.hashCode()));
        }

    }

    @Override
    public void run() {
        System.out.println("Started running");
        long timestamp;
        while (true) {
            while (currentRange > 0) {
                timestamp = new Date().getTime();
                orderGen();
                while (new Date().getTime() < timestamp + StockState.tickLength) {
                }

                currentRange--;
            }
            currentAction = (currentAction.equals(OrderBook.ASKCOMMANDSYMBOL)) ? OrderBook.BIDCOMMANDSYMBOL : OrderBook.ASKCOMMANDSYMBOL;
            currentRange = this.minrange + randomGen.nextInt(this.maxrange - this.minrange);
        }
    }
}
