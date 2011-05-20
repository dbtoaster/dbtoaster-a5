/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package stockexchangesim;

import state.StockState;
import state.OrderBook;
import java.io.IOException;
import rules.impl.BasicMatcher;
import java.util.concurrent.Semaphore;


/**
 *
 * @author kunal
 */
public class Terminal {
    public StockState stockState;
    public OrderBook orderBook;
    public BasicMatcher matchmaker;
    public Semaphore dbLock;
    public Semaphore sLock;
    
    public Terminal() throws IOException{
        //Initialise the stock market
        stockState = new StockState();
        stockState.init();
        
        //Create the OrderBook Object
        orderBook = new OrderBook();
        
        //Create the rules object
        matchmaker = new BasicMatcher(orderBook, stockState);
        
        //Create the Semaphores
        dbLock = new Semaphore(1);
        sLock = new Semaphore(1);
    }
    
    
}
