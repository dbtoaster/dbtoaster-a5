/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package stockexchangesim;

import connections.OrderBook;
import java.io.IOException;
import rules.MatchRules;
import java.util.concurrent.Semaphore;


/**
 *
 * @author kunal
 */
public class Terminal {
    public StockState stockState;
    public OrderBook orderBook;
    public MatchRules matchmaker;
    public Semaphore dbLock;
    
    Terminal() throws IOException{
        //Initialise the stock market
        stockState = new StockState();
        stockState.init();
        
        //Create the OrderBook Object
        orderBook = new OrderBook();
        
        //Create the rules object
        matchmaker = new MatchRules(orderBook);
        
        //Create the Semaphore
        dbLock = new Semaphore(1);
    }
    
    
}
