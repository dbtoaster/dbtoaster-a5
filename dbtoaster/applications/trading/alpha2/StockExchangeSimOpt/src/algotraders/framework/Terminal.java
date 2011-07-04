/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.framework;

import java.io.IOException;
import rules.impl.BasicMatcher;
import state.OrderBook;
import state.StockState;

/**
 *
 * Class to create singleton objects for a particular trader. 
 * @author kunal
 */
public class Terminal{
    public StockState stockState;
    public OrderBook orderBook;
    public BasicMatcher matchmaker;
    
    public Terminal() throws IOException{
        //Initialise the stock market
        this.stockState = new StockState();
        this.stockState.init();
        
        //Create the OrderBook Object
        this.orderBook = new OrderBook();
        
        //Create the rules object
        this.matchmaker = new BasicMatcher(orderBook, stockState);
    }
}
