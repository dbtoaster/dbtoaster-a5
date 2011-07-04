/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.framework;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;

/**
 *
 * @author kunal
 */
public abstract class GeneralStockPropts {
    protected Map<String, Object> stockPropts;
    public OrderBook orderBook;
    /**
     * Constructor
     */
    public GeneralStockPropts(){
        this.stockPropts = new HashMap<String, Object>();
        generalProptsInit();
    }
    
    /**
     * Constructor
     */
    public GeneralStockPropts(Map<String, Object> map){
        this.stockPropts = new HashMap<String, Object>(map);;
    }
    
    /**
     * Method to initialize basic properties needed for every trader implementation
     */
    public void generalProptsInit(){
        //General parameters for every stock, which must be maintained for every algorithm
        addPropt("pendingBids", new HashMap<Double, Integer>());
        addPropt("pendingAsks", new HashMap<Double, Integer>());
        addPropt("price", new Double(0));
        addPropt("totBidVolume", new Integer(0));
        addPropt("totAskVolume", new Integer(0));
    }
    
    /**
     * Method to update the general bid related properties.
     * 
     * @param bidOrderBook 
     */
    public void updateGeneralBids() {
        List<OrderBookEntry> bidOrderBook = orderBook.getBidOrderBook();
        Map<Double, Integer> bidPropts = new HashMap<Double, Integer>();
        Integer totBidVolume = 0;
        for (OrderBookEntry e : bidOrderBook) {
            totBidVolume += e.volume;
            if (bidPropts.get(e.price) == null) {
                bidPropts.put(e.price, e.volume);
            } else {
                bidPropts.put(e.price, e.volume + bidPropts.get(e.price));
            }
        }
        setPropt("pendingBids", bidPropts);
        setPropt("totBidVolume", totBidVolume);
    }

    /**
     * Method to update general ask related properties
     * 
     * @param askOrderBook 
     */
    public void updateGeneralAsks() {
        List<OrderBookEntry> askOrderBook = orderBook.getAskOrderBook();
        Map<Double, Integer> askPropts = new HashMap<Double, Integer>();
        Integer totAskVolume = 0;

        for (OrderBookEntry e : askOrderBook) {
            totAskVolume+=e.volume;
            if (askPropts.get(e.price) == null) {
                askPropts.put(e.price, e.volume);
            } else {
                askPropts.put(e.price, e.volume + askPropts.get(e.price));
            }
        }
        setPropt("pendingAsks", askPropts);
        setPropt("totAskVolume", totAskVolume);
    }
    
    /**
     * Method to add a new property
     * 
     * @param propt Name of property
     * @param target Property object
     */
    public void addPropt(String propt, Object target){
        this.stockPropts.put(propt, target);
    }
    
    /**
     * Method to remove a property
     * 
     * @param propt Name of property
     */
    public void removePropt(String propt){
        this.stockPropts.remove(propt);
    }
    
    /**
     * Accessor method for property
     * 
     * @param propt name of property
     * @return Object related to the property
     */
    public Object getPropt(String propt){
        return this.stockPropts.get(propt);
    }
    
    /**
     * Change the value of a property
     * 
     * @param propt Name
     * @param target Object for that property
     */
    public void setPropt(String propt, Object target){
        this.stockPropts.put(propt, target);
    }
    
    /**
     * Method to update all properties for given order book.
     * 
     * @param orderBook 
     */
    public void updatePending() {
        updateGeneralBids();
        updateGeneralAsks();
        updateSpecificBids();
        updateSpecificAsks();
    }

    /**
     * Updating the market price of stock. This property does not depend on order book hence separate method defined for this.
     * 
     * @param stockPrice 
     */
    public void updatePrice(Double stockPrice) {
        setPropt("price", new Double(stockPrice));
    }


    /**
     * Abstract method. Implement to obtain complete properties
     */
    public abstract void updateSpecificBids();
    
    /**
     * Abstract method. Implement to obtain complete properties
     */
    public abstract void updateSpecificAsks();
    
    /**
     * Abstract method. Implement to obtain complete properties
     */
    public abstract String getTrade();
    
   
    
    
}
