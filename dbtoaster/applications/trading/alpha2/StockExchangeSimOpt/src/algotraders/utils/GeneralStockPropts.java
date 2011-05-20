/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.utils;

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

    public GeneralStockPropts(){
        this.stockPropts = new HashMap<String, Object>();
        generalProptsInit();
    }
    
    public GeneralStockPropts(Map<String, Object> map){
        this.stockPropts = new HashMap<String, Object>(map);;
    }
    
    public void generalProptsInit(){
        //General parameters for every stock, which must be maintained for every algorithm
        addPropt("pendingBids", new HashMap<Integer, Integer>());
        addPropt("pendingAsks", new HashMap<Integer, Integer>());
        addPropt("price", new Integer(0));
        addPropt("totBidVolume", new Integer(0));
        addPropt("totAskVolume", new Integer(0));
    }
    
    public void updateGeneralBids(List<OrderBookEntry> bidOrderBook) {
        Map<Integer, Integer> bidPropts = new HashMap<Integer, Integer>();
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

    public void updateGeneralAsks(List<OrderBookEntry> askOrderBook) {
        Map<Integer, Integer> askPropts = new HashMap<Integer, Integer>();
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
    
    public void addPropt(String propt, Object target){
        this.stockPropts.put(propt, target);
    }
    
    public void removePropt(String propt){
        this.stockPropts.remove(propt);
    }
    
    public Object getPropt(String propt){
        return this.stockPropts.get(propt);
    }
    
    public void setPropt(String propt, Object target){
        this.stockPropts.put(propt, target);
    }
    
    public void updatePending(OrderBook orderBook) {
        updateGeneralBids(orderBook.getBidOrderBook());
        updateGeneralAsks(orderBook.getAskOrderBook());
        updateSpecificBids();
        updateSpecificAsks();
    }

    public void updatePrice(Integer stockPrice) {
        setPropt("price", new Integer(stockPrice));
    }


    public abstract void updateSpecificBids();
    
    public abstract void updateSpecificAsks();
    
    public abstract String getTrade();
    
   
    
    
}
