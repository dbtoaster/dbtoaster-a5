/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.utils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author kunal
 */
public class WatchList {
    List<Integer> stockList;
    
    public WatchList(){
        stockList = new ArrayList<Integer>();
    }
    
    public WatchList(List<Integer> a){
        this.stockList = new ArrayList<Integer>(a);
    }
    
    public void addStockToList(Integer newWatch){
        this.stockList.add(newWatch);
    }
    
    public boolean removeStockFromList(Integer toRemove){
        return this.stockList.remove((Object)toRemove);
    }
    
    public List<Integer> getList(){
        return stockList;
    }
    
    public static WatchList createDefaultList(){
        //get all stocks from data.
        List<Integer> allStocks = new ArrayList<Integer>();
        allStocks.add(10101);
        allStocks.add(10102);
        return new WatchList(allStocks);
    }
}
