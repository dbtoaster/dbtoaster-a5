/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package state;

import algotraders.framework.WatchList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * A static location which stores the price and volume of each stock.
 * @author kunal
 */
public class StockPrice {
    static final Map<Integer, Double> priceMap;
    static final Map<Integer, Integer> volumeMap;
    static{
        priceMap = new HashMap<Integer, Double>();
        volumeMap = new HashMap<Integer, Integer>();
        List<Integer> stockList = WatchList.createDefaultList().getList();
        for (Integer i : stockList) {
            priceMap.put(i, 100.);
            volumeMap.put(i, 0);
        }
    }
    
    public static Double getStockPrice(Integer stockId){
        return priceMap.get(stockId);
    }
    
    public static void setStockPrice(Integer stockId, Double price){
        priceMap.put(stockId, price);
    }

    public static Integer getStockVolume(int stockId) {
        return volumeMap.get(stockId);
    }

    public static void setStockVolume(int stockId, Integer currentTradedVolume) {
        volumeMap.put(stockId, currentTradedVolume);
    }
}
