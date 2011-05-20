/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * This class stores the accessor functions for database connections
 * @author kunal
 */
public class OrderBook {

    public static boolean delete(List<OrderBookEntry> orderBook, OrderBookEntry match) {
        return orderBook.remove(match);
    }

    public static boolean update(List<OrderBookEntry> orderBook, OrderBookEntry a, OrderBookEntry i) {
        boolean removeStatus = orderBook.remove(a);
        orderBook.add(i);
        return removeStatus;
    }
    
    public static final int MARKETORDER = Integer.MAX_VALUE / 2;

    public class OrderBookEntry {

        public int stockId;
        public int price;
        public int volume;
        public long timestamp;
        public int traderId;
        

        public OrderBookEntry(int stockId, int price, int volume, long timestamp, int traderId) {
            this.price = price;
            this.stockId = stockId;
            this.timestamp = timestamp;
            this.traderId = traderId;
            this.volume = volume;
            
        }
        
        @Override
        public boolean equals(Object a){
            OrderBookEntry e = (OrderBookEntry)a;
            if(e.price==this.price &&
                    e.stockId==this.stockId &&
                    e.timestamp==this.timestamp &&
                    e.traderId==this.traderId &&
                    e.volume==this.volume)
                return true;
            else return false;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 67 * hash + this.stockId;
            hash = 67 * hash + this.price;
            hash = 67 * hash + this.volume;
            hash = (int) (67 * hash + this.timestamp);
            hash = 67 * hash + this.traderId;
            return hash;
        }
    }
    List<OrderBookEntry> bidOrderBook;
    int maxBid;
    List<OrderBookEntry> askOrderBook;
    int minAsk;
    static List<String> schemaKeys;
    static Map<String,String> schema;
    public OrderBook() {
        bidOrderBook = new ArrayList<OrderBookEntry>();
        maxBid=Integer.MIN_VALUE;
        askOrderBook = new ArrayList<OrderBookEntry>();
        minAsk=Integer.MAX_VALUE;
        
        schemaKeys = new ArrayList<String>();
        schemaKeys.add("stock_id");
        schemaKeys.add("price");
        schemaKeys.add("volume");
        schemaKeys.add("timestamp");
        schemaKeys.add("trader");
        
        schema = new HashMap<String, String>();
        schema.put("timestamp", "int");
        schema.put("price", "int");
        schema.put("volume", "int");
        schema.put("stock_id", "int");
        schema.put("trader", "int");
    }
    
    public static List<String> getSchemaKeys(){        
        return schemaKeys;
    }
    
    public static Map<String, String> getSchema(){
        return schema;
    }

    public OrderBookEntry createEntry(int stockId, int price, int volume, long timestamp, int traderId) {
        return new OrderBookEntry(stockId, price, volume, timestamp, traderId);
    }

    public void executeCommand(String action, OrderBookEntry args) throws IOException {
        OrderBookEntry newEntry = createEntry(args.stockId, args.price, args.volume, args.timestamp, args.traderId);
        args.equals(args);
        if (action.equals("bid")) {
            bidOrderBook.add(newEntry);
        }
        else if(action.equals("ask")){
            askOrderBook.add(newEntry);
        }
        else{
            throw new IOException("Invalid tuple passed for action");
        }
    }
    
    public List<OrderBookEntry> getBidOrderBook(){
        return bidOrderBook;
    }
    
    public List<OrderBookEntry> getAskOrderBook(){
        return askOrderBook;
    }
    
}
