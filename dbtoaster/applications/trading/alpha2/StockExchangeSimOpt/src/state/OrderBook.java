/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * This class stores the accessor functions for database connections
 * 
 * Entries of the order book(NOTE: Sequence is very important):
 * ->int stock_id;
 * ->double price;
 * ->int volume;
 * ->int order_id;
 * ->long timestamp;
 * ->string trader;
 * 
 * 
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
    public static final String BIDCOMMANDSYMBOL = "B";
    public static final String ASKCOMMANDSYMBOL = "S";
    public static final String DELETECOMMANDSYMBOL = "D";
    public static final int HISTORICTRADERID = -1;

    public class OrderBookEntry {

        public int stockId;
        public double price;
        public int volume;
        public int order_id;
        public long timestamp;
        public int traderId;
        

        public OrderBookEntry(int stockId, double price, int volume, int order_id, long timestamp, int traderId) {
            this.price = price;
            this.stockId = stockId;
            this.timestamp = timestamp;
            this.traderId = traderId;
            this.volume = volume;
            this.order_id = order_id;
            
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
            hash = (int) (67 * hash + this.price);
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
        schemaKeys.add("order_id");
        schemaKeys.add("timestamp");
        schemaKeys.add("trader");
        
        schema = new HashMap<String, String>();
        schema.put("timestamp", "long");
        schema.put("price", "double");
        schema.put("volume", "int");
        schema.put("order_id", "int");
        schema.put("stock_id", "int");
        schema.put("trader", "int");
    }
    
    public static List<String> getSchemaKeys(){        
        return schemaKeys;
    }
    
    public static Map<String, String> getSchema(){
        return schema;
    }

    public OrderBookEntry createEntry(int stockId, double price, int volume, int order_id, long timestamp, int traderId) {
        return new OrderBookEntry(stockId, price, volume, order_id, timestamp, traderId);
    }
    
    public OrderBookEntry createEntry(Object[] a){
        int stock_id = (Integer)a[0];
        double price = a[1]==null?0.:(Double)a[1];
        int volume = (Integer)a[2];
        int order_id = (Integer)a[3];
        long timestamp = new Date().getTime();
        int traderId = (Integer)a[5];
        return createEntry(stock_id, price, volume, order_id, timestamp, traderId);
    }

    public void executeCommand(String action, OrderBookEntry args) throws IOException {
        OrderBookEntry newEntry = createEntry(args.stockId, args.price, args.volume, args.order_id, args.timestamp, args.traderId);
        args.equals(args);
        if (action.equals(OrderBook.BIDCOMMANDSYMBOL)) {
            bidOrderBook.add(newEntry);
        }
        else if(action.equals(OrderBook.ASKCOMMANDSYMBOL)){
            askOrderBook.add(newEntry);
        }
        else if(action.equals(OrderBook.DELETECOMMANDSYMBOL)){
            for(OrderBookEntry e : bidOrderBook){
                if(e.order_id==args.order_id && e.traderId==args.traderId){
                    OrderBook.delete(bidOrderBook, e);
                    return;
                }
            }
            for(OrderBookEntry e: askOrderBook){
                if(e.order_id==args.order_id && e.traderId==args.traderId){
                    OrderBook.delete(askOrderBook, e);
                }
            }
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
