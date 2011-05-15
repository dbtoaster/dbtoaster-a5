/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package rules;

import connections.OrderBook;
import connections.OrderBook.OrderBookEntry;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author kunal
 */
public class MatchRules {

    OrderBook orderBook;
    List<Rule> bidMatchRules; //The rules to match a new bid
    List<Rule> askMatchRules; //The rules to match a new ask
    public final static Logger logger = Logger.getLogger("match_results");

    public class TimestampComparator implements Comparator {

        @Override
        public int compare(Object o1, Object o2) {
            long ts1 = ((OrderBookEntry) o1).timestamp;
            long ts2 = ((OrderBookEntry) o2).timestamp;

            if (ts1 < ts2) {
                return -1;
            } else if (ts1 > ts2) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    public MatchRules(OrderBook dbconn) throws IOException {

        orderBook = dbconn;
        bidMatchRules = new ArrayList<Rule>();
        askMatchRules = new ArrayList<Rule>();

        logger.setLevel(Level.ALL);
        FileHandler fh = new FileHandler("logfile.txt");
        logger.addHandler(fh);

    }

    public void addNewBidMatchRule(Rule r) {
        bidMatchRules.add(r);
    }

    public void addNewAskMatchRule(Rule r) {
        askMatchRules.add(r);
    }

    public void match(String action, OrderBookEntry a) {
        List<OrderBookEntry> targetOrderBook = (action.equals("bid")) ? orderBook.getAskOrderBook() : orderBook.getBidOrderBook();
        List<OrderBookEntry> tupleOrderBook = (action.equals("ask")) ? orderBook.getAskOrderBook() : orderBook.getBidOrderBook();
        logger.info(String.format("---Matching new entry: %s Stock: %s, Qty: %s, Price: %s, TimeStamp: %s", action,
                a.stockId, a.volume, (a.price == OrderBook.MARKETORDER) ? "marketorder" : a.price, a.timestamp));
        //Step 1: Get the highest entries in the order book to match
        List<OrderBookEntry> getTopMatches = getTopMatch(action, targetOrderBook, a.stockId, a.price);

        //Step 2: See if a match is possible in terms of price. If a better price is not available, get the market orders and equals price matches
        boolean matched = false;
        if (!getTopMatches.isEmpty()) {
            matched = true;
        }
        if (!matched) {
            getTopMatches = getMarketOrders(targetOrderBook,a.stockId, a.price);
            if (!getTopMatches.isEmpty()) {
                matched = true;
            }
        }
        //Step 3: If there is match complete a trade
        if (!matched) {
            logger.info("No match found---");
            return;
        }

        Collections.sort(getTopMatches, new TimestampComparator());
        OrderBookEntry match = getTopMatches.get(0);
        logger.info(String.format("Matched an entry: Stock: %s, Qty: %s, Price: %s, TimeStamp: %s---",
                match.stockId, match.volume, (match.price == OrderBook.MARKETORDER) ? "marketorder" : match.price, match.timestamp));
        boolean status = true;
        if (match.volume == a.volume) {
            status = status && OrderBook.delete(targetOrderBook, match);
            status = status && OrderBook.delete(tupleOrderBook, a);
        } else if (match.volume < a.volume) {
            status = status && OrderBook.delete(targetOrderBook, match);
            OrderBookEntry newEntry = orderBook.createEntry(a.stockId, a.price, a.volume-match.volume, a.timestamp, a.traderId);
            status = status && OrderBook.update(tupleOrderBook, a, newEntry);
            match(action, newEntry);
        } else {
            OrderBookEntry newEntry = orderBook.createEntry(match.stockId, match.price, match.volume-a.volume, match.timestamp, match.traderId);
            status = status && OrderBook.update(targetOrderBook, match, newEntry);
            status = status && OrderBook.delete(tupleOrderBook, a);
        }

        if (!status) {
            logger.warning("Deletion or updation from order book failed during matching");
        }
    }

    private List<OrderBookEntry> getTopMatch(String action, List<OrderBookEntry> targetOrderBook, int stockId, int price) {
        List<OrderBookEntry> matchTargets = new ArrayList<OrderBookEntry>();
        int limit;
        boolean betterMatchExists = false;
        if (action.equals("bid")) {
            limit = Integer.MAX_VALUE;
            for (OrderBookEntry e : targetOrderBook) {
                if (e.price < limit && e.stockId == stockId) {
                    limit = e.price;
                }
            }
            if(limit < price || price == OrderBook.MARKETORDER) betterMatchExists= true;
        } else {
            limit = Integer.MIN_VALUE;
            for (OrderBookEntry e : targetOrderBook) {
                if (e.price > limit && e.stockId == stockId) {
                    limit = e.price;
                }
            }
            if(limit>price || price==OrderBook.MARKETORDER)betterMatchExists = true;
        }
        if(betterMatchExists){
            for(OrderBookEntry e : targetOrderBook){
                if(e.price == limit) matchTargets.add(e);
            }
        }
        
        return matchTargets;
    }

    private List<OrderBookEntry> getMarketOrders(List<OrderBookEntry> targetOrderBook,int stockId, int price) {
        List<OrderBookEntry> toReturn = new ArrayList<OrderBookEntry>();
        for (OrderBookEntry e : targetOrderBook) {
            if (e.stockId == stockId && (e.price == OrderBook.MARKETORDER || e.price == price)) {
                toReturn.add(e);
            }
        }
        return toReturn;
    }
    /*
    private void generateRules(String action, int price, int volume, int stockId, int timestamp, int traderId) {
    
    //RULE 1;
    //TODO: See if there is a way to abstract or modularise this process
    String topViewName = (action.equals("bid")) ? topAskViewName : topBidViewName;
    String newEntryTable = (action.equals("bid")) ? bidTable : askTable;
    String matchTable = (action.equals("ask")) ? bidTable : askTable;
    String oppAction = (action.equals("ask")) ? "bid" : "ask";
    
    String query = "select * from " + topViewName + " a where stock_id=" + stockId + " and a.timestamp= (select min(timestamp) from " + topViewName + " where stock_id=" + stockId + ")";
    Statement stmt;
    try {
    stmt = orderBook.getConnection().createStatement();
    ResultSet rs = stmt.executeQuery(query);
    if (rs.next()) {
    int match_price = Integer.parseInt(rs.getString(oppAction + "_price"));
    int match_volume = Integer.parseInt(rs.getString(oppAction + "_volume"));
    
    boolean matched, recurse;
    if (action.equals("bid")) {
    matched = (match_price <= price);
    } else {
    matched = (match_price >= price);
    }
    
    if (matched) {
    String consequence1;
    String consequence2;
    if (match_volume == volume) {
    consequence1 = String.format("delete from %s where timestamp=%s and stock_id=%s and %s_price=%s and %s_volume=%s and trader=%s;\n",
    newEntryTable,
    timestamp,
    stockId,
    action, price,
    action, volume,
    traderId);
    consequence2 = String.format("delete from %s where timestamp=%s and stock_id=%s and %s_price=%s and %s_volume=%s and trader=%s;",
    matchTable,
    rs.getString("timestamp"),
    stockId,
    oppAction, match_price,
    oppAction, match_volume,
    rs.getString("trader"));
    //System.out.println(String.format("matched %s request of %s at %s equally with %s's %s of %s at %s", traderId, stockId, price, rs.getString("trader"), oppAction, stockId, match_price));
    logger.info(String.format("---Match generated; \n"
    + "User: %s, %s request: %s stocks of %s at %s;\n"
    + "MatchUser: %s, %s order: %s stocks of %s at %s\n"
    + "Equal volumes matched---\n", traderId, action, volume, stockId, price, rs.getString("trader"), oppAction, match_volume, stockId, match_price));
    
    recurse = false;
    } else if (match_volume < volume) {
    consequence1 = String.format("update %s set %s_volume=%s where timestamp=%s and stock_id=%s and %s_price=%s and %s_volume=%s and trader=%s;\n",
    newEntryTable,
    action, volume - match_volume,
    timestamp,
    stockId,
    action, price,
    action, volume,
    traderId);
    consequence2 = String.format("delete from %s where timestamp=%s and stock_id=%s and %s_price=%s and %s_volume=%s and trader=%s;",
    matchTable,
    rs.getString("timestamp"),
    stockId,
    oppAction, match_price,
    oppAction, match_volume,
    rs.getString("trader"));
    //System.out.println(String.format("matched %s request of %s stocks of %s at %s equally with %s's %s of %s stocks of %s at %s", traderId, volume, stockId, price, rs.getString("trader"), oppAction, match_volume, stockId, match_price));
    logger.info(String.format("---Match generated; \n"
    + "User: %s, %s request: %s stocks of %s at %s;\n"
    + "MatchUser: %s, %s order: %s stocks of %s at %s\n"
    + "Lower match volume---\n", traderId, action, volume, stockId, price, rs.getString("trader"), oppAction, match_volume, stockId, match_price));
    
    recurse = true;
    } else {
    consequence1 = String.format("delete from %s where timestamp=%s and stock_id=%s and %s_price=%s and %s_volume=%s and trader=%s;\n",
    newEntryTable,
    timestamp,
    stockId,
    action, price,
    action, volume,
    traderId);
    consequence2 = String.format("update %s set %s_volume=%s where timestamp=%s and stock_id=%s and %s_price=%s and %s_volume=%s and trader=%s;",
    matchTable,
    oppAction, match_volume - volume,
    rs.getString("timestamp"),
    stockId,
    oppAction, match_price,
    oppAction, match_volume,
    rs.getString("trader"));
    //System.out.println(String.format("matched %s request of %s stocks of %s at %s equally with %s's %s of %s stocks of %s at %s", traderId, volume, stockId, price, rs.getString("trader"), oppAction, match_volume, stockId, match_price));
    logger.info(String.format("---Match generated; \n"
    + "User: %s, %s request: %s stocks of %s at %s;\n"
    + "MatchUser: %s, %s order: %s stocks of %s at %s\n"
    + "Higher match volume---\n", traderId, action, volume, stockId, price, rs.getString("trader"), oppAction, match_volume, stockId, match_price));
    
    recurse = false;
    }
    Statement stmt2 = orderBook.getConnection().createStatement();
    //System.out.println("\n\n"+consequence1+"\n"+consequence2+"\n\n");
    stmt2.executeUpdate(consequence1);
    stmt2.executeUpdate(consequence2);
    stmt2.close();
    
    if (recurse) {
    generateRules(action, price, volume - match_volume, stockId, timestamp, traderId);
    }
    }
    
    }
    stmt.close();
    } catch (SQLException ex) {
    logger.severe(ex.getMessage());
    }
    
    
    }*/
}
