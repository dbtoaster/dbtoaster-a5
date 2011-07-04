/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import rules.Matcher;
import rules.impl.BasicMatcher;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;
import state.StockPrice;
import state.StockState;

/**
 *
 * @author Kunal Shah
 */
public class ModDataFileGen {

    public static void main(String args[]) throws IOException {
        OrderBook orderBook = new OrderBook();
        StockState stockState = new StockState();
        stockState.init();
        Integer order_id=1;
        Matcher matcher = new BasicMatcher(orderBook, stockState);
        FileReader fr = new FileReader("cleanedData4.csv");
        BufferedReader br = new BufferedReader(fr);
        FileWriter fw = new FileWriter("history.csv");
        String fields[];
        String order;
        int stock_id = 10101;
        while ((order = br.readLine()) != null) {
            
            fields = order.split(",");
            
            double price = (Integer.valueOf(fields[4])) * 1.0 / 10000;
            int volume = (Integer.valueOf(fields[3]));
            String action = fields[2];
            int traderId = -1;
            long timestamp = Long.parseLong(fields[0]);
            String line = order+","+StockPrice.getStockPrice(stock_id);
            
            if(action.equals("D"))continue;
            fw.write(line+"\n");
            System.out.println(line);
            OrderBookEntry entry = orderBook.createEntry(stock_id, price, volume, order_id, timestamp, traderId);
            orderBook.executeCommand(action, entry);
            matcher.match(action, entry);
        }
        fw.close();
        br.close();
        fr.close();
    }
}
