/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package rules;

import java.util.List;
import state.OrderBook.OrderBookEntry;

/**
 *
 * @author kunal
 */
public interface Matcher {
    public void match(String action, OrderBookEntry a);
}
