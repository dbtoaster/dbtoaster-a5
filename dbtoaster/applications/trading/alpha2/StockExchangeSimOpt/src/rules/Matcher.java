/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package rules;

import java.util.List;
import state.OrderBook.OrderBookEntry;

/**
 *
 * General interface which needs to implemented by any matching rules object.
 * @author kunal
 */
public interface Matcher {
    public List<OrderBookEntry> match(String action, OrderBookEntry a);
}
