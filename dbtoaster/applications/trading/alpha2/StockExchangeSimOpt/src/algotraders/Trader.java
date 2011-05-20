/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders;

import algotraders.utils.GeneralStockPropts;
import org.jboss.netty.channel.Channel;

/**
 *
 * @author kunal
 */
public interface Trader {
    
    void runSim(String payload, Channel ch);
    
    String generateTrade(Integer stock, GeneralStockPropts s);
}
