/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package nasdaq.utils;

import org.jboss.netty.channel.SimpleChannelHandler;

/**
 *
 * Incomplete. Handler to decide how the broker handles inputs from the marketMaker. Till now there is no message pattern fixed for marketMaker output.
 * Hence this is left open. Its a simple matter to just design the messageReceived method to take input from the marketMaker channel, see if its offered quotes are better 
 * (using methods in {@link Broker} ). NO other functionality is required atm.
 * @author Kunal Shah
 */
public class BrokerClientChannelHandler extends SimpleChannelHandler{


    public BrokerClientChannelHandler(){

    }
}
