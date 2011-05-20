/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.basicsobitrader;

import algotraders.utils.GeneralStockPropts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import state.OrderBook;
import state.OrderBook.OrderBookEntry;

/**
 *
 * @author kunal
 */
public class BasicSobiPropts extends GeneralStockPropts {

    public BasicSobiPropts(Integer theta, Integer volToTrade, Integer margin) {
        this.stockPropts = new HashMap<String, Object>();
        
        //General propts for every algorithm
        generalProptsInit();
        
        //Propts specific to current algorithm
        specificProptsInit(theta, volToTrade, margin);
    }
    
    private void specificProptsInit(Integer theta, Integer volToTrade, Integer margin){
        addPropt("bidVolWeightAvg", new Integer(0));
        addPropt("askVolWeightAvg", new Integer(0));
        addPropt("volHeld", new Integer(0));
        addPropt("theta", theta);
        addPropt("orderVol", volToTrade);
        addPropt("margin", margin);
    }
    
    @Override
    public void updateSpecificBids() {
        int volWt = 0;
        int vol = 0;
        int marketPrice = (Integer) getPropt("price");
        Set<Integer> prices = ((Map<Integer, Integer>) this.stockPropts.get("pendingBids")).keySet();
        for (Integer price : prices) {
            int newVol = ((Map<Integer, Integer>) this.stockPropts.get("pendingBids")).get(price);
            int newPrice = (price == OrderBook.MARKETORDER) ? marketPrice : price;
            volWt += newVol * newPrice;
            vol += newVol;
        }
        if (vol != 0) {
            setPropt("bidVolWeightAvg", volWt / vol);
        }
    }

    @Override
    public void updateSpecificAsks() {
        int volWt = 0;
        int vol = 0;
        Set<Integer> prices = ((Map<Integer, Integer>) this.stockPropts.get("pendingAsks")).keySet();
        for (Integer price : prices) {
            int newVol = ((Map<Integer, Integer>) this.stockPropts.get("pendingAsks")).get(price);
            volWt += newVol * price;
            vol += newVol;
        }
        if (vol != 0) {
            setPropt("askVolWeightAvg", volWt / vol);
        }
    }

    @Override
    public String getTrade() {
        int price = (Integer) getPropt("price");
        int bidVolWt = (Integer) getPropt("bidVolWeightAvg");
        int askVolWt = (Integer) getPropt("askVolWeightAvg");
        int bidVol = (Integer)getPropt("totBidVolume");
        int askVol= (Integer)getPropt("totAskVolume");
        
        int theta = (Integer)getPropt("theta");
        Integer volTraded=0;
        Integer volToTrade = (Integer)getPropt("orderVol");
        Integer margin = (Integer)getPropt("margin");
        String action=null;
        
        if(price==0 || bidVolWt==0 || askVolWt==0 || bidVol==0 || askVol==0){
            //Insufficient market data. Wait
            System.out.println("Insufficient Information to trade---");
        }else{
            
            int stat = (bidVolWt + askVolWt - (2*price));
            System.out.println("_____"+bidVolWt+" "+askVolWt+" "+price+" "+stat+" "+theta+" "+"_____");
            if(stat >= theta){
                //Market in bull phase. Try to get in at slightly lower costs.
                action = String.format("bid;price:%s volume:%s", price-margin, volToTrade);
            }
            else if(stat <= -theta){
                //Market in bear phase. Try to get out at slightly higher costs.
                action = String.format("ask;price:%s volume:%s", price+margin, volToTrade);
            }
            else{
                //No indication to buy or sell. Stay put
            }
        }
        
        return action;
    }


    public void updateVolHeld(Integer volTraded) {
        setPropt("volHeld", (Integer) getPropt("volHeld") + volTraded);
    }
}
