/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package algotraders.basicsobitrader;

import algotraders.framework.GeneralStockPropts;
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

    public BasicSobiPropts(Double theta, Integer volToTrade, Double margin) {
        this.stockPropts = new HashMap<String, Object>();
        
        //General propts for every algorithm
        generalProptsInit();
        
        //Propts specific to current algorithm
        specificProptsInit(theta, volToTrade, margin);
    }
    
    private void specificProptsInit(Double theta, Integer volToTrade, Double margin){
        addPropt("bidVolWeightAvg", new Double(0));
        addPropt("askVolWeightAvg", new Double(0));
        addPropt("volHeld", new Integer(0));
        addPropt("theta", theta);
        addPropt("orderVol", volToTrade);
        addPropt("margin", margin);
    }
    
    @Override
    public void updateSpecificBids() {
        double volWt = 0;
        int vol = 0;
        double marketPrice = (Double) getPropt("price");
        Set<Double> prices = ((Map<Double, Integer>) this.stockPropts.get("pendingBids")).keySet();
        for (Double price : prices) {
            int newVol = ((Map<Double, Integer>) this.stockPropts.get("pendingBids")).get(price);
            double newPrice = (price == OrderBook.MARKETORDER) ? marketPrice : price;
            volWt += newVol * newPrice;
            vol += newVol;
        }
        if (vol != 0) {
            setPropt("bidVolWeightAvg", volWt*1.0 / vol);
        }
    }

    @Override
    public void updateSpecificAsks() {
        double volWt = 0;
        int vol = 0;
        Set<Double> prices = ((Map<Double, Integer>) this.stockPropts.get("pendingAsks")).keySet();
        for (Double price : prices) {
            int newVol = ((Map<Double, Integer>) this.stockPropts.get("pendingAsks")).get(price);
            volWt += newVol * price;
            vol += newVol;
        }
        if (vol != 0) {
            setPropt("askVolWeightAvg", volWt / vol);
        }
    }

    @Override
    public String getTrade() {
        double price = (Double) getPropt("price");
        double bidVolWt = (Double) getPropt("bidVolWeightAvg");
        double askVolWt = (Double) getPropt("askVolWeightAvg");
        int bidVol = (Integer)getPropt("totBidVolume");
        int askVol= (Integer)getPropt("totAskVolume");
        
        double theta = (Double)getPropt("theta");
        Integer volTraded=0;
        Integer volToTrade = (Integer)getPropt("orderVol");
        double margin = (Double)getPropt("margin");
        String action=null;
        
        if(price==0 || bidVolWt==0 || askVolWt==0 || bidVol==0 || askVol==0){
            //Insufficient market data. Wait
            //System.out.println("Insufficient Information to trade---");
        }else{
            
            double stat = (bidVolWt + askVolWt - (2*price));
            if(stat >= theta){
                //Market in bull phase. Try to get in at slightly lower costs.
                action = String.format("%s;price:%s volume:%s",OrderBook.BIDCOMMANDSYMBOL, price-margin, volToTrade);
            }
            else if(stat <= -theta){
                //Market in bear phase. Try to get out at slightly higher costs.
                action = String.format("%s;price:%s volume:%s",OrderBook.ASKCOMMANDSYMBOL, price+margin, volToTrade);
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
