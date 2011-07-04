/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nasdaq.utils;

/**
 *
 * Window based properties of the market that one may want to study or keep an eye on. Like the portfolio changes in a particular time window.
 * @author kunal
 */
public class WindowPropts {
    public Double initPortfolioValue;
    
    
    public WindowPropts(Double portfolioValue){
        this.initPortfolioValue = portfolioValue;
    }
}
