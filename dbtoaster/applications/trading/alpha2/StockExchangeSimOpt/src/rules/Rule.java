/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package rules;

/**
 *
 * The format for a matching rule
 * @author kunal
 */
public class Rule {
    String query;
    String queryEqualVolume;
    String queryMatchTargetLowerVolume;
    String queryMatchTargetHigherVolume;
    
    public Rule(String rule, String onEqual, String onHigher, String onLower){
        this.query = rule;
        this.queryEqualVolume = onEqual;
        this.queryMatchTargetHigherVolume = onHigher;
        this.queryMatchTargetLowerVolume = onLower;
    }
    
    public String getQuery(){
        return query;
    }
    
    public String getAction(int volSource, int volTargetMatch){
        if(volSource==volTargetMatch){
            return getOnEqual();
        }
        else if(volSource < volTargetMatch){
            return getOnHigher();
        }
        else{
            return getOnHigher();
        }
    }
    
    String getOnEqual(){
        return queryEqualVolume;
    }
    String getOnHigher(){
        return queryMatchTargetHigherVolume;
    }
    String getOnLower(){
        return queryMatchTargetLowerVolume;
    }
    
}
