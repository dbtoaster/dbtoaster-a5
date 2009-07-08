package ExchangeServer;

import java.util.*;

public  class ComparatorOrderTuples implements Comparator<Order_tuple>{
	
	public int compare ( Order_tuple o1, Order_tuple o2 )   {  

	     return o1.compareTo (o2) ; 
	    }                                      
	   public boolean equals ( Order_tuple o )   {  
	     return this.equals(o);
	    }  

}
