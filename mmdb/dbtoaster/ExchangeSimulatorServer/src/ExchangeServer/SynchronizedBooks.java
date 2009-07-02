package ExchangeServer;


import java.util.*;

public class SynchronizedBooks {
	
	Map<Integer, Order_tuple> ask_orders;
	Map<Integer, Order_tuple> bid_orders;
	int order_id;
	
	public SynchronizedBooks(){
		order_id=0;
		ask_orders=new HashMap<Integer, Order_tuple>();
		bid_orders=new HashMap<Integer, Order_tuple>();
		
	}

	public Integer add_Ask_order(Order_tuple t){
		order_id++;
		System.out.println("order_id is: "+order_id);
		Integer t_order_id= new Integer(order_id);
		ask_orders.put(t_order_id, t);
		
		return t_order_id;
	}
	public boolean remove_Ask_order(Integer id){
		ask_orders.remove(id);		
		return true;
	}
	public Integer add_Bid_order(Order_tuple t){
		order_id++;
		Integer t_order_id= new Integer(order_id);
		bid_orders.put(t_order_id, t);
		
		return t_order_id;
	}
	public boolean remove_Bid_order(Integer id){
		bid_orders.remove(id);
		return true;
	}
	public boolean update_order(Integer id, Order_tuple t){
		//Changes current bid/ask by delta if volume goes below 0 
		//removes such tuple
		double delta = t.volume;
		if (ask_orders.containsKey(id)){
			Order_tuple tuple = ask_orders.get(id);
			tuple.volume=tuple.volume-delta;
			tuple.time=t.time;
			if (tuple.volume<=0){
				remove_Ask_order(id);
			}
		}else if (bid_orders.containsKey(id)){
			Order_tuple tuple = bid_orders.get(id);
			tuple.volume=tuple.volume-delta;
			tuple.time=t.time;
			if (tuple.volume<=0){
				remove_Bid_order(id);
			}
		}
		
		return true;
	}
	public boolean remove(Integer id){
		
		return remove_Ask_order(id) | remove_Bid_order(id);
	}

}
