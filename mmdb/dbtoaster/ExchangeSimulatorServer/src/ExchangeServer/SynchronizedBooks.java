package ExchangeServer;


import java.util.*;

public class SynchronizedBooks {
	
	SortedBook ask_orders;
	SortedBook bid_orders;
	int order_id;
	boolean DEBUG;
	
	public int setOrderId(int id){
		return order_id=id;
	}
	
	public SynchronizedBooks(boolean d){
		order_id=0;
		DEBUG=d;
		ask_orders=new SortedBook(new ComparatorOrderTuples());
		bid_orders=new SortedBook(new ComparatorOrderTuples());
		
	}

	public LinkedList<Stream_tuple> add_Ask_order(Order_tuple t){
		order_id++;
		t.id=order_id;
		//TODO: check if order IDs in the datafile actually consistent.
		
		LinkedList<Stream_tuple> messages=new LinkedList<Stream_tuple>();
		
		Stream_tuple msg=new Stream_tuple();
		msg.time=t.time;
		msg.order_id=t.id;
		msg.action="S";
		msg.price=t.price;
		msg.volume=t.volume;
		messages.addLast(msg);
		
		messages=match_ask(t, messages);
				
		return messages;
	}

	public LinkedList<Stream_tuple> add_Bid_order(Order_tuple t){
		order_id++;
		t.id=order_id;
		
		LinkedList<Stream_tuple> messages=new LinkedList<Stream_tuple>();
		
		Stream_tuple msg=new Stream_tuple();
		msg.time=t.time;
		msg.order_id=t.id;
		msg.action="B";
		msg.price=t.price;
		msg.volume=t.volume;
		messages.addLast(msg);
		
		messages=match_bid(t, messages);
				
		return messages;
	}

	public boolean update_order(Integer id, double delta){
		//The intention of this function to update in case of the exchange 
		// i.e. if we have an "E"
		
		if (ask_orders.containsKey(id)){
			Order_tuple t=ask_orders.get(id);
			
			ask_orders.remove(id);
			
			t.volume=t.volume-delta;
			if (t.volume>0){
				ask_orders.put(id, t);
			}
			return true;
			
		}else if (bid_orders.containsKey(id)){
			
			Order_tuple t=bid_orders.get(id);
			
			bid_orders.remove(id);
			
			t.volume=t.volume-delta;
			
			if(t.volume>0){
				bid_orders.put(id, t);
			}
			return true;
		}
		
		return false;
	}
	
	public boolean remove(Integer id){
		
		if (ask_orders.containsKey(id)){
			ask_orders.remove(id);
			return true;
		}else if (bid_orders.containsKey(id)){
			bid_orders.remove(id);
			return true;
		}		
		return false;
	}
		
	private LinkedList<Stream_tuple> match_ask(Order_tuple t, LinkedList<Stream_tuple> message){
		//got a sell order and we want to match it with the best bid (buy order)
		Stream_tuple msg;
		
		if (bid_orders.isEmpty()){
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="S";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
			
			ask_orders.put(new Integer(t.id), t);
			
			return message;
			
		}
		
		Order_tuple best_bid=bid_orders.last();
		
		if (t.price>best_bid.price){
			ask_orders.put(new Integer(t.id), t);
			
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="S";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
			
		}else if (t.volume<best_bid.volume){
						
			best_bid.volume=best_bid.volume-t.volume;
			
			msg=new Stream_tuple();
			msg.time=best_bid.time;
			msg.order_id=best_bid.id;
			msg.action="E";
			msg.price=0;
			msg.volume=t.volume;

			message.addLast(msg);
			
			bid_orders.pop_back();
			bid_orders.put(new Integer(best_bid.id), best_bid);
			
		}else if (t.volume==best_bid.volume){
			//TODO: exec t in full change best bid, exec best bid in full 
			
			
			msg=new Stream_tuple();
			msg.time=best_bid.time;
			msg.order_id=best_bid.id;
			msg.action="E";
			msg.price=0;
			msg.volume=t.volume;
			message.addLast(msg);
			
			bid_orders.pop_back();
			
		}else {
			//TODO: change best_bid, exec_best bid in full, change t, call match_ask
						
			t.volume=t.volume-best_bid.volume;
										
			msg=new Stream_tuple();
			msg.time=best_bid.time;
			msg.order_id=best_bid.id;
			msg.action="E";
			msg.price=0;
			msg.volume=best_bid.volume;
			message.addLast(msg);
			
			bid_orders.pop_back();
			
			return match_ask(t, message);
		}
		
		
		return message;
	}
	private LinkedList<Stream_tuple> match_bid(Order_tuple t, LinkedList<Stream_tuple> message){
		//got a new buy request want to check if there is a matching sell request 
		
		//The price is a mean of the bid and ask prices
		//TODO: on a partial order we need to decrease the price. 
		
		Stream_tuple msg;

		if (ask_orders.isEmpty()){
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="B";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
			
			bid_orders.put(new Integer(t.id), t);
			
			return message;
		}
		
		Order_tuple best_ask=ask_orders.first();
		
		
		if (t.price<best_ask.price){
			//there is no matching 
			
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="B";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
			
			bid_orders.put(new Integer(t.id), t);
			
		}else if (t.volume<best_ask.volume){
			//TODO: add bit executed in full message to the ask message, change the best_ask volume			
						
			msg=new Stream_tuple();
			msg.time=best_ask.time;
			msg.order_id=best_ask.id;
			msg.action="E";
			msg.price=0;
			msg.volume=t.volume;
			message.addLast(msg);
			
			//TODO: need the code to handle consistency by adding this staff back as a new order or something.
			//changing the volume of the tuple
			best_ask.volume=best_ask.volume-t.volume;
			
			ask_orders.pop_front();
			ask_orders.put(new Integer(best_ask.id), best_ask);
			
			
		}else if (t.volume==best_ask.volume){
			//TODO: add bit executed in full message to the ask message, add execute in full message for the best ask
						
			msg=new Stream_tuple();
			msg.time=best_ask.time;
			msg.order_id=best_ask.id;
			msg.action="E";
			msg.price=0;
			msg.volume=t.volume;
			message.addLast(msg);
			
			ask_orders.pop_front();
			
			//TODO: need the code to handle consistency by adding this staff back as a new order or something.
			//changing the volume of the tuple
			//best_ask.volume=best_ask.volume-t.volume;
		}else{
										
			
			t.volume=t.volume-best_ask.volume;
										
			msg=new Stream_tuple();
			msg.time=best_ask.time;
			msg.order_id=best_ask.id;
			msg.action="E";
			msg.price=0;
			msg.volume=best_ask.volume;
			message.addLast(msg);
			
			ask_orders.pop_front();
			
			return match_bid(t, message);
						
		}
		return message;
	}

}
