package ExchangeServer;


import java.util.*;

public class SynchronizedBooks {
	
	SortedBook ask_orders;
	SortedBook bid_orders;
	
	SortedBook live_ask_orders;
	SortedBook live_bid_orders;
	
	HashMap<Integer, Integer> old_ids;
	
	int order_id;
	long currentTime;
	boolean DEBUG;
	
	int numberOfMatchings;
	long numberOfBrought;
	long numberOfSold;
	long totalB;
	long totalS;
	
	boolean doSkips;
	
	public int setOrderId(int id){
		return order_id=id;
	}
	
	public SynchronizedBooks(boolean d, boolean skips){
		order_id=0;
		currentTime=0;
		numberOfMatchings=0;
		numberOfBrought=0;
		numberOfSold=0;
		totalB=0;
		totalS=0;
		DEBUG=d;
		doSkips=skips;
		ask_orders=new SortedBook(new ComparatorOrderTuples());
		bid_orders=new SortedBook(new ComparatorOrderTuples());
		live_ask_orders=new SortedBook(new ComparatorOrderTuples());
		live_bid_orders=new SortedBook(new ComparatorOrderTuples());
		
		old_ids=new HashMap<Integer, Integer>();
		
	}

	public LinkedList<Stream_tuple> add_Ask_order(Order_tuple t){
		
		if (t.id == 0)
		{
			order_id++;
			t.id=order_id;
			
//			currentTime++;
			t.time=currentTime;
		}
		else
		{
			Integer old_id;
			if ((old_id = old_ids.get(new Integer(t.id))) != null)
			{
				t.id=old_id.intValue();
			}
			else
			{
				old_ids.put(new Integer(t.id), ++order_id);
				t.id=order_id;
			}
			
			currentTime=t.time;
		}
		
		
		//TODO: check if order IDs in the datafile actually consistent.
		
		LinkedList<Stream_tuple> messages=new LinkedList<Stream_tuple>();
		
		Stream_tuple msg=new Stream_tuple();
		msg.time=t.time;
		msg.order_id=t.id;
		msg.action="S";
		msg.price=t.price;
		msg.volume=t.volume;
		msg.company_id=t.c_id;
		messages.addLast(msg);
		
		totalS+=t.volume;
		
		messages=match_ask(t, messages);
				
		return messages;
	}

	public LinkedList<Stream_tuple> add_Bid_order(Order_tuple t){
		
		if (t.id == 0)
		{
			order_id++;
			t.id=order_id;
			
//			currentTime++;
			t.time=currentTime;
		}
		else
		{
			Integer old_id;
			if ((old_id = old_ids.get(new Integer(t.id))) != null)
			{
				t.id=old_id.intValue();
			}
			else
			{
				old_ids.put(new Integer(t.id), ++order_id);
				t.id=order_id;
			}
			
			currentTime=t.time;
		}
		
		LinkedList<Stream_tuple> messages=new LinkedList<Stream_tuple>();
		
		Stream_tuple msg=new Stream_tuple();
		msg.time=t.time;
		msg.order_id=t.id;
		msg.action="B";
		msg.price=t.price;
		msg.volume=t.volume;
		msg.company_id=t.c_id;
		messages.addLast(msg);
		
		totalB+=t.volume;
		
		messages=match_bid(t, messages);
				
		return messages;
	}

	public boolean update_order(Stream_tuple tuple, int delta, boolean live){
		//The intention of this function to update in case of the exchange 
		// i.e. if we have an "E"
		Integer id=new Integer(tuple.order_id);
		
		Integer old_id;
		if ((old_id = old_ids.get(id)) != null)
		{
			id=old_id;
			tuple.order_id=id.intValue();
		}
		
		if ( !live || !doSkips){
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
		}
		else
		{
			if (live_ask_orders.containsKey(id)){
				Order_tuple t=live_ask_orders.get(id);
				
				live_ask_orders.remove(id);
				
				t.volume=t.volume-delta;
				if (t.volume>0){
					live_ask_orders.put(id, t);
				}
				return true;
				
			}else if (live_bid_orders.containsKey(id)){
				
				Order_tuple t=live_bid_orders.get(id);
				
				live_bid_orders.remove(id);
				
				t.volume=t.volume-delta;
				
				if(t.volume>0){
					live_bid_orders.put(id, t);
				}
				return true;
			}
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
		}if (live_ask_orders.containsKey(id)){
			live_ask_orders.remove(id);
			return true;
		}else if (live_bid_orders.containsKey(id)){
			live_bid_orders.remove(id);
			return true;
		}		
		return false;
	}
		
	private LinkedList<Stream_tuple> match_ask(Order_tuple t, LinkedList<Stream_tuple> message){
		//got a sell order and we want to match it with the best bid (buy order)
		Stream_tuple msg;
		Stream_tuple msg2;
		
		boolean doLive=t.live && doSkips;
		
		if (!doLive && bid_orders.isEmpty()){
/*
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="S";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
*/			
			ask_orders.put(new Integer(t.id), t);
			
			return message;
			
		}else if (doLive && live_bid_orders.isEmpty()){
		
			live_ask_orders.put(new Integer(t.id), t);
			
			return message;
			
		}
				
		Order_tuple best_bid;
		
		if (doLive)
		{
			best_bid=live_bid_orders.last();
		}
		else
		{
			best_bid=bid_orders.last();
		}
		
		if (t.price>best_bid.price){
			ask_orders.put(new Integer(t.id), t);
/*			
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="S";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
*/			
		}else if (t.volume<best_bid.volume){
						
			best_bid.volume=best_bid.volume-t.volume;
			
			numberOfMatchings++;
			numberOfSold+=t.volume;
			numberOfBrought+=(t.volume)*best_bid.price;
			
			msg=new Stream_tuple();
			msg.time=best_bid.time;
			msg.order_id=best_bid.id;
			msg.action="E";
			msg.price=best_bid.price;
			msg.volume=t.volume;
			msg.company_id=best_bid.c_id;
			
			msg2=new Stream_tuple();
			msg2.time=t.time;
			msg2.order_id=t.id;
			msg2.action="F";
			msg2.price=best_bid.price;
			msg2.volume=t.volume;
			msg2.company_id=t.c_id;

			message.addLast(msg);
			message.addLast(msg2);
			
			bid_orders.pop_back();
			bid_orders.put(new Integer(best_bid.id), best_bid);
			
		}else if (t.volume==best_bid.volume){
			//TODO: exec t in full change best bid, exec best bid in full 
			
			numberOfMatchings++;
			
			numberOfSold+=t.volume;
			numberOfBrought+=(t.volume)*best_bid.price;
			
			msg=new Stream_tuple();
			msg.time=best_bid.time;
			msg.order_id=best_bid.id;
			msg.action="F";
			msg.price=best_bid.price;
			msg.volume=t.volume;
			msg.company_id=best_bid.c_id;
			message.addLast(msg);
			
			msg2=new Stream_tuple();
			msg2.time=t.time;
			msg2.order_id=t.id;
			msg2.action="F";
			msg2.price=best_bid.price;
			msg2.volume=t.volume;
			msg2.company_id=t.c_id;
			message.addLast(msg2);
			
			bid_orders.pop_back();
			
		}else {
			//TODO: change best_bid, exec_best bid in full, change t, call match_ask
/*			
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="S";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
*/						
			numberOfMatchings++;
			
			t.volume=t.volume-best_bid.volume;
			
			numberOfSold+=best_bid.volume;
			numberOfBrought+=(best_bid.volume)*best_bid.price;
										
			msg=new Stream_tuple();
			msg.time=best_bid.time;
			msg.order_id=best_bid.id;
			msg.action="F";
			msg.price=best_bid.price;
			msg.volume=best_bid.volume;
			msg.company_id=best_bid.c_id;
			message.addLast(msg);
			
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="E";
			msg.price=best_bid.price;
			msg.volume=best_bid.volume;
			msg.company_id=t.c_id;
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
		Stream_tuple msg2;
		
		boolean doLive=t.live && doSkips;

		if (!doLive && ask_orders.isEmpty()){
/*
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="B";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
*/			
			bid_orders.put(new Integer(t.id), t);
			
			return message;
		}else if (doLive && live_ask_orders.isEmpty()){
			
			live_bid_orders.put(new Integer(t.id), t);
			
			return message;
		}
		
		Order_tuple best_ask;
		
		if (doLive)
		{
			best_ask=live_ask_orders.last();
		}
		else
		{
			best_ask=ask_orders.last();
		}
		
		
		if (t.price<best_ask.price){
			//there is no matching 
/*			
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="B";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
*/			
			bid_orders.put(new Integer(t.id), t);
			
		}else if (t.volume<best_ask.volume){
			//TODO: add bit executed in full message to the ask message, change the best_ask volume			
						
			numberOfMatchings++;
			numberOfSold+=t.volume;
			numberOfBrought+=(t.volume)*best_ask.price;
			
			msg=new Stream_tuple();
			msg.time=best_ask.time;
			msg.order_id=best_ask.id;
			msg.action="E";
			msg.price=best_ask.price;
			msg.volume=t.volume;
			msg.company_id=best_ask.c_id;
			message.addLast(msg);
			
			msg2=new Stream_tuple();
			msg2.time=t.time;
			msg2.order_id=t.id;
			msg2.action="F";
			msg2.price=best_ask.price;
			msg2.volume=t.volume;
			msg2.company_id=t.c_id;
			message.addLast(msg2);
			
			//TODO: need the code to handle consistency by adding this staff back as a new order or something.
			//changing the volume of the tuple
			best_ask.volume=best_ask.volume-t.volume;
			
			ask_orders.pop_front();
			ask_orders.put(new Integer(best_ask.id), best_ask);
			
			
		}else if (t.volume==best_ask.volume){
			//TODO: add bit executed in full message to the ask message, add execute in full message for the best ask
			
			numberOfMatchings++;
			
			numberOfSold+=t.volume;
			numberOfBrought+=(t.volume)*best_ask.price;
			
			msg=new Stream_tuple();
			msg.time=best_ask.time;
			msg.order_id=best_ask.id;
			msg.action="F";
			msg.price=best_ask.price;
			msg.volume=t.volume;
			msg.company_id=best_ask.c_id;
			message.addLast(msg);
			
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="F";
			msg.price=t.price;
			msg.volume=t.volume;
			msg.company_id=t.c_id;
			message.addLast(msg);
			
			ask_orders.pop_front();
			
			//TODO: need the code to handle consistency by adding this staff back as a new order or something.
			//changing the volume of the tuple
			//best_ask.volume=best_ask.volume-t.volume;
		}else{
/*						
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="B";
			msg.price=t.price;
			msg.volume=t.volume;
			message.addLast(msg);
*/			
			numberOfMatchings++;
			
			t.volume=t.volume-best_ask.volume;
			
			numberOfSold+=best_ask.volume;
			numberOfBrought+=(best_ask.volume)*best_ask.price;
										
			msg=new Stream_tuple();
			msg.time=best_ask.time;
			msg.order_id=best_ask.id;
			msg.action="F";
			msg.price=best_ask.price;
			msg.volume=best_ask.volume;
			msg.company_id=best_ask.c_id;
			message.addLast(msg);
			
			msg=new Stream_tuple();
			msg.time=t.time;
			msg.order_id=t.id;
			msg.action="E";
			msg.price=best_ask.price;
			msg.volume=best_ask.volume;
			msg.company_id=t.c_id;
			message.addLast(msg);
			
			ask_orders.pop_front();
			
			return match_bid(t, message);
						
		}
		return message;
	}
	
	int getNumMatchings()
	{
		return numberOfMatchings;
	}
	
	long getNumBought()
	{
		return numberOfBrought;
	}
	
	long getNumSold()
	{
		return numberOfSold;
	}
	
	long getTotalS()
	{
		return totalS;
	}
	
	long getTotalB()
	{
		return totalB;
	}

}
