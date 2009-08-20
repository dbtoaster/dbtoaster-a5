package ExchangeServer;

import java.util.*;

public class SortedBook {
	
	TreeSet<Order_tuple> sorted_tuples;
	HashMap<Integer, Order_tuple> hash_tuples;
	
	public SortedBook (ComparatorOrderTuples comp){
		sorted_tuples=new TreeSet<Order_tuple>(comp);
		hash_tuples=new HashMap<Integer, Order_tuple>();
	}
	public void put(Integer key, Order_tuple value){
		sorted_tuples.add(value);
		hash_tuples.put(key, value);
	}
	public void remove(Integer key){
		Order_tuple value=hash_tuples.get(key);
		sorted_tuples.remove(value);
		hash_tuples.remove(key);
	}
	public boolean containsKey(Integer key){
		return hash_tuples.containsKey(key);
	}
	public Order_tuple get(Integer key){
		return hash_tuples.get(key);
	}
	public Order_tuple first(){
		return sorted_tuples.first();
	}
	public Order_tuple last(){
		return sorted_tuples.last();
	}
	public Order_tuple pop_front(){
		Order_tuple t;
		try{
			t=first();
		}catch(NoSuchElementException e){
			return null;
		}
		Integer id=new Integer(t.id);
		sorted_tuples.remove(t);
		hash_tuples.remove(id);
		
		return t;
		
	}
	public Order_tuple pop_back(){
		Order_tuple t;
		try{
			t=last();
		}catch(NoSuchElementException e){
			return null;
		}
		Integer id=new Integer(t.id);
		sorted_tuples.remove(t);
		hash_tuples.remove(id);
		
		return t;
	}
	public boolean isEmpty(){
		return sorted_tuples.isEmpty() && hash_tuples.isEmpty();
	}
}
