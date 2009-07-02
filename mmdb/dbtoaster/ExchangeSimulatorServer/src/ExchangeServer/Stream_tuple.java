package ExchangeServer;

public class Stream_tuple{
	// Timestamp, order id, action, volume, price
	public long time;
	public int order_id;
	public String action;
	public double volume;
	public double price;
	
	Stream_tuple(){
		action=new String("");
		time=0;
		order_id=0;
		volume=0;
		price=0;
	}
	public String toString(){
		return time+" "+order_id+" "+action+" "+volume+" "+price;
	}
		
}
