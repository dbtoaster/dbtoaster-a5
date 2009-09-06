package ExchangeServer;

/**
 * This is a class stores information to be sent to clients
 * easy accessible and changeable
 * @author antonmorozov
 *
 */
public class Stream_tuple{
	// Timestamp, order id, action, volume, price
	public long time;
	public int order_id;
	public String action;
	public int volume;
	public int price;
	public int company_id;
	public boolean live;
	
	Stream_tuple(){
		action=new String("");
		time=0;
		order_id=0;
		volume=0;
		price=0;
		company_id=0;
		live=false;
	}
	public String toString(){
		return time+" "+order_id+" "+company_id+" "+action+" "+volume+" "+price;
	}
		
}
