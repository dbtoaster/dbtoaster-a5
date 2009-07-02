package ExchangeServer;

public class Order_tuple {
	
	double price;
	double volume;
	long time;
	
	public Order_tuple(){
		price=0;
		volume=0;
		time=0;
	}
	public Order_tuple(long t, double p, double v){
		price=p;
		volume=v;
		time=t;
	}
	

}
