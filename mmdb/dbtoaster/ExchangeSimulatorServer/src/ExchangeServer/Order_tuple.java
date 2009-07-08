package ExchangeServer;

public class Order_tuple implements Comparable<Order_tuple> {
	
	//structure for storing price volume and time of the bid
	
	double price;
	double volume;
	long time;
	int id;

	
	public Order_tuple(){
		price=0;
		volume=0;
		time=0;
		id=0;

	}
	public Order_tuple(long t, double p, double v, int order_id){
		price=p;
		volume=v;
		time=t;
		id=order_id;
	}
	public boolean equals(Object o) {
        if (!(o instanceof Order_tuple))
            return false;
        Order_tuple n = (Order_tuple)o;
        return n.id==id &&
        	   n.price==price &&
               n.volume==volume &&
               n.time==time;
    }


    public String toString() {
    	return id+" "+time+" "+price+" "+volume;
    }

    public int compareTo(Order_tuple n) {
    	
    	if (price<n.price) {
    		return -1;
    	}else if (price>n.price){
    		return 1;
    	}else if (time<n.time){
    		return -1;
    	}else if (time>n.time){
    		return 1;
    	}else if (id<n.id){
    		return -1;
    	}else if (id>n.id){
    		return 1;
    	}else if (volume<n.volume){
    		return -1;
    	}else if (volume>n.volume){
    		return 1;
    	}else{
    		return 0;
    	}

    }

}
