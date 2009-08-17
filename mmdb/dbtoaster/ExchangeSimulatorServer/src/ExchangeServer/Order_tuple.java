package ExchangeServer;

public class Order_tuple implements Comparable<Order_tuple> {
	
	//structure for storing price volume and time of the bid
	
	int price;
	int volume;
	long time;
	int id;
	int c_id; //id of a company which placed an order
	boolean live;

	
	public Order_tuple(){
		price=0;
		volume=0;
		time=0;
		id=0;
		c_id=0;
		live=false;

	}
	public Order_tuple(long t, int p, int v, int comp_id, int order_id, boolean l){
		price=p;
		volume=v;
		time=t;
		id=order_id;
		c_id=comp_id;
		live=l;
	}
	public boolean equals(Object o) {
        if (!(o instanceof Order_tuple))
            return false;
        Order_tuple n = (Order_tuple)o;
        return n.id==id &&
        	   n.c_id==c_id &&
        	   n.live == live &&
        	   n.price==price &&
               n.volume==volume &&
               n.time==time;
    }


    public String toString() {
    	return time+" "+id+" "+c_id+" "+volume+" "+price;
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
    	}else if (c_id<n.c_id){
    		return -1;
    	}else if (c_id>n.c_id){
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
