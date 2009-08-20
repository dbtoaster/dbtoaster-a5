package ExchangeServer;

import java.io.*;
import java.net.Socket;
import java.util.*;



import ExchangeServer.Stream_tuple;

public class ExchangeThread extends Thread{

	private Socket socket = null;
	private SynchronizedBooks orders_book= null;
	private List<ExchangeThread> clientList;
	private DataOutputStream out;
	private DataInputStream in;
	private boolean isToaster;
	private Date timer;
	private boolean DEBUG;
	private HashMap<Integer, Integer> orderIDtoCompID;

	public ExchangeThread(Socket socket,SynchronizedBooks book, boolean d, HashMap<Integer, Integer> oIDtocID) {
		super("ExchangeThread");
		orders_book=book;
		this.socket = socket;
		timer= new Date();
		DEBUG=d;
		orderIDtoCompID=oIDtocID;
		System.out.println("Server: Clinet Connects, opening thread to handle.");
	}

	public void run() {  
		//Assumption that if the time stamp is there and not equal to 0 
		//then the message comes from the data file

		try {

			out = new DataOutputStream(socket.getOutputStream());
			in = new DataInputStream(socket.getInputStream());
			Random generator = new Random();

			if (DEBUG){
				System.out.println("Server: Opened socket: "+socket.toString());
			}

			try{

				Integer clientType = Integer.reverseBytes(in.readInt());
				if (DEBUG){
					System.out.println("Client type: " + clientType);
				}
				setExchangeType(!clientType.equals(0));

			}catch(IOException e){
				System.out.println("Server: Client failed to send its type correctly: "+e.getMessage());
				e.printStackTrace();
			}

			if (!isToaster){

				Stream_tuple nextTuple;

				while ((nextTuple = getInputTuple(in)) != null) {
					
					//Assuming that the order ID of a new tuple is 0
					// for any client but a data thread

					int order_id = nextTuple.order_id;
					String action= nextTuple.action;
					long time=nextTuple.time;
					int company_id=-1;
					boolean live=true;
					
					if (nextTuple.company_id < 1)
					{
						live=false;
						synchronized (orderIDtoCompID)
						{
							if (order_id > 0)
							{
								//check if there is a company ID for this order ...
								Integer comp;
								if ((comp=orderIDtoCompID.get(new Integer(order_id))) != null)
								{
									company_id=comp.intValue();
								}
							}
						}
					}
					else
					{
						live=true;
						company_id=nextTuple.company_id;
					}
					
					LinkedList<Stream_tuple> messages;

					if (action.equals("B")){
						// Insert bids
						
						//retrieve price and volume;
						int price = nextTuple.price;
						int volume = nextTuple.volume;	
						
						if (company_id < 0)
						{
							//if brokerage ID is unknown create new one
							company_id=generator.nextInt(8) + 1;
							synchronized (orderIDtoCompID)
							{
								orderIDtoCompID.put(new Integer(order_id), new Integer(company_id));
							}
						}

						Order_tuple pv = new Order_tuple(0,price, volume, company_id, 0, live);
						
//						if (time!=0){
							pv.time=time;
							pv.id=order_id;
							pv.c_id=company_id;
//						}
						synchronized (orders_book){
							
//							orders_book.setOrderId(order_id);
							messages=orders_book.add_Bid_order(pv);

						}
						synchronized(this.clientList)
						{

							nextTuple=messages.getFirst();
//							nextTuple.time=timer.getTime();
							sendMessageBack(nextTuple);
//							messages.removeFirst();

							do{
								nextTuple=messages.getFirst();
//								nextTuple.time=timer.getTime();
								messages.removeFirst();
								
								for (ExchangeThread item : clientList)
								{
									if (item.getExchangeType())
										item.sendMessageBack(nextTuple);
	
								}
							}while (!messages.isEmpty());

						}

					}
					else if (action.equals("S"))
					{
						//insert ask

						int price = nextTuple.price;
						int volume = nextTuple.volume;
						
						if (company_id < 0)
						{
							company_id=generator.nextInt(8) + 1;
							synchronized (orderIDtoCompID)
							{
								orderIDtoCompID.put(new Integer(order_id), new Integer(company_id));
							}
						}

						Order_tuple pv = new Order_tuple(0, price, volume, company_id, 0, live);
						
//						if (time!=0){
							pv.time=time;
							pv.id=order_id;
							pv.c_id=company_id;
//						}
						
						synchronized (orders_book){
							
//							orders_book.setOrderId(order_id);
							messages=orders_book.add_Ask_order(pv);
						}
						synchronized(this.clientList)
						{
							nextTuple=messages.getFirst();
//							nextTuple.time=timer.getTime();
							sendMessageBack(nextTuple);
//							messages.removeFirst();

							do{
								nextTuple=messages.getFirst();
//								nextTuple.time=timer.getTime();
								messages.removeFirst();
								
								for (ExchangeThread item : clientList)
								{
									if (item.getExchangeType())
										item.sendMessageBack(nextTuple);
	
								}
							}while (!messages.isEmpty());
						}
					}
					else if (action.equals("E")){
						
						//need to deal with accounting and pass the message to clients
						
						if (company_id > 0)
						{
							synchronized (orderIDtoCompID)
							{
								Integer temp_c_id;
								if ((temp_c_id = orderIDtoCompID.get(new Integer(order_id))) != null)
								{
									nextTuple.company_id=temp_c_id.intValue();
								}
							}
						}
						
						synchronized(orders_book){
							
							orders_book.update_order(nextTuple, nextTuple.volume, live);
						}
						synchronized(this.clientList)
						{
//							nextTuple.time=timer.getTime();
							for (ExchangeThread item : clientList)
							{
								if (item.getExchangeType())
									item.sendMessageBack(nextTuple);

							}
						}
					}
					else if (action.equals("F"))
					{
						if (company_id > 0)
						{
							synchronized (orderIDtoCompID)
							{
								Integer temp_c_id;
								if ((temp_c_id = orderIDtoCompID.get(new Integer(order_id))) != null)
								{
									nextTuple.company_id=temp_c_id.intValue();
									orderIDtoCompID.remove(new Integer(order_id));
								}
							}
						}
						
						synchronized(orders_book){
//							nextTuple.time=timer.getTime();
							orders_book.remove(new Integer(order_id));
						}
						synchronized(this.clientList)
						{
							for (ExchangeThread item : clientList)
							{
								if (item.getExchangeType())
									item.sendMessageBack(nextTuple);

							}
						}
					}
					else if (action.equals("D")){
						
						if (company_id > 0)
						{
							synchronized (orderIDtoCompID)
							{
								Integer temp_c_id;
								if ((temp_c_id = orderIDtoCompID.get(new Integer(order_id))) != null)
								{
									nextTuple.company_id=temp_c_id.intValue();
									orderIDtoCompID.remove(new Integer(order_id));
								}
							}
						}
						
						synchronized(orders_book){
//							nextTuple.time=timer.getTime();
							orders_book.remove(new Integer(order_id));
						}
						synchronized(this.clientList)
						{
							for (ExchangeThread item : clientList)
							{

								if (item.getExchangeType())
									item.sendMessageBack(nextTuple);

							}
						}
					}


				}

				//System.out.println("Closing connection!!!!");
				//   	    	out.close();
				//   	    	in.close();
				//   	    	socket.close();
			}else{
				//NEED a better way to deal with this 
				//TODO: toaster thread needs to hang around without doing anything with an open connection to the other world
				while (true)
					sleep(2);
			}


		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("Toaster thread doesn't sleep");
			e.printStackTrace();
		} 


	}

	private boolean setExchangeType(boolean b) {		
		isToaster=b;
		return isToaster;
	}
	public boolean getExchangeType(){
		return isToaster;
	}

	private Stream_tuple getInputTuple(DataInputStream in) {
		//the input of the stream is as follows:
		//Timestamp, order id, action, volume, price

		Stream_tuple t=new Stream_tuple();

		String [] input_tuple;
		if (DEBUG){
			System.out.println("ServerThread: reading an input tuple.");
		}
		byte[] tuple_in_bytes = new byte[512];
		byte current_byte;

		try{

			int i=0;
			if (DEBUG){
				System.out.println("is available");
			}
			while((current_byte=in.readByte())!='\n'){
				tuple_in_bytes[i]=current_byte;
				i++;
			}

			if (DEBUG){
				System.out.println("done reading: "+tuple_in_bytes);
			}
			input_tuple=((new String(tuple_in_bytes)).split(" "));


		}catch(IOException e){
			System.out.println("read bytes ... "+e.getMessage());
			return null;
		}


//		System.out.println(input_tuple[0]+"_"+input_tuple[1]+"_"+input_tuple[2]+"_"+input_tuple[3]+"_"+input_tuple[4]+"_"+input_tuple[5]);
		t.time=(Integer.valueOf(input_tuple[0])).longValue();
		t.order_id=(Integer.valueOf(input_tuple[1])).intValue();
		t.company_id=(Integer.valueOf(input_tuple[2])).intValue();
		t.action=input_tuple[3];
		t.volume=(Integer.valueOf(input_tuple[4])).intValue();
		
//		System.out.println(t.volume);
		t.price=Double.valueOf(input_tuple[5]).intValue();//(Integer.valueOf(input_tuple[5])).intValue();
//		String ts1=new String(input_tuple[5]);
//		Integer temp1=Integer.valueOf("10000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   ");//.valueOf(ts1);
//		t.price= temp1.intValue();// (Integer.valueOf(input_tuple[5])).intValue();
		
		if (DEBUG){
			System.out.println("ServerThread: got from client: "+t);
		}
		return t;
	}

	public void sendMessageBack(Stream_tuple t) throws IOException {
		//Sends output in the following order:
		//Timestamp, order id, action, volume, price
		if (DEBUG){
			System.out.println("ServerThread: sending to client "+t.toString());
		}	
		try{
			char tail=10;
			String msg = t.toString() + tail;
			byte[] b = msg.getBytes();
			
			if (DEBUG){
				System.out.println("Sending " + msg);
				System.out.println("Msg length: " + b.length);
			}
			
			out.write(b, 0, b.length);
		}catch(IOException e){
			System.out.println("exception "+e.getMessage());
		}
		if (DEBUG){
			System.out.println("number of matchings is "+orders_book.getNumMatchings());
			System.out.println("number of bought stocks "+orders_book.getNumBought());
			System.out.println("number of sold stocks "+orders_book.getNumSold());
			System.out.println("volume of B "+orders_book.getTotalB());
			System.out.println("volume of S "+orders_book.getTotalS());
			
			System.out.println("ServerThread: finished sending.");
		}
	}

	public void setClientList(List<ExchangeThread> clientList) {
		this.clientList = clientList;
	}

}
