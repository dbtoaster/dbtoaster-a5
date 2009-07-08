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

	public ExchangeThread(Socket socket,SynchronizedBooks book, boolean d) {
		super("ExchangeThread");
		orders_book=book;
		this.socket = socket;
		timer= new Date();
		DEBUG=d;
		System.out.println("Server: Clinet Connects, opening thread to handle.");
	}

	public void run() {  
		//Assumption that if the time stamp is there and not equal to 0 
		//then the message comes from the data file

		try {

			out = new DataOutputStream(socket.getOutputStream());
			in = new DataInputStream(socket.getInputStream());

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

					int order_id = nextTuple.order_id;
					String action= nextTuple.action;
					long time=nextTuple.time;
					
					LinkedList<Stream_tuple> messages;

					if (action.equals("B")){
						// Insert bids
						
						//retrieve price and volume;
						double price = nextTuple.price;
						double volume = nextTuple.volume;						

						Order_tuple pv = new Order_tuple(0,price, volume, 0);
						
						if (time!=0){
							pv.time=time;
							pv.id=order_id;
						}
						synchronized (orders_book){
							
							orders_book.setOrderId(order_id);
							messages=orders_book.add_Bid_order(pv);

						}
						synchronized(this.clientList)
						{

							nextTuple=messages.getFirst();
							nextTuple.time=timer.getTime();
							sendMessageBack(nextTuple);
							messages.removeFirst();

							do{
								nextTuple=messages.getFirst();
								nextTuple.time=timer.getTime();
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

						double price = nextTuple.price;
						double volume = nextTuple.volume;


						Order_tuple pv = new Order_tuple(0, price, volume, 0);
						
						if (time!=0){
							pv.time=time;
							pv.id=order_id;
						}
						
						synchronized (orders_book){
							
							orders_book.setOrderId(order_id);
							messages=orders_book.add_Ask_order(pv);
						}
						synchronized(this.clientList)
						{
							nextTuple=messages.getFirst();
							nextTuple.time=timer.getTime();
							sendMessageBack(nextTuple);
							messages.removeFirst();

							do{
								nextTuple=messages.getFirst();
								nextTuple.time=timer.getTime();
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
						

						synchronized(orders_book){
							
							orders_book.update_order(order_id, nextTuple.volume);
						}
						synchronized(this.clientList)
						{
							nextTuple.time=timer.getTime();
							for (ExchangeThread item : clientList)
							{
								if (item.getExchangeType())
									item.sendMessageBack(nextTuple);

							}
						}
					}
					else if (action.equals("F"))
					{
						synchronized(orders_book){
							nextTuple.time=timer.getTime();
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
						
						synchronized(orders_book){
							nextTuple.time=timer.getTime();
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



		t.time=(Integer.valueOf(input_tuple[0])).longValue();
		t.order_id=(Integer.valueOf(input_tuple[1])).intValue();
		t.action=input_tuple[2];
		t.volume=(Double.valueOf(input_tuple[3])).doubleValue();
		t.price=(Double.valueOf(input_tuple[4])).doubleValue();
		
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
			String msg = t.toString() + "\n";
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
			System.out.println("ServerThread: finished sending.");
		}
	}

	public void setClientList(List<ExchangeThread> clientList) {
		this.clientList = clientList;
	}

}
