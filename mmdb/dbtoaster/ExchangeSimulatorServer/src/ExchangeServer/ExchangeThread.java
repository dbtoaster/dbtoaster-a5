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

	public ExchangeThread(Socket socket,SynchronizedBooks book) {
		super("ExchangeThread");
		orders_book=book;
		this.socket = socket;
		timer= new Date();
		System.out.println("Server: Clinet Connects, opening thread to handle.");
	}

	public void run() {   

		try {

			out = new DataOutputStream(socket.getOutputStream());
			in = new DataInputStream(socket.getInputStream());

			System.out.println("Server: Opened socket: "+socket.toString());

			/*
//    	    out.writeChars("hello");
    	    out.writeBytes("1234567890");
    	    System.out.println("printing out Byte");
			 */

			//   	    socket.close();
			//   	    System.out.println("closed connection blah");
			//   	    return;

			try{
				/*
   	    	byte[] test = new byte[128];
    	    	int read = in.read(test);
    	    	System.out.println("Read bytes: " + read);
    	    	for ( int i = 0; i < read; ++i)
    	    		System.out.println(test[i]);
				 */
				//Check to distinguish between a toaster and proxy
				//if reads 1 then it is a toaster.
				//   	    	Integer clientType = Integer.reverse(in.readInt());
				Integer clientType = Integer.reverseBytes(in.readInt());
				System.out.println("Client type: " + clientType);
				setExchangeType(!clientType.equals(0));

			}catch(IOException e){
				System.out.println("Server: Client failed to send its type correctly: "+e.getMessage());
				e.printStackTrace();
			}
			/*   	    socket.close();
    	    System.out.println("closed connection blah");
    	    return;
/*
    	    Object sentByClient;

    	    while ((sentByClient = in.readObject()) != null)
    	    {
    	    	if (sentByClient.getClass().equals(String.class))
    	    	{
    	    		String s;
    	    		synchronized (orders_book)
    	    		{
    	    			s = (String) sentByClient;
    	    			s = s.toLowerCase();
    	    		}
    	    		synchronized(this.clientList)
    	    		{
    	    			for (ExchangeThread item : clientList)
    	    			{
  //  	    				item.sendMessageBack(s);
						}
    	    		}
    	    	}
    	    	else
    	    	{
    	    		System.err.println("Invalid object type received by server\n");
    	    		System.exit(1);
    	    	}
    	    }
			 */    	    


			if (!isToaster){

				Stream_tuple nextTuple;

				while ((nextTuple = getInputTuple(in)) != null) {

					//   	    	while (in.available()>0){
					//   	    		nextTuple = getInputTuple(in);
					//   	    	System.out.println("looking at the returned tuple "+nextTuple.time);

					int order_id = nextTuple.order_id;
					String action= nextTuple.action;

					if (action.equals("B")){
						// Insert bids
						double price = nextTuple.price;
						double volume = nextTuple.volume;

						//					System.out.println("in B "+order_id+" "+action+" "+price+" "+volume);
						Order_tuple pv = new Order_tuple(0,price, volume);

						synchronized (orders_book){

							pv.time=timer.getTime();
							nextTuple.time=pv.time;
							nextTuple.order_id=(orders_book.add_Bid_order(pv)).intValue();

						}

						synchronized(this.clientList)
						{

							sendMessageBack(nextTuple);

							for (ExchangeThread item : clientList)
							{
								if (item.getExchangeType())
									item.sendMessageBack(nextTuple);

							}

						}

					}
					else if (action.equals("S"))
					{
						//insert ask

						double price = nextTuple.price;
						double volume = nextTuple.volume;


						Order_tuple pv = new Order_tuple(0, price, volume);

						synchronized (orders_book){
							pv.time=timer.getTime();
							nextTuple.time=pv.time;
							nextTuple.order_id=(orders_book.add_Ask_order(pv)).intValue();
						}
						synchronized(this.clientList)
						{
							sendMessageBack(nextTuple);
							for (ExchangeThread item : clientList)
							{
								if (item.getExchangeType())
									item.sendMessageBack(nextTuple);

							}
						}
					}
					else if (action.equals("E")){

						double delta = nextTuple.volume;

						Order_tuple pv = new Order_tuple(0, -1, delta);

						//price should we update it?

						synchronized(orders_book){
							pv.time=timer.getTime();
							nextTuple.time=pv.time;
							orders_book.update_order(new Integer(order_id), pv);
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
					else if (action.equals("F"))
					{
						synchronized(orders_book){
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

				System.out.println("Closing connection!!!!");
				//   	    	out.close();
				//   	    	in.close();
				//   	    	socket.close();
			}else{
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
		System.out.println("ServerThread: reading an input tuple.");
		byte[] tuple_in_bytes = new byte[512];
		byte current_byte;

		try{

			int i=0;
			//			System.out.println("about to read: "+tuple_in_bytes);
			//			if (in.available()>0){

			//			int num_bytes = in.read(tuple_in_bytes);
			System.out.println("is available");
			while((current_byte=in.readByte())!='\n'){
				//					System.out.println(current_byte);
				tuple_in_bytes[i]=current_byte;
				i++;
			}


			//			String str=new String(tuple_in_bytes);
			System.out.println("done reading: "+tuple_in_bytes);
			input_tuple=((new String(tuple_in_bytes)).split(" "));
			//			}else{
			//				return null;
			//			}
			/*			
//	    	int num_bytes = in.read(tuple_in_bytes);
	    	if (in.read(tuple_in_bytes)>0){
//		    	System.out.println("Read bytes: " + num_bytes);
	//	    	tuple=new String(test);
		    	input_tuple=((new String(tuple_in_bytes)).split(" "));
	    	}else{
	    		return null;
	    	}
			 */
			//	    	System.out.println("input length "+input_tuple.length);
			//	    	System.out.println("Read bytes: " + read);
			//	    	for ( int i = 0; i < input_tuple.length; i++)
			//	    		System.out.println(input_tuple[i]);

			//	    	System.out.println(input_tuple[0]);

		}catch(IOException e){
			System.out.println("read bytes ... "+e.getMessage());
			return null;
		}



		t.time=(Integer.valueOf(input_tuple[0])).longValue();
		t.order_id=(Integer.valueOf(input_tuple[1])).intValue();
		t.action=input_tuple[2];
		t.volume=(Double.valueOf(input_tuple[3])).doubleValue();
		t.price=(Double.valueOf(input_tuple[4])).doubleValue();

		//		System.out.println("tuple time "+t.time);
		//		return null;

		System.out.println("ServerThread: got from client: "+t);
		return t;
	}

	public void sendMessageBack(Stream_tuple t) throws IOException {
		//Sends output in the following order:
		//Timestamp, order id, action, volume, price
		System.out.println("ServerThread: sending to client "+t.toString());
		try{
			String msg = t.toString() + "\n";
			System.out.println("Sending " + msg);
			byte[] b = msg.getBytes();
			System.out.println("Msg length: " + b.length);
			out.write(b, 0, b.length);
		}catch(IOException e){
			System.out.println("exception "+e.getMessage());
		}
		System.out.println("ServerThread: finished sending.");
	}

	public void setClientList(List<ExchangeThread> clientList) {
		this.clientList = clientList;
	}

}
