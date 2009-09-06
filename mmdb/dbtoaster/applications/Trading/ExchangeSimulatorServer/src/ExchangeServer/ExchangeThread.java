package ExchangeServer;

import java.io.*;
import java.net.Socket;
import java.util.*;

import ExchangeServer.Stream_tuple;

/*
 * A thread to handle request and communication with each client.
 */
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

	/**
	 * Constructor. 
	 * Takes in 
	 * @param Socket            socket the socket over which communicate to the client
	 * @param SynchronizedBooks book   order books
	 * @param  boolean          d      debugger mode 
	 */
	public ExchangeThread(Socket socket,SynchronizedBooks book, boolean d, HashMap<Integer, Integer> oIDtocID) {
		super("ExchangeThread");
		orders_book=book;
		this.socket = socket;
		timer= new Date();
		DEBUG=d;
		orderIDtoCompID=oIDtocID;
		System.out.println("Server: Clinet Connects, opening thread to handle.");
	}

	/**
	 * Handles the thread's execution.
	 */
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
				//get's the type of the client (reader/writer)
				Integer clientType = Integer.reverseBytes(in.readInt());
				if (DEBUG){
					System.out.println("Client type: " + clientType);
				}
				setExchangeType(!clientType.equals(0));

			}catch(IOException e){
				System.out.println("Server: Client failed to send its type correctly: "+e.getMessage());
				e.printStackTrace();
			}

			//if this is a writer
			if (!isToaster){

				Stream_tuple nextTuple;

				//while there is input from the client
				while ((nextTuple = getInputTuple(in)) != null) {
					
					//Assuming that the order ID of a new tuple is 0
					// for any client but a data thread

					int order_id = nextTuple.order_id;
					String action= nextTuple.action;
					long time=nextTuple.time;
					int company_id=-1;
					boolean live=true;
					
					//if order is coming from historical file
					if (nextTuple.company_id < 1)
					{
						live=false; //indicator of historical data
						synchronized (orderIDtoCompID)
						{
							if (order_id > 0)
							{
								//check if there is a company ID for this order
								//and if so set it up
								Integer comp;
								if ((comp=orderIDtoCompID.get(new Integer(order_id))) != null)
								{
									company_id=comp.intValue();
								}
								else
								{
									if (nextTuple.action.equals("S") || nextTuple.action.equals("B"))
									{
										//for this two order types there should not be any previously 
										//stored IDs
									}
									else
									{		
										//in other cases there should be a mention and this should not 
										//be executed. If this is executed the historical data file needs to 
										//be cleaned up.
										System.out.println("NOT found "+nextTuple);										
									}
									
								}
							}
						}
					}
					else
					{
						//this order came from an AlgoEngine
						live=true;
						company_id=nextTuple.company_id;
					}
					
					LinkedList<Stream_tuple> messages;

					//For each action take appropriate action
					if (action.equals("B")){
						// Insert bids					
						//retrieve price and volume;
						int price = nextTuple.price;
						int volume = nextTuple.volume;	
						
						if (company_id < 0)
						{
							//brokerage ID should be unknown for historic data
							//in this case create a random ID and insert it to 
							//orderIDtoCompID.
							company_id=generator.nextInt(8) + 1;
							synchronized (orderIDtoCompID)
							{
								orderIDtoCompID.put(new Integer(order_id), new Integer(company_id));
							}
						}

						//create an order tuple
						Order_tuple pv = new Order_tuple(0,price, volume, company_id, 0, live);
						
						pv.time=time;
						pv.id=order_id;
						pv.c_id=company_id;

						synchronized (orders_book){
							
							// insert order to the order book and check for matching
							messages=orders_book.add_Bid_order(pv);

						}
						synchronized(this.clientList)
						{

							//notify the client that the messages was added 
							//so that client can get order_id for their buy order
							nextTuple=messages.getFirst();
							sendMessageBack(nextTuple);

							//go through all messages and 
							//notify readers about the changes to the Order Book
							do{
								nextTuple=messages.getFirst();
								messages.removeFirst();
								
								for (ExchangeThread item : clientList)
								{
									if (item.getExchangeType())
										item.sendMessageBack(nextTuple);
	
								}
							}while (!messages.isEmpty());

						}

					}
					//for sell orders
					else if (action.equals("S"))
					{
						//insert ask(sell order)

						int price = nextTuple.price;
						int volume = nextTuple.volume;
						
						if (company_id < 0)
						{
							//create brokerage ID
							company_id=generator.nextInt(8) + 1;
							synchronized (orderIDtoCompID)
							{
								orderIDtoCompID.put(new Integer(order_id), new Integer(company_id));
							}
						}

						Order_tuple pv = new Order_tuple(0, price, volume, company_id, 0, live);
						
						pv.time=time;
						pv.id=order_id;
						pv.c_id=company_id;
						
						synchronized (orders_book){
							
						    // insert order to the order book and check for matching
							messages=orders_book.add_Ask_order(pv);
						}
						synchronized(this.clientList)
						{
							//send message to the client which requested the order
							nextTuple=messages.getFirst();
							sendMessageBack(nextTuple);

							//send change to all readers
							do{
								nextTuple=messages.getFirst();
								messages.removeFirst();
								
								for (ExchangeThread item : clientList)
								{
									if (item.getExchangeType())
										item.sendMessageBack(nextTuple);
	
								}
							}while (!messages.isEmpty());
						}
					}
					//change of the order book
					else if (action.equals("E")){
						//currently should not be present in historical data
						//
						//need to deal with accounting and pass message to client					
						if (company_id > 0)
						{
							synchronized (orderIDtoCompID)
							{
								//brokerage had to be there
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
							//when historic data will contain this information
							//this needs to be changes to pass the parameters to 
							//other Readers.
/*							for (ExchangeThread item : clientList)
							{
								if (item.getExchangeType())
									item.sendMessageBack(nextTuple);

							}*/
						}
					}
					//order was fulfilled 
					else if (action.equals("F"))
					{
						if (company_id > 0)
						{
							synchronized (orderIDtoCompID)
							{
								Integer temp_c_id;
								if ((temp_c_id = orderIDtoCompID.get(new Integer(order_id))) != null)
								{
									System.out.println(nextTuple);
									nextTuple.company_id=temp_c_id.intValue();
									orderIDtoCompID.remove(new Integer(order_id));
									
								}
							}
						}
						
						synchronized(orders_book){
							orders_book.remove(nextTuple);
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
					//order was removed by one of the clients
					else if (action.equals("D")){
						
						if (company_id > 0)
						{
							synchronized (orderIDtoCompID)
							{
								Integer temp_c_id;
								if ((temp_c_id = orderIDtoCompID.get(new Integer(order_id))) != null)
								{
									if (DEBUG){
										System.out.println("got Temp ID "+ temp_c_id+" "+ temp_c_id.intValue());
									}
									nextTuple.company_id=temp_c_id.intValue();
									orderIDtoCompID.remove(new Integer(order_id));
									
								}
								else
								{
									//the order ID had to be found other wise the order was not placed.
									System.out.println("order id not found");
									System.exit(-1);
								}
							}
						}
						
						synchronized(orders_book){
							orders_book.remove(nextTuple);
						}
						synchronized(this.clientList)
						{
							//notify readers about the change
							for (ExchangeThread item : clientList)
							{
								if (item.getExchangeType())
									item.sendMessageBack(nextTuple);

							}
						}
					}
				}
			}else{
				//NEED a better way to deal with this 
				//TODO: reader thread needs to hang around without doing anything
				//the connection should be open.
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

	/**
	 * sets the type of the client
	 * @param b true if this is a reader
	 * @return true if this is a reader
	 */
	private boolean setExchangeType(boolean b) {		
		isToaster=b;
		return isToaster;
	}
	
	/**
	 * gets the type of a client
	 * @return true if it is a reader
	 */
	public boolean getExchangeType(){
		return isToaster;
	}

	/**
	 * Reads input message from a "writer" client
	 * @param in input stream to be read: Timestamp, order id, action, volume, price
	 * @return structured tuple with parameters set to that of the message received
	 */
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
		t.company_id=(Integer.valueOf(input_tuple[2])).intValue();
		t.action=input_tuple[3];
		t.volume=(Integer.valueOf(input_tuple[4])).intValue();
		
		//really annoying but for some reason can be read only as a double
		t.price=Double.valueOf(input_tuple[5]).intValue(); //(Integer.valueOf(input_tuple[5])).intValue();
		
		if (DEBUG){
			System.out.println("ServerThread: got from client: "+t);
		}
		return t;
	}

	/**
	 * Send to the client a message in a form of a Stream_tuple
	 * @param t Timestamp, order id, action, volume, price
	 * @throws IOException
	 */
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
			//current statistics about the stocks and actual matches made
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
