package ExchangeServer;

import java.io.*;
import java.net.*;

/*
 * Thread for reading historic data from a file 
 * and sending it over the network to Exchange Server
 * 
 */

public class DataThread extends Thread{

	private Socket local_socket = null;
	private DataOutputStream out = null;
	private DataInputStream in = null;

	private File file;
	private boolean DEBUG; //debugging flag when on 
	private BufferedReader reader = null;

	/*
	 * Constructor opens connection to server and opens historical file for
	 * reading.
	 */
	public DataThread(String in_file, String port, int socket, boolean d){
		super("DataThread");
		DEBUG=d;
		

		try {
			local_socket = new Socket(port, socket);

			out = new DataOutputStream(local_socket.getOutputStream());
			in = new DataInputStream(local_socket.getInputStream());

			if (DEBUG){
				System.out.println("DataThread: Opened socket: "+local_socket.toString());
			}

			file= new File(in_file);
			reader = new BufferedReader(new FileReader(file));

			StringBuffer contents = new StringBuffer();

		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: "+port);
			System.exit(1);
		} catch (FileNotFoundException e) {
			System.err.println("Couldn't open the reader file "+in_file);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: "+port);
			System.exit(1);
		}

		if (DEBUG){
			System.out.println("Streaming client connects to server.");
		}
	}

	public void run(){

		try {
			Integer type=new Integer(0);
			String text = null;

			byte clientType = type.byteValue();

			//this writes one byte at a time to server to indicate the connection type
			//dirty but works
			out.write(clientType);		    
			out.write(clientType);
			out.write(clientType);
			out.write(clientType);

			//while there is data in the file
			while ((text=reader.readLine())!=null)
			{
				Stream_tuple t=new Stream_tuple();
				String [] input_tuple;
				input_tuple=text.split(",");

				if (DEBUG){
					System.out.println(text);
				}

				t.time=(Integer.valueOf(input_tuple[0])).longValue();
				t.order_id=(Integer.valueOf(input_tuple[1])).intValue();
				t.action=input_tuple[2];
				t.volume=(Integer.valueOf(input_tuple[3])).intValue();
				t.price=((Integer.valueOf(input_tuple[4])).intValue());
				//price is divided by 10K to convert into currency 

				//This indicates to server that this data come from historical file
				//to which we do not know the ID of a brokerage firm. 
				t.company_id=-1;

				if (DEBUG){
					System.out.println("DataThread: sending to server "+t.toString());
				}
				try{
					String msg = t.toString() + "\n";
					byte[] b = msg.getBytes();
					out.write(b, 0, b.length);
				}catch(IOException e){
					System.out.println("exception "+e.getMessage());
				}
				
				if (DEBUG){
					System.out.println("DataThread: finished sending.");
				}

				//this is the only type that expects a return so read and disregard
				if (t.action.equals("B") || t.action.equals("S")){
					boolean message=false;
					byte[] tuple_in_bytes = new byte[512];
					try{
						while (!message){
							if (in.read(tuple_in_bytes)>0){
								message=true;
							}
						}
					}catch(IOException e){
						System.out.println("read bytes ... "+e.getMessage());
					}			    	
				}

			}

		} catch (IOException e) {
			System.err.println("Error occured reading the file");
			e.printStackTrace();
		}
	}


}
