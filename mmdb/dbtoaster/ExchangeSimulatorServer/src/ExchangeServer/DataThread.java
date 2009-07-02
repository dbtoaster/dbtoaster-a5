package ExchangeServer;

import java.io.*;
import java.net.*;


public class DataThread extends Thread{
	
	public DataThread(String in_file, String port, int socket){
    	super("DataThread");

        try {
        	local_socket = new Socket(port, socket);
        	
        	out = new DataOutputStream(local_socket.getOutputStream());
    	    in = new DataInputStream(local_socket.getInputStream());
    	    
    	    System.out.println("DataThread: Opened socket: "+local_socket.toString());
    	    
//            out = new PrintWriter(local_socket.getOutputStream(), true);
 //           in = new BufferedReader(new InputStreamReader(local_socket.getInputStream()));
            
            //notifying that it is a proxy 


            file= new File(in_file);
            reader = new BufferedReader(new FileReader(file));
 //           fis = new FileInputStream(file);
 //           bis = new BufferedInputStream(fis);
 //           dis = new DataInputStream(bis);
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

    	System.out.println("Streaming client connects to server.");
	}
	
	public void run(){
		
		try {
			Integer type=new Integer(0);
			String text = null;
			
			

		    byte clientType = type.byteValue();
//		    System.out.println("sending the first time");
		    out.write(clientType);		    
		    out.write(clientType);
		    out.write(clientType);
		    out.write(clientType);
/*		    
		    boolean u=true;
		    while(u){
		    try {
		    	out.write(clientType);
		    }catch(IOException e){
		    	u=false;
		    	System.out.println("exception "+e.getMessage());
		    }
		    }
		    
			
			
*/			
			while ((text=reader.readLine())!=null)
			{
				Stream_tuple t=new Stream_tuple();
				String [] input_tuple;
//				System.out.println("DataThread: "+ text);
				input_tuple=text.split(",");
				
				t.time=(Integer.valueOf(input_tuple[0])).longValue();
				t.order_id=(Integer.valueOf(input_tuple[1])).intValue();
				t.action=input_tuple[2];
				t.volume=(Double.valueOf(input_tuple[3])).doubleValue();
				t.price=(Double.valueOf(input_tuple[4])).doubleValue();
				
				System.out.println("DataThread: sending to server "+t.toString());
				try{
				    String msg = t.toString() + "\n";
//				    System.out.println("Sending " + msg);
				    byte[] b = msg.getBytes();
//				    System.out.println("Msg length: " + b.length);
				    out.write(b, 0, b.length);
				}catch(IOException e){
					System.out.println("exception "+e.getMessage());
				}
				System.out.println("DataThread: finished sending.");
				
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

			
/*			while (dis.available() != 0){
				
				Stream_tuple t=new Stream_tuple();
				
				
				System.out.println(dis.);
//				t.time=dis.readLong();
//				dis.readChar();
				t.order_id=dis.readInt();
				dis.readChar();
				t.action=new String((new Character(dis.readChar())).toString());
				dis.readChar();
				t.volume=dis.readDouble();
				dis.readChar();
				t.price=dis.readDouble();
				
				System.out.println("tuple: "+t);
				
/*				System.out.println("DataThread: sending to server "+t.toString());
				try{
				    String msg = t.toString() + "\n";
				    System.out.println("Sending " + msg);
				    byte[] b = msg.getBytes();
				    System.out.println("Msg length: " + b.length);
				    out.write(b, 0, b.length);
				}catch(IOException e){
					System.out.println("exception "+e.getMessage());
				}
				System.out.println("DataThread: finished sending.");
				
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
			*/
		
		} catch (IOException e) {
			System.err.println("Error occured reading the file");
			e.printStackTrace();
		}
	}
	
	private Socket local_socket = null;
    private DataOutputStream out = null;
    private DataInputStream in = null;
    
    private File file;
    private FileInputStream fis = null;
    private BufferedInputStream bis = null;
    private DataInputStream dis = null;
    private DataInput dis2=null;
    private StringBuffer contents = null;
    BufferedReader reader = null;

}
