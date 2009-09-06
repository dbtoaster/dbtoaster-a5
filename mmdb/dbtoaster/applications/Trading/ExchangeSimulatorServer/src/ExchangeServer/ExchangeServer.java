package ExchangeServer;


import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;


public class ExchangeServer {
	
		public static void main(String[] args) throws IOException{
		
		boolean DEBUG=true; //flag to output debugging output 
							//when true produces produces a lot of output
		
		ServerSocket serverSocket = null;
        boolean listening = true;

        
        if (args.length<1){ //check for input file with historic data
        	System.out.println("Usage: ExchangeServer data_file.cvs");
        	System.exit(-1);
        }
        
        String input_file=new String(args[0]);
        boolean isConnected=false;
        
        //host and port for the server can 
        int port=5501;  
        String IP="localhost";
        
        //type of matching can be either skip or drop 
        //gives the interaction between data from historic file and algorithms
        boolean doSkip=true;
        
        //clientList stores all clients readers/writers alike.
        //the distinction: writer sends buy/sell requests
        //reader does not send any
        List<ExchangeThread> clientList =new LinkedList<ExchangeThread>();

        //creating a server
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            System.err.println("Could not listen on port: "+port);
            System.exit(-1);
        }
        if (DEBUG){
        	System.out.println("Server: Opened a socket");
        }
        
        //a map to convert order ID (if seen before) to brokerage ID.
        HashMap<Integer, Integer> sharedOrderIdsToCompanyId=new HashMap<Integer, Integer>();
        
        //SycnchronizedBooks are order books for bids and asks
        SynchronizedBooks book=new SynchronizedBooks(DEBUG, doSkip);
        
        //thread for reading the historic data and sending it over network to server
        //as one of the clients.
        DataThread data_stream= new DataThread(input_file, IP, port, DEBUG);
        

        
        while (listening){


        	ExchangeThread client = new ExchangeThread(serverSocket.accept(), 
        			book, DEBUG, sharedOrderIdsToCompanyId);
        	
        	synchronized(clientList)
        	{
        		clientList.add(client);
        	}
        	client.setClientList(clientList);
        	client.start();

        	if (!isConnected && clientList.size()>3){
        		isConnected=true;
        		if (DEBUG){
        			System.out.println("Stating Datathread");
        		}
        		data_stream.start();       		
        	}        	
        }

        //for now this is never executed since the server runs forever.
        serverSocket.close();

	}
}
