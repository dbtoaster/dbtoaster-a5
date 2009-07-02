package ExchangeServer;


import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;

public class ExchangeServer {


	public static void main(String[] args) throws IOException{
		
		ServerSocket serverSocket = null;
        boolean listening = true;
        
        if (args.length<1){
        	System.out.println("Usage: ExchangeServer data_file.cvs");
        	System.exit(-1);
        }
        
        String input_file=new String(args[0]);
        boolean isConnected=false;
        int socket=4452;
        String port="localhost";
        
        //clientList stores all the client toasters/proxies alike.
        List<ExchangeThread> clientList =new LinkedList<ExchangeThread>();

        try {
            serverSocket = new ServerSocket(socket);
        } catch (IOException e) {
            System.err.println("Could not listen on port: 4445.");
            System.exit(-1);
        }
        System.out.println("Server: Opened a socket");
        
        SynchronizedBooks book=new SynchronizedBooks();

        DataThread data_stream= new DataThread(input_file, port, socket);
        

        
        while (listening){


        	ExchangeThread client = new ExchangeThread(serverSocket.accept(), book);
        	
        	synchronized(clientList)
        	{
        		clientList.add(client);
        	}
        	client.setClientList(clientList);
        	client.start();

        	if (!isConnected && clientList.size()>1){// && client!=null){
        		isConnected=true;
 //       		DataThread data_stream= new DataThread(input_file, port, socket);
        		System.out.println("Stating Datathread");
        		data_stream.start();
        		
        	}
        	
        	
        }

        serverSocket.close();

	}
}
