package org.dbtoaster.cumulus.slicer;

import java.util.HashMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;

import org.dbtoaster.cumulus.config.CumulusConfig;
import org.dbtoaster.cumulus.net.*;
import org.dbtoaster.cumulus.net.TProtocol.TProtocolException;
import org.dbtoaster.cumulus.net.NetTypes.*;

public class SlicerNode
{
    public interface SlicerNodeIFace
    {
        void start_switch() throws TException;
        void start_node(int port) throws TException;
        void start_client() throws TException;
        void shutdown() throws TException;
        void start_logging(String host) throws TException;
        void receive_log(String log_message) throws TException;
        String poll_stats() throws TException;
    }
    
    public interface PrimarySlicerNodeIFace extends SlicerNodeIFace
    {
        void bootstrap() throws TException;
    }

    public static SlicerNodeClient getClient(InetSocketAddress addr)
        throws IOException
    {
        System.out.println("Creating SlicerNodeClient...");
        return Client.get(addr, SlicerNodeClient.class);
    }

    public static class SlicerNodeClient extends Client implements SlicerNodeIFace
    {
        public SlicerNodeClient(InetSocketAddress s, Selector selector)
            throws IOException
        {
            super(s, selector);
        }

        public void start_switch() throws TException
        {
            try {
                oprot.beginMessage();
                oprot.putObject(SlicerNodeMethod.START_SWITCH);
                oprot.endMessage();
            } catch (TProtocolException e) { throw new TException(e.getMessage()); }
        }
        
        public void start_node(int port) throws TException
        {
            try {
                oprot.beginMessage();
                oprot.putObject(SlicerNodeMethod.START_NODE);
                oprot.putInteger(port);
                oprot.endMessage();
            } catch (TProtocolException e) { throw new TException(e.getMessage()); }
        }

        public void start_client() throws TException
        {
            try {
                oprot.beginMessage();
                oprot.putObject(SlicerNodeMethod.START_CLIENT);
                oprot.endMessage();
            } catch (TProtocolException e) { throw new TException(e.getMessage()); }
        }

        public void shutdown() throws TException
        {
            try {
                oprot.beginMessage();
                oprot.putObject(SlicerNodeMethod.SHUTDOWN);
                oprot.endMessage();
            } catch (TProtocolException e) { throw new TException(e.getMessage()); }
        }

        public void start_logging(String host) throws TException
        {
            try {
                oprot.beginMessage();
                oprot.putObject(SlicerNodeMethod.START_LOGGING);
                oprot.putObject(host);
                oprot.endMessage();
            } catch (TProtocolException e) { throw new TException(e.getMessage()); }
        }

        public void receive_log(String logMessage) throws TException
        {
            try {
                oprot.beginMessage();
                oprot.putObject(SlicerNodeMethod.RECEIVE_LOG);
                oprot.putObject(logMessage);
                oprot.endMessage();
            } catch (TProtocolException e) { throw new TException(e.getMessage()); }
        }

        public String poll_stats() throws TException
        {
            String r = null;
            try {
                oprot.beginMessage();
                oprot.putObject(SlicerNodeMethod.POLL_STATS);
                oprot.endMessage();
                r = (String) iprot.getObject();
            } catch (TProtocolException e) { throw new TException(e.getMessage()); }
            return r;
        }
    }

    public static enum SlicerNodeMethod {
        START_SWITCH, START_NODE, START_CLIENT,
        SHUTDOWN, START_LOGGING, RECEIVE_LOG, POLL_STATS };

    public static class Processor extends TProcessor<SlicerNodeMethod>
    {
        private SlicerNodeIFace handler;

        public Processor(SlicerNodeIFace h)
        {
            handler = h;
            handlerMap.put(SlicerNodeMethod.START_SWITCH, new start_switch());
            handlerMap.put(SlicerNodeMethod.START_NODE, new start_node());
            handlerMap.put(SlicerNodeMethod.START_CLIENT, new start_client());
            handlerMap.put(SlicerNodeMethod.SHUTDOWN, new shutdown());
            handlerMap.put(SlicerNodeMethod.START_LOGGING, new start_logging());
            handlerMap.put(SlicerNodeMethod.RECEIVE_LOG, new receive_log());
            handlerMap.put(SlicerNodeMethod.POLL_STATS, new poll_stats());
        }
        
        private class start_switch extends TProcessor.HandlerFunction
        {
            public void process(TProtocol iprot, TProtocol oprot)
                throws TException
            {
                handler.start_switch();
            }
        }
        
        private class start_node extends TProcessor.HandlerFunction
        {
            public void process(TProtocol iprot, TProtocol oprot)
                throws TException
            {
                try {
                    int port = iprot.getInteger();
                    handler.start_node(port);
                } catch (TProtocolException e) {
                    throw new TException(
                        "Protocol error for SlicerNodeHandler." +
                        getClass().getName());
                }
            }
        }
        
        private class start_client extends TProcessor.HandlerFunction
        {
            public void process(TProtocol iprot, TProtocol oprot)
            throws TException
            {
                handler.start_client();
            }
        }
        
        private class shutdown extends TProcessor.HandlerFunction
        {
            public void process(TProtocol iprot, TProtocol oprot)
            throws TException
            {
                handler.shutdown();
            }
        }

        private class start_logging extends TProcessor.HandlerFunction
        {
            public void process(TProtocol iprot, TProtocol oprot)
            throws TException
            {
                try {
                    String host = (String) iprot.getObject();
                    handler.start_logging(host);
                } catch (TProtocolException e) {
                    throw new TException(
                        "Protocol error for SlicerNodeHandler." +
                        getClass().getName());
                }
            }
        }
        
        private class receive_log extends TProcessor.HandlerFunction
        {
            public void process(TProtocol iprot, TProtocol oprot)
            throws TException
            {
                try {
                    String log_message = (String) iprot.getObject();
                    handler.receive_log(log_message);
                } catch (TProtocolException e) {
                    throw new TException(
                        "Protocol error for SlicerNodeHandler." +
                        getClass().getName());
                } 
            }
        }
        
        private class poll_stats extends TProcessor.HandlerFunction
        {
            public void process(TProtocol iprot, TProtocol oprot)
            throws TException
            {
                try {
                    String r = handler.poll_stats();
                    oprot.putObject(r);
                } catch (TProtocolException e) {
                    throw new TException(
                        "Protocol error for SlicerNodeHandler." +
                        getClass().getName());
                }
            }
        }
    }

    public static void main(String args[]) throws Exception
    {
      CumulusConfig conf = new CumulusConfig();
      conf.defineOption("q", "quiet", false);
      conf.defineOption("serve", false);
      conf.configure(args);
      
      PrimarySlicerNodeIFace handler = conf.loadRubyObject("slicer/slicer.rb", PrimarySlicerNodeIFace.class);
      Server s = new Server(new SlicerNode.Processor(handler), 52980);

      System.out.println("Created slicer node handler...");
      
      Thread t = new Thread(s);
      t.start();
      
      // Start up slicer network.
      String serveOption = conf.getProperty("serve");
      System.out.println("Serve option: " + serveOption);
      if ( serveOption == null || !(serveOption.equals("YES")) ) {
          Thread.sleep(2000);
          handler.bootstrap();
      }
      
      // Join with primary slicer server.
      t.join();
    }
}
