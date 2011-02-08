package org.dbtoaster.cumulus.slicer;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import org.apache.log4j.Logger;
import org.dbtoaster.cumulus.net.NetTypes;
import org.dbtoaster.cumulus.net.TProtocol.TProtocolException;
import org.dbtoaster.cumulus.net.Server;
import org.dbtoaster.cumulus.net.TException;
import org.dbtoaster.cumulus.net.TProcessor;
import org.dbtoaster.cumulus.net.TProtocol;
import org.dbtoaster.cumulus.net.SpreadException;
import org.dbtoaster.cumulus.net.Client;
import org.dbtoaster.cumulus.config.CumulusConfig;

public class SlicerNode
{
  protected static final Logger logger = Logger.getLogger("dbtoaster.Slicer.SlicerNode");

  public interface SlicerNodeIFace
  {
    public void start_switch()
      throws TException;

    public void start_node(int port)
      throws TException;

    public void start_client()
      throws TException;

    public void shutdown()
      throws TException;

    public void start_logging(String host)
      throws TException;

    public void receive_log(String log_message)
      throws TException;

    public String poll_stats()
      throws TException;
  }

  public static SlicerNodeClient getClient(InetSocketAddress addr) throws IOException {
    try {
      return Client.get(addr, SlicerNodeClient.class);
    } catch(Exception e){
      e.printStackTrace();
      return null;
    }
  }

  public static class SlicerNodeClient extends Client implements SlicerNodeIFace
  {
    public SlicerNodeClient(InetSocketAddress s, Selector selector)
      throws IOException
    {
      super(s, selector);
    }
    public void start_switch()
      throws TException
    {
      try {
        SlicerNode.logger.debug("Sending START_SWITCH("+")");
        oprot.putObject(SlicerNodeMethod.START_SWITCH);
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void start_node(int port)
      throws TException
    {
      try {
        SlicerNode.logger.debug("Sending START_NODE("+port+")");
        oprot.beginMessage();
        oprot.putObject(SlicerNodeMethod.START_NODE);
        oprot.putInteger(port);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void start_client()
      throws TException
    {
      try {
        SlicerNode.logger.debug("Sending START_CLIENT("+")");
        oprot.putObject(SlicerNodeMethod.START_CLIENT);
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void shutdown()
      throws TException
    {
      try {
        SlicerNode.logger.debug("Sending SHUTDOWN("+")");
        oprot.putObject(SlicerNodeMethod.SHUTDOWN);
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void start_logging(String host)
      throws TException
    {
      try {
        SlicerNode.logger.debug("Sending START_LOGGING("+host+")");
        oprot.beginMessage();
        oprot.putObject(SlicerNodeMethod.START_LOGGING);
        oprot.putObject(host);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void receive_log(String log_message)
      throws TException
    {
      try {
        SlicerNode.logger.debug("Sending RECEIVE_LOG("+log_message+")");
        oprot.beginMessage();
        oprot.putObject(SlicerNodeMethod.RECEIVE_LOG);
        oprot.putObject(log_message);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public String poll_stats()
      throws TException
    {
      try {
        SlicerNode.logger.debug("Sending POLL_STATS("+")");
        oprot.putObject(SlicerNodeMethod.POLL_STATS);
        waitForFrame();
        return (String)iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }
    }
  }

  public static enum SlicerNodeMethod {
    START_SWITCH,START_NODE,START_CLIENT,SHUTDOWN,START_LOGGING,RECEIVE_LOG,POLL_STATS
  };

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
        throws TException, TProtocolException
      {
        SlicerNode.logger.debug("Received START_SWITCH("+")");
        handler.start_switch();
      }
    }

    private class start_node extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        int port = iprot.getInteger();
        SlicerNode.logger.debug("Received START_NODE("+port+")");
        handler.start_node(port);
      }
    }

    private class start_client extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        SlicerNode.logger.debug("Received START_CLIENT("+")");
        handler.start_client();
      }
    }

    private class shutdown extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        SlicerNode.logger.debug("Received SHUTDOWN("+")");
        handler.shutdown();
      }
    }

    private class start_logging extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String host = (String)iprot.getObject();
        SlicerNode.logger.debug("Received START_LOGGING("+host+")");
        handler.start_logging(host);
      }
    }

    private class receive_log extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String log_message = (String)iprot.getObject();
        SlicerNode.logger.debug("Received RECEIVE_LOG("+log_message+")");
        handler.receive_log(log_message);
      }
    }

    private class poll_stats extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        SlicerNode.logger.debug("Received POLL_STATS("+")");
        oprot.putObject(handler.poll_stats());
      }
    }
  }

  public static void main(String args[]) throws Exception
  {
    CumulusConfig conf = new CumulusConfig();
    conf.defineOption("p", "port", true);
    conf.defineOption("q", "quiet", false);
    conf.defineOption("serve", false);
    conf.configure(args);
    logger.debug("Starting SlicerNode server");
    SlicerNodeIFace handler = conf.loadRubyObject("slicer/slicer.rb", SlicerNodeIFace.class);
    Server s = new Server(new SlicerNode.Processor(handler), (conf.getProperty("port") == null) ? 52980 : Integer.parseInt(conf.getProperty("port")));
    Thread t = new Thread(s);

    t.start();
      // Start up slicer network.
      String serveOption = conf.getProperty("serve");
      logger.debug("Serve option: " + serveOption);
      if ( serveOption == null || !(serveOption.equals("YES")) ) {
          Thread.sleep(2000);
          ((PrimarySlicerNodeIFace)handler).bootstrap();
      } else {
          System.out.println("====> Server Ready <====");
      }
    t.join();
  }
}
