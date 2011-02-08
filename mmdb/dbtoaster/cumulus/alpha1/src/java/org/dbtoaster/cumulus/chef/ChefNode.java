package org.dbtoaster.cumulus.chef;

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

public class ChefNode
{
  protected static final Logger logger = Logger.getLogger("dbtoaster.Chef.ChefNode");

  public interface ChefNodeIFace
  {
    public void update(String relation, List<Double> params)
      throws TException;

    public void forward_update(String relation, List<Double> params, int basecmd)
      throws TException;

    public void set_forwarders(List<InetSocketAddress> nodes, int nodeOrChef)
      throws TException;

    public void query(Long id, List<Double> params)
      throws TException;

    public void forward_query(Long id, List<Double> params, int basecmd)
      throws TException;

    public String dump()
      throws TException;

    public void request_backoff(String nodeID)
      throws TException;

    public void finish_backoff(String nodeID)
      throws TException;
  }

  public static ChefNodeClient getClient(InetSocketAddress addr) throws IOException {
    try {
      return Client.get(addr, ChefNodeClient.class);
    } catch(Exception e){
      e.printStackTrace();
      return null;
    }
  }

  public static class ChefNodeClient extends Client implements ChefNodeIFace
  {
    public ChefNodeClient(InetSocketAddress s, Selector selector)
      throws IOException
    {
      super(s, selector);
    }
    public void update(String relation, List<Double> params)
      throws TException
    {
      try {
        ChefNode.logger.debug("Sending UPDATE("+relation+", "+params+")");
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.UPDATE);
        oprot.putObject(relation);
        oprot.putList(params);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void forward_update(String relation, List<Double> params, int basecmd)
      throws TException
    {
      try {
        ChefNode.logger.debug("Sending FORWARD_UPDATE("+relation+", "+params+", "+basecmd+")");
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.FORWARD_UPDATE);
        oprot.putObject(relation);
        oprot.putList(params);
        oprot.putInteger(basecmd);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void set_forwarders(List<InetSocketAddress> nodes, int nodeOrChef)
      throws TException
    {
      try {
        ChefNode.logger.debug("Sending SET_FORWARDERS("+nodes+", "+nodeOrChef+")");
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.SET_FORWARDERS);
        oprot.putList(nodes);
        oprot.putInteger(nodeOrChef);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void query(Long id, List<Double> params)
      throws TException
    {
      try {
        ChefNode.logger.debug("Sending QUERY("+id+", "+params+")");
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.QUERY);
        oprot.putLong(id);
        oprot.putList(params);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void forward_query(Long id, List<Double> params, int basecmd)
      throws TException
    {
      try {
        ChefNode.logger.debug("Sending FORWARD_QUERY("+id+", "+params+", "+basecmd+")");
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.FORWARD_QUERY);
        oprot.putLong(id);
        oprot.putList(params);
        oprot.putInteger(basecmd);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public String dump()
      throws TException
    {
      try {
        ChefNode.logger.debug("Sending DUMP("+")");
        oprot.putObject(ChefNodeMethod.DUMP);
        waitForFrame();
        return (String)iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }
    }

    public void request_backoff(String nodeID)
      throws TException
    {
      try {
        ChefNode.logger.debug("Sending REQUEST_BACKOFF("+nodeID+")");
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.REQUEST_BACKOFF);
        oprot.putObject(nodeID);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void finish_backoff(String nodeID)
      throws TException
    {
      try {
        ChefNode.logger.debug("Sending FINISH_BACKOFF("+nodeID+")");
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.FINISH_BACKOFF);
        oprot.putObject(nodeID);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
  }

  public static enum ChefNodeMethod {
    UPDATE,FORWARD_UPDATE,SET_FORWARDERS,QUERY,FORWARD_QUERY,DUMP,REQUEST_BACKOFF,FINISH_BACKOFF
  };

  public static class Processor extends TProcessor<ChefNodeMethod>
  {
    private ChefNodeIFace handler;

    public Processor(ChefNodeIFace h)
    {
      handler = h;
      handlerMap.put(ChefNodeMethod.UPDATE, new update());
      handlerMap.put(ChefNodeMethod.FORWARD_UPDATE, new forward_update());
      handlerMap.put(ChefNodeMethod.SET_FORWARDERS, new set_forwarders());
      handlerMap.put(ChefNodeMethod.QUERY, new query());
      handlerMap.put(ChefNodeMethod.FORWARD_QUERY, new forward_query());
      handlerMap.put(ChefNodeMethod.DUMP, new dump());
      handlerMap.put(ChefNodeMethod.REQUEST_BACKOFF, new request_backoff());
      handlerMap.put(ChefNodeMethod.FINISH_BACKOFF, new finish_backoff());
    }
    private class update extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String relation = (String)iprot.getObject();
        List<Double> params = iprot.getList(Double.class);
        ChefNode.logger.debug("Received UPDATE("+relation+", "+params+")");
        handler.update(relation, params);
      }
    }

    private class forward_update extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String relation = (String)iprot.getObject();
        List<Double> params = iprot.getList(Double.class);
        int basecmd = iprot.getInteger();
        ChefNode.logger.debug("Received FORWARD_UPDATE("+relation+", "+params+", "+basecmd+")");
        handler.forward_update(relation, params, basecmd);
      }
    }

    private class set_forwarders extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        List<InetSocketAddress> nodes = iprot.getList(InetSocketAddress.class);
        int nodeOrChef = iprot.getInteger();
        ChefNode.logger.debug("Received SET_FORWARDERS("+nodes+", "+nodeOrChef+")");
        handler.set_forwarders(nodes, nodeOrChef);
      }
    }

    private class query extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        Long id = iprot.getLong();
        List<Double> params = iprot.getList(Double.class);
        ChefNode.logger.debug("Received QUERY("+id+", "+params+")");
        handler.query(id, params);
      }
    }

    private class forward_query extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        Long id = iprot.getLong();
        List<Double> params = iprot.getList(Double.class);
        int basecmd = iprot.getInteger();
        ChefNode.logger.debug("Received FORWARD_QUERY("+id+", "+params+", "+basecmd+")");
        handler.forward_query(id, params, basecmd);
      }
    }

    private class dump extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        ChefNode.logger.debug("Received DUMP("+")");
        oprot.putObject(handler.dump());
      }
    }

    private class request_backoff extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String nodeID = (String)iprot.getObject();
        ChefNode.logger.debug("Received REQUEST_BACKOFF("+nodeID+")");
        handler.request_backoff(nodeID);
      }
    }

    private class finish_backoff extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String nodeID = (String)iprot.getObject();
        ChefNode.logger.debug("Received FINISH_BACKOFF("+nodeID+")");
        handler.finish_backoff(nodeID);
      }
    }
  }

  public static void main(String args[]) throws Exception
  {
    CumulusConfig conf = new CumulusConfig();
    conf.defineOption("p", "port", true);
    conf.configure(args);
    logger.debug("Starting ChefNode server");
    ChefNodeIFace handler = conf.loadRubyObject("chef/chef.rb", ChefNodeIFace.class);
    Server s = new Server(new ChefNode.Processor(handler), (conf.getProperty("port") == null) ? 52981 : Integer.parseInt(conf.getProperty("port")));
    Thread t = new Thread(s);

    t.start();
    t.join();
  }
}
