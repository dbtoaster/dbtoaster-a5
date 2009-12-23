package org.dbtoaster.cumulus.chef;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;

import org.dbtoaster.cumulus.net.NetTypes.*;
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
  public interface ChefNodeIFace
  {
    public void update(String relation, List<Double> params)
      throws TException;
    
    public void forward_update(String relation, List<Double> params, int basecmd)
      throws TException;
    
    public void set_forwarders(List<InetSocketAddress> nodes, int nodeOrChef)
      throws TException;
    
    public String dump() throws TException;
    
    public void request_backoff(String nodeID) throws TException;
    
    public void finish_backoff(String nodeID) throws TException;
  }
  
  public static ChefNodeClient getClient(InetSocketAddress addr) throws IOException {
    return Client.get(addr, 1, ChefNodeClient.class);
  }

  public static ChefNodeClient getClient(InetSocketAddress addr, Integer sendFrameBatchSize)
    throws IOException {
    return Client.get(addr, sendFrameBatchSize, ChefNodeClient.class);
  }

  public static class ChefNodeClient extends Client implements ChefNodeIFace
  {
    public ChefNodeClient(InetSocketAddress s, Integer sendFrameBatchSize, Selector selector)
      throws IOException
    {
      super(s, sendFrameBatchSize, selector);
    }
    
    public void update(String relation, List<Double> params)
      throws TException
    {
      try {
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
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.SET_FORWARDERS);
        oprot.putList(nodes);
        oprot.putInteger(nodeOrChef);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public String dump()
      throws TException
    {
      try {
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
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.FINISH_BACKOFF);
        oprot.putObject(nodeID);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
  }
    
  public static enum ChefNodeMethod {
      UPDATE,FORWARD_UPDATE,SET_FORWARDERS,DUMP,REQUEST_BACKOFF,FINISH_BACKOFF
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
      handlerMap.put(ChefNodeMethod.DUMP, new dump());
      handlerMap.put(ChefNodeMethod.REQUEST_BACKOFF, new request_backoff());
      handlerMap.put(ChefNodeMethod.FINISH_BACKOFF, new finish_backoff());
    }
    
    private class update extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        String       relation = (String)iprot.getObject();
        List<Double> params   =         iprot.getList(Double.class);
        handler.update(relation, params);
      } 
    }
    
    private class forward_update extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String       relation = (String)iprot.getObject();
        List<Double> params   =         iprot.getList(Double.class);
        Integer      basecmd  =         iprot.getInteger();
        handler.forward_update(relation, params, basecmd);
      }
    }

    private class set_forwarders extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        List<InetSocketAddress> nodes = iprot.getList(InetSocketAddress.class);
        Integer nodeType = iprot.getInteger();
        handler.set_forwarders(nodes, nodeType);
      }
    }
    
    private class dump extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        oprot.putObject(handler.dump());
      }
    }
    
    private class request_backoff extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        String node = (String)iprot.getObject();
        handler.request_backoff(node);
      }
    }
    
    private class finish_backoff extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        String node = (String)iprot.getObject();
        handler.finish_backoff(node);
      }
    }
  }
    
  public static void main(String args[]) throws Exception
  {
    CumulusConfig conf = new CumulusConfig();
    conf.configure(args);
    
    ChefNodeIFace handler = conf.loadRubyObject("chef/chef.rb", ChefNodeIFace.class);
    Server s = new Server(new ChefNode.Processor(handler), 52981);
    Thread t = new Thread(s);
    
    t.start();
    t.join();
  }
}
