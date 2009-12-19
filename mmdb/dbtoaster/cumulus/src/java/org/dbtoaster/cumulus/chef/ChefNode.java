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
    
    public String dump()
      throws TException;
    
    public void request_backoff(String nodeID)
      throws TException;
    
    public void finish_backoff(String nodeID)
      throws TException;
  }
  
  public static ChefNodeClient getClient(InetSocketAddress addr) throws IOException {
    return Client.get(addr, ChefNodeClient.class);
  }

  public static class ChefNodeClient extends Client implements ChefNodeIFace
  {
    public ChefNodeClient(InetSocketAddress s, Selector selector) throws IOException {
      super(s, selector);
    }
    
    public void update(String relation, List<Double> params)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(ChefNodeMethod.UPDATE);
        oprot.putObject(relation);
        oprot.putObject(params);
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
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
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
    
  public static enum ChefNodeMethod { UPDATE,DUMP,REQUEST_BACKOFF,FINISH_BACKOFF };
  
  public static class Processor extends TProcessor<ChefNodeMethod>
  {
    private ChefNodeIFace handler;
    
    public Processor(ChefNodeIFace h)
    {
      handler = h;
      handlerMap.put(ChefNodeMethod.UPDATE, new update());
      handlerMap.put(ChefNodeMethod.DUMP, new dump());
      handlerMap.put(ChefNodeMethod.REQUEST_BACKOFF, new request_backoff());
      handlerMap.put(ChefNodeMethod.FINISH_BACKOFF, new finish_backoff());
    }
    
    private class update extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        String       relation = (String      )iprot.getObject();
        List<Double> params   = (List<Double>)iprot.getObject();
        handler.update(relation, params);
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
    
    ArrayList<Double> params = new ArrayList<Double>();
    params.add(2.0); params.add(5.0);
    handler.update("R", params);
    System.out.println(handler.dump());
    
    t.start();
    t.join();
  }
}
