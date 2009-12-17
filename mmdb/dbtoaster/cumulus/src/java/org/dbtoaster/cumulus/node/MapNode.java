package org.dbtoaster.cumulus.node;

import java.util.HashMap;
import java.util.List;
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

public class MapNode
{
  public interface MapNodeIFace
  {
    public void put(long id, long template, List<Double> params)
      throws TException;

    public void mass_put(long id, long template, long expected_gets, List<Double> params)
      throws TException;

    public Map<Entry,Double> get(List<Entry> target)
      throws SpreadException, TException;

    public void fetch(List<Entry> target, NodeID destination, long cmdid)
      throws TException;

    public void push_get(Map<Entry,Double> result, long cmdid)
      throws TException;

    public void meta_request(long base_cmd, List<PutRequest> put_list,
                             List<GetRequest> get_list, List<Double> params)
      throws TException;

    public Map<Entry,Double> aggreget(List<Entry> target, int agg)
      throws SpreadException, TException;

    public String dump() throws TException;

    public void localdump() throws TException;
  }
  
  public static MapNodeClient getClient(InetSocketAddress addr) throws IOException {
    return Client.get(addr, MapNodeClient.class);
  }
  
  public static class MapNodeClient extends Client implements MapNodeIFace
  {
    public MapNodeClient(InetSocketAddress s, Selector selector) throws IOException {
      super(s, selector);
    }

    public void put(long id, long template, List<Double> params)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.PUT);
        oprot.putLong(id);
        oprot.putLong(template);
        oprot.putObject(params);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public void mass_put(long id, long template, long expected_gets, List<Double> params)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.MASS_PUT);
        oprot.putLong(id);
        oprot.putLong(template);
        oprot.putLong(expected_gets);
        oprot.putObject(params);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
    
    public Map<Entry,Double> get(List<Entry> target)
      throws SpreadException, TException
    {
      Map<Entry, Double> r = null;
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.GET);
        oprot.putObject(target);
        oprot.endMessage();
        waitForFrame();
        r = (Map<Entry, Double>) iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
      return r;
    }
  
    public void fetch(List<Entry> target, NodeID destination, long cmdid)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.FETCH);
        oprot.putObject(target);
        oprot.putObject(destination);
        oprot.putLong(cmdid);
        oprot.endMessage();
      } catch (TProtocolException e) {
        e.printStackTrace();
        throw new TException(e.getMessage());
      }
    }
    
    public void push_get(Map<Entry,Double> result, long cmdid)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.PUSH_GET);
        oprot.putObject(result);
        oprot.putLong(cmdid);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
    
    public void meta_request(long base_cmd, List<PutRequest> put_list,
                             List<GetRequest> get_list, List<Double> params)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.META_REQUEST);
        oprot.putLong(base_cmd);
        oprot.putObject(put_list);
        oprot.putObject(get_list);
        oprot.putObject(params);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
    
    public Map<Entry,Double> aggreget(List<Entry> target, int agg)
      throws SpreadException, TException
    {
      Map<Entry, Double> r = null;
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.AGGREGET);
        oprot.putObject(target);
        oprot.putInteger(agg);
        oprot.endMessage();
        waitForFrame();
        r = (Map<Entry, Double>) iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
      return r;
    }
    
    public String dump() throws TException
    {
      String r = null;
      try {
        oprot.putObject(MapNodeMethod.DUMP);
        waitForFrame();
        r = (String) iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
        return r;
      }
    
    public void localdump() throws TException
    {
      try {
        oprot.putObject(MapNodeMethod.LOCALDUMP);
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
    
    public void processFrame(){
      
    }
  }
    
  public static enum MapNodeMethod {
    PUT, MASS_PUT, GET, FETCH, PUSH_GET,
    META_REQUEST, AGGREGET, DUMP, LOCALDUMP };

  public static class Processor extends TProcessor<MapNodeMethod>
  {
    private MapNodeIFace handler;
    
    public Processor(MapNodeIFace h)
    {
      handler = h;
      handlerMap.put(MapNodeMethod.PUT, new put());
      handlerMap.put(MapNodeMethod.MASS_PUT, new mass_put());
      handlerMap.put(MapNodeMethod.GET, new get());
      handlerMap.put(MapNodeMethod.FETCH, new fetch());
      handlerMap.put(MapNodeMethod.PUSH_GET, new push_get());
      handlerMap.put(MapNodeMethod.META_REQUEST, new meta_request());
      handlerMap.put(MapNodeMethod.AGGREGET, new aggreget());
      handlerMap.put(MapNodeMethod.DUMP, new dump());
      handlerMap.put(MapNodeMethod.LOCALDUMP, new localdump());
    }
        
    private class put extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        Long id = iprot.getLong();
        Long template = iprot.getLong();
        List<Double> params = (List<Double>) iprot.getObject();
        handler.put(id, template, params);
      }
    }
        
    private class mass_put extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        try
        {
          Long id = iprot.getLong();
          Long template = iprot.getLong();
          Long expected_gets = iprot.getLong();
          List<Double> params = (List<Double>) iprot.getObject();
          handler.mass_put(id, template, expected_gets, params);
        } catch (TProtocolException e)
        {
          throw new TException(
            "Protocol error for MapNodeHandler." +
            getClass().getName());
        }
      }
    }

    private class get extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException,SpreadException
      {
        List<Entry> target = (List<Entry>) iprot.getObject();
        Map<Entry, Double> r = handler.get(target);
        oprot.putObject(r);
      }
    }

    private class fetch extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        List<Entry> target = (List<Entry>) iprot.getObject();
        NodeID destination = (NodeID) iprot.getObject();
        Long cmdid = iprot.getLong();
        handler.fetch(target, destination, cmdid);
      }
    }

    private class push_get extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        Map<Entry, Double> result = (Map<Entry, Double>) iprot.getObject();
        Long cmdid = iprot.getLong();
        handler.push_get(result, cmdid);
      }
    }

    private class meta_request extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        Long base_cmd = iprot.getLong();
        List<PutRequest> put_list = (List<PutRequest>) iprot.getObject();
        List<GetRequest> get_list = (List<GetRequest>) iprot.getObject();
        List<Double> params = (List<Double>) iprot.getObject();
        handler.meta_request(base_cmd, put_list, get_list, params);
      }
    }

    private class aggreget extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException,SpreadException
      {
        List<Entry> target = (List<Entry>) iprot.getObject();
        int agg = iprot.getInteger();
        Map<Entry, Double> r = handler.aggreget(target, agg);
        oprot.putObject(r);
      }
    }

    private class dump extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        String r = handler.dump();
        oprot.putObject(r);
      }
    }

    private class localdump extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        handler.localdump();
      }
    }
  }
    
  public static void main(String args[]) throws Exception
  {
    CumulusConfig conf = new CumulusConfig();
    conf.defineOption("name", true);
    conf.configure(args);
    
    MapNodeIFace handler = conf.loadRubyObject("node/node.rb", MapNodeIFace.class);
    Server s = new Server(new MapNode.Processor(handler), 52982);
    Thread t = new Thread(s);
    
    t.start();
    t.join();
  }
}
