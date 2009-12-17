package org.dbtoaster.cumulus.node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.InetSocketAddress;

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
  
  public static MapNodeClient getClient(InetSocketAddress addr){
    return Client.get(addr, MapNodeClient.class);
  }
  
  public static class MapNodeClient extends Client implements MapNodeIFace
  {
    protected TProtocol iprot;
    protected TProtocol oprot;

    public MapNodeClient(TProtocol prot) { this(prot, prot); }
    public MapNodeClient(TProtocol in, TProtocol out) { iprot = in; oprot = out; }

    public void put(long id, long template, List<Double> params)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(Processor.MapNodeMethod.PUT);
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
        oprot.putObject(Processor.MapNodeMethod.MASS_PUT);
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
        oprot.putObject(Processor.MapNodeMethod.GET);
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
        oprot.putObject(Processor.MapNodeMethod.FETCH);
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
        oprot.putObject(Processor.MapNodeMethod.PUSH_GET);
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
        oprot.putObject(Processor.MapNodeMethod.META_REQUEST);
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
        oprot.putObject(Processor.MapNodeMethod.AGGREGET);
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
        oprot.putObject(Processor.MapNodeMethod.DUMP);
        waitForFrame();
        r = (String) iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
        return r;
      }
    
    public void localdump() throws TException
    {
      try {
        oprot.putObject(Processor.MapNodeMethod.LOCALDUMP);
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
    
    public void processFrame(){
      
    }
  }
    
  public static class Processor implements TProcessor
  {
    public static interface HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot) throws TException;
    }
        
    public enum MapNodeMethod {
      PUT, MASS_PUT, GET, FETCH, PUSH_GET,
      META_REQUEST, AGGREGET, DUMP, LOCALDUMP };

    private MapNodeIFace handler;

    protected final HashMap<MapNodeMethod, HandlerFunction> handlerMap =
      new HashMap<MapNodeMethod, HandlerFunction>();
    
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
        
    public boolean process(TProtocol iprot, TProtocol oprot)
      throws TException
    {
      MapNodeMethod key;
      try
      {
        //System.out.println("Getting method...");
        key = (MapNodeMethod) iprot.getObject();
        //System.out.println("Got method key: " + key);

        HandlerFunction fn = handlerMap.get(key);
        if ( fn == null )
        {
          // TODO: Thrift sends out an exception here...
          String message = "No handler found for " + key;
          System.out.println(message);
          //throw new TException(message);
          return false;
        }
        
        fn.process(iprot, oprot);
      } catch (TException te)
      {
      } catch (TProtocolException tpe)
      {
          // TODO: write out an exception...
      }
      
      return true;
    }
        
    private class put implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
      {
        try
        {
          Long id = iprot.getLong();
          Long template = iprot.getLong();
          List<Double> params = (List<Double>) iprot.getObject();
          handler.put(id, template, params);
        } catch (TProtocolException e)
        {
          throw new TException(
            "Protocol error for MapNodeHandler." +
            getClass().getName());
        }
      }
    }
        
    private class mass_put implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
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

    private class get implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
      {
        try
        {
          List<Entry> target = (List<Entry>) iprot.getObject();
          Map<Entry, Double> r = handler.get(target);
          oprot.putObject(r);
        } catch (TProtocolException e)
        {
          throw new TException(
            "Protocol error for MapNodeHandler." +
            getClass().getName());
        } catch (SpreadException spe)
        {
          throw new TException("Function exception: " + spe.getMessage());
        }
      }
    }

    private class fetch implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
      {
        try
        {
          List<Entry> target = (List<Entry>) iprot.getObject();
          NodeID destination = (NodeID) iprot.getObject();
          Long cmdid = iprot.getLong();
          handler.fetch(target, destination, cmdid);
        } catch (TProtocolException e)
        {
          throw new TException(
            "Protocol error for MapNodeHandler." +
            getClass().getName());
          }
        }
    }

    private class push_get implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
      {
        try
        {
          Map<Entry, Double> result = (Map<Entry, Double>) iprot.getObject();
          Long cmdid = iprot.getLong();
          handler.push_get(result, cmdid);
        } catch (TProtocolException e)
        {
          throw new TException(
            "Protocol error for MapNodeHandler." +
            getClass().getName());
        }
      }
    }

    private class meta_request implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
      {
        try {
          Long base_cmd = iprot.getLong();
          List<PutRequest> put_list = (List<PutRequest>) iprot.getObject();
          List<GetRequest> get_list = (List<GetRequest>) iprot.getObject();
          List<Double> params = (List<Double>) iprot.getObject();
          handler.meta_request(base_cmd, put_list, get_list, params);
        } catch (TProtocolException tpe)
        {
          throw new TException(
            "Protocol error for MapNodeHandler." +
            getClass().getName());
        }
      }
    }

    private class aggreget implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
      {
        try
        {
          List<Entry> target = (List<Entry>) iprot.getObject();
          int agg = iprot.getInteger();
          Map<Entry, Double> r = handler.aggreget(target, agg);
          oprot.putObject(r);
        } catch (TProtocolException e)
        {
          throw new TException(
            "Protocol error for MapNodeHandler." +
            getClass().getName());
        } catch (SpreadException spe) {
          throw new TException("Function exception: " + spe.getMessage());
        }
      }
    }

    private class dump implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
      {
        try
        {
          String r = handler.dump();
          oprot.putObject(r);
        } catch (TProtocolException e)
        {
          throw new TException(
            "Protocol error for MapNodeHandler." +
            getClass().getName());
        }
      }
    }

    private class localdump implements HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException
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
    
    MapNodeIFace handler = conf.loadRubyObject("src/ruby/node/node.rb", MapNodeIFace.class);
    Server s = new Server(new MapNode.Processor(handler), 52982);
    Thread t = new Thread(s);
    
    t.start();
    t.join();
  }
}
