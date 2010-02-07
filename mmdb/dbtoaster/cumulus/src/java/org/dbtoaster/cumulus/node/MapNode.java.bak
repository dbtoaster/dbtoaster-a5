package org.dbtoaster.cumulus.node;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;

import org.dbtoaster.cumulus.net.NetTypes;
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
    public Map<NetTypes.Entry,Double> get(List<NetTypes.Entry> target)
      throws SpreadException, TException;

    public Map<NetTypes.Entry,Double> aggreget(List<NetTypes.Entry> target, int agg)
      throws SpreadException, TException;

    public void push_get(Map<NetTypes.Entry,Double> result, long cmdid)
      throws TException;

    public void update(String relation, List<Double> params, int basecmd)
      throws TException;

    public void query(long id, List<Double> params, int basecmd)
        throws TException;

    public String dump() throws TException;

    public void localdump() throws TException;
  }
  
  public static MapNodeClient getClient(InetSocketAddress addr) throws IOException
  {
    return getClient(addr,1);
  }

  public static MapNodeClient getClient(InetSocketAddress addr, Integer sendFrameBatchSize)
    throws IOException
  {
    try {
      MapNodeClient c = Client.get(addr, sendFrameBatchSize, MapNodeClient.class);
    return c;
    } catch(Exception e){
      e.printStackTrace();
      return null;
    }
  }

  public static class MapNodeClient extends Client implements MapNodeIFace
  {
    public MapNodeClient(InetSocketAddress s, Integer sendFrameBatchSize, Selector selector)
      throws IOException
    {
      super(s, sendFrameBatchSize, selector);
    }

    public Map<NetTypes.Entry,Double> get(List<NetTypes.Entry> target)
      throws SpreadException, TException
    {
      Map<NetTypes.Entry, Double> r = null;
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.GET);
        oprot.putList(target);
        oprot.endMessage();
        waitForFrame();
        r = iprot.getMap(NetTypes.Entry.class, Double.class);
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }
      
      return r;
    }
  
    public Map<NetTypes.Entry,Double> aggreget(List<NetTypes.Entry> target, int agg)
      throws SpreadException, TException
    {
      Map<NetTypes.Entry, Double> r = null;
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.AGGREGET);
        oprot.putList(target);
        oprot.putInteger(agg);
        oprot.endMessage();
        waitForFrame();
        r = iprot.getMap(NetTypes.Entry.class, Double.class);
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }

      return r;
    }
    
    public void push_get(Map<NetTypes.Entry,Double> result, long cmdid)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.PUSH_GET);
        oprot.putMap(result);
        oprot.putLong(cmdid);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
    
    public void update(String relation, List<Double> params, int basecmd)
      throws TException
    {
      try {
          oprot.beginMessage();
          oprot.putObject(MapNodeMethod.UPDATE);
          oprot.putObject(relation);
          oprot.putList(params);
          oprot.putInteger(basecmd);
          oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
    
    public void query(long id, List<Double> params, int basecmd)
        throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.QUERY);
        oprot.putLong(id);
        oprot.putList(params);
        oprot.putInteger(basecmd);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }

    public String dump() throws TException
    {
      String r = null;
      try {
        oprot.putObject(MapNodeMethod.DUMP);
        waitForFrame();
        r = (String) iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }
      
      return r;
    }
    
    public void localdump() throws TException
    {
      try {
        oprot.putObject(MapNodeMethod.LOCALDUMP);
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
  }
    
  public static enum MapNodeMethod {
    UPDATE, PUT, MASS_PUT, GET, FETCH, PUSH_GET,
    META_REQUEST, AGGREGET, QUERY, DUMP, LOCALDUMP };

  public static class Processor extends TProcessor<MapNodeMethod>
  {
    private MapNodeIFace handler;
    
    public Processor(MapNodeIFace h)
    {
      handler = h;
      handlerMap.put(MapNodeMethod.GET, new get());
      handlerMap.put(MapNodeMethod.AGGREGET, new aggreget());
      handlerMap.put(MapNodeMethod.PUSH_GET, new push_get());
      handlerMap.put(MapNodeMethod.UPDATE, new update());
      handlerMap.put(MapNodeMethod.QUERY, new query());
      handlerMap.put(MapNodeMethod.DUMP, new dump());
      handlerMap.put(MapNodeMethod.LOCALDUMP, new localdump());
    }

    private class get extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException,SpreadException
      {
        List<NetTypes.Entry> target = iprot.getList(NetTypes.Entry.class);
        NetTypes.regularizeEntryList(target);
        Map<NetTypes.Entry, Double> r = handler.get(target);
        oprot.putObject(r);
      }
    }

    private class aggreget extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException,SpreadException
      {
        List<NetTypes.Entry> target = iprot.getList(NetTypes.Entry.class);
        NetTypes.regularizeEntryList(target);
        int agg = iprot.getInteger();
        Map<NetTypes.Entry, Double> r = handler.aggreget(target, agg);
        oprot.putObject(r);
      }
    }

    private class push_get extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        Map<NetTypes.Entry, Double> result = iprot.getMap(NetTypes.Entry.class, Double.class);
        Long cmdid = iprot.getLong();
        handler.push_get(result, cmdid);
      }
    }
     
    private class update extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String relation = (String) iprot.getObject();
        List<Double> params = iprot.getList(Double.class);
        Integer basecmd = iprot.getInteger();
        handler.update(relation, params, basecmd);
      }
    }

    private class query extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        Long id = iprot.getLong();
        List<Double> params = iprot.getList(Double.class);
        Integer basecmd = iprot.getInteger();
        handler.query(id, params, basecmd);
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
    conf.defineOption("p", "port", true);
    conf.configure(args);
    
    String portProperty = conf.getProperty("port");
    Integer nodePort = portProperty == null? 52982 : Integer.parseInt(portProperty);  
    
    MapNodeIFace handler = conf.loadRubyObject("node/node.rb", MapNodeIFace.class);
    Server s = new Server(new MapNode.Processor(handler), nodePort);
    Thread t = new Thread(s);
    
    t.start();
    t.join();
  }
}
