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
    public void update(String relation, List<Double> params, int basecmd)
      throws TException;

    public void put(long id, long template, List<Double> params)
      throws TException;

    public void mass_put(long id, long template, long expected_gets, List<Double> params)
      throws TException;

    public Map<NetTypes.Entry,Double> get(List<NetTypes.Entry> target)
      throws SpreadException, TException;

    public void fetch(List<NetTypes.Entry> target, InetSocketAddress destination, long cmdid)
      throws TException;

    public void push_get(Map<NetTypes.Entry,Double> result, long cmdid)
      throws TException;

    public void meta_request(long base_cmd, List<NetTypes.PutRequest> put_list,
                             List<NetTypes.GetRequest> get_list, List<Double> params)
      throws TException;

    public Map<NetTypes.Entry,Double> aggreget(List<NetTypes.Entry> target, int agg)
      throws SpreadException, TException;

    public String dump() throws TException;

    public void localdump() throws TException;
  }
  
  public static MapNodeClient getClient(InetSocketAddress addr) throws IOException {
    try {
      MapNodeClient c = Client.get(addr, 1, MapNodeClient.class);
    return c;
    } catch(Exception e){
      e.printStackTrace();
      return null;
    }
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

    public void update(String relation, List<Double> params, int basecmd)
      throws TException
    {
      try {
          oprot.beginMessage();
          oprot.putObject(MapNodeMethod.UPDATE);
          oprot.putObject(relation);
          oprot.putObject(new ArrayList(params));
          oprot.putInteger(basecmd);
          oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
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
    
    public Map<NetTypes.Entry,Double> get(List<NetTypes.Entry> target)
      throws SpreadException, TException
    {
      Map<NetTypes.Entry, Double> r = null;
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.GET);
        oprot.putObject(target);
        oprot.endMessage();
        waitForFrame();
        r = (Map<NetTypes.Entry, Double>) iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }
      
      return r;
    }
  
    public void fetch(List<NetTypes.Entry> target, InetSocketAddress destination, long cmdid)
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
    
    public void meta_request(long base_cmd, List<NetTypes.PutRequest> put_list,
                             List<NetTypes.GetRequest> get_list, List<Double> params)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.META_REQUEST);
        oprot.putLong(base_cmd);
        oprot.putList(put_list);
        oprot.putList(get_list);
        oprot.putList(params);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
    
    public Map<NetTypes.Entry,Double> aggreget(List<NetTypes.Entry> target, int agg)
      throws SpreadException, TException
    {
      Map<NetTypes.Entry, Double> r = null;
      try {
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.AGGREGET);
        oprot.putObject(target);
        oprot.putInteger(agg);
        oprot.endMessage();
        waitForFrame();
        r = (Map<NetTypes.Entry, Double>) iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }

      return r;
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
    META_REQUEST, AGGREGET, DUMP, LOCALDUMP };

  public static class Processor extends TProcessor<MapNodeMethod>
  {
    private MapNodeIFace handler;
    
    public Processor(MapNodeIFace h)
    {
      handler = h;
      handlerMap.put(MapNodeMethod.UPDATE, new update());
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
     
    private class update extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
//        System.out.println("In Java");
        String relation = (String) iprot.getObject();
        List<Double> params = (List<Double>) iprot.getObject();
        Integer basecmd = iprot.getInteger();
        handler.update(relation, params, basecmd);
      }
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
        List<NetTypes.Entry> target = (List<NetTypes.Entry>) iprot.getObject();
        NetTypes.regularizeEntryList(target);
        Map<NetTypes.Entry, Double> r = handler.get(target);
        oprot.putObject(r);
      }
    }

    private class fetch extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        List<NetTypes.Entry> target = (List<NetTypes.Entry>) iprot.getObject();
        NetTypes.regularizeEntryList(target);
        InetSocketAddress destination = (InetSocketAddress) iprot.getObject();
        Long cmdid = iprot.getLong();
        handler.fetch(target, destination, cmdid);
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

    private class meta_request extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        Long base_cmd = iprot.getLong();
        List<NetTypes.PutRequest> put_list = iprot.getList(NetTypes.PutRequest.class);
        List<NetTypes.GetRequest> get_list = iprot.getList(NetTypes.GetRequest.class);
        List<Double> params = iprot.getList(Double.class);
        handler.meta_request(base_cmd, put_list, get_list, params);
      }
    }

    private class aggreget extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException,SpreadException
      {
        List<NetTypes.Entry> target = (List<NetTypes.Entry>) iprot.getObject();
        NetTypes.regularizeEntryList(target);
        int agg = iprot.getInteger();
        Map<NetTypes.Entry, Double> r = handler.aggreget(target, agg);
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
