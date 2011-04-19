package org.dbtoaster.cumulus.node;

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

public class MapNode
{
  protected static final Logger logger = Logger.getLogger("dbtoaster.Node.MapNode");

  public interface MapNodeIFace
  {
    public Map<NetTypes.Entry,Double> get(List<NetTypes.Entry> target)
      throws TException;

    public Map<NetTypes.Entry,Double> aggreget(List<NetTypes.Entry> target, int agg)
      throws TException;

    public void push_get(Map<NetTypes.Entry,Double> result, long cmdid)
      throws TException;

    public void update(String relation, List<Double> params, int basecmd)
      throws TException;

    public void query(long id, List<Double> params, int basecmd)
      throws TException;

    public String dump()
      throws TException;

    public void localdump()
      throws TException;
  }

  public static MapNodeClient getClient(InetSocketAddress addr) throws IOException {
    try {
      return Client.get(addr, MapNodeClient.class);
    } catch(Exception e){
      e.printStackTrace();
      return null;
    }
  }

  public static class MapNodeClient extends Client implements MapNodeIFace
  {
    public MapNodeClient(InetSocketAddress s, Selector selector)
      throws IOException
    {
      super(s, selector);
    }
    public Map<NetTypes.Entry,Double> get(List<NetTypes.Entry> target)
      throws TException
    {
      try {
        MapNode.logger.debug("Sending GET("+target+")");
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.GET);
        oprot.putList(target);
        oprot.endMessage();
        waitForFrame();
        return (Map<NetTypes.Entry,Double>)iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }
    }

    public Map<NetTypes.Entry,Double> aggreget(List<NetTypes.Entry> target, int agg)
      throws TException
    {
      try {
        MapNode.logger.debug("Sending AGGREGET("+target+", "+agg+")");
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.AGGREGET);
        oprot.putList(target);
        oprot.putInteger(agg);
        oprot.endMessage();
        waitForFrame();
        return (Map<NetTypes.Entry,Double>)iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }
    }

    public void push_get(Map<NetTypes.Entry,Double> result, long cmdid)
      throws TException
    {
      try {
        MapNode.logger.debug("Sending PUSH_GET("+result+", "+cmdid+")");
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
        MapNode.logger.debug("Sending UPDATE("+relation+", "+params+", "+basecmd+")");
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
        MapNode.logger.debug("Sending QUERY("+id+", "+params+", "+basecmd+")");
        oprot.beginMessage();
        oprot.putObject(MapNodeMethod.QUERY);
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
        MapNode.logger.debug("Sending DUMP("+")");
        oprot.putObject(MapNodeMethod.DUMP);
        waitForFrame();
        return (String)iprot.getObject();
      } catch (TProtocolException e) { throw new TException(e.getMessage());
      } catch (IOException e) { throw new TException(e.getMessage()); }
    }

    public void localdump()
      throws TException
    {
      try {
        MapNode.logger.debug("Sending LOCALDUMP("+")");
        oprot.putObject(MapNodeMethod.LOCALDUMP);
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
  }

  public static enum MapNodeMethod {
    GET,AGGREGET,PUSH_GET,UPDATE,QUERY,DUMP,LOCALDUMP
  };

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
        throws TException, TProtocolException
      {
        List<NetTypes.Entry> target = iprot.getList(NetTypes.Entry.class);
        MapNode.logger.debug("Received GET("+target+")");
        oprot.putObject(handler.get(target));
      }
    }

    private class aggreget extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        List<NetTypes.Entry> target = iprot.getList(NetTypes.Entry.class);
        int agg = iprot.getInteger();
        MapNode.logger.debug("Received AGGREGET("+target+", "+agg+")");
        oprot.putObject(handler.aggreget(target, agg));
      }
    }

    private class push_get extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        Map<NetTypes.Entry,Double> result = iprot.getMap(NetTypes.Entry.class, Double.class);
        long cmdid = iprot.getLong();
        MapNode.logger.debug("Received PUSH_GET("+result+", "+cmdid+")");
        handler.push_get(result, cmdid);
      }
    }

    private class update extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        String relation = (String)iprot.getObject();
        List<Double> params = iprot.getList(Double.class);
        int basecmd = iprot.getInteger();
        MapNode.logger.debug("Received UPDATE("+relation+", "+params+", "+basecmd+")");
        handler.update(relation, params, basecmd);
      }
    }

    private class query extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        long id = iprot.getLong();
        List<Double> params = iprot.getList(Double.class);
        int basecmd = iprot.getInteger();
        MapNode.logger.debug("Received QUERY("+id+", "+params+", "+basecmd+")");
        handler.query(id, params, basecmd);
      }
    }

    private class dump extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        MapNode.logger.debug("Received DUMP("+")");
        oprot.putObject(handler.dump());
      }
    }

    private class localdump extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException, TProtocolException
      {
        MapNode.logger.debug("Received LOCALDUMP("+")");
        handler.localdump();
      }
    }
  }

  public static void main(String args[]) throws Exception
  {
    CumulusConfig conf = new CumulusConfig();
    conf.defineOption("p", "port", true);
    conf.configure(args);
    logger.debug("Starting MapNode server");
    MapNodeIFace handler = conf.loadRubyObject("node/node.rb", MapNodeIFace.class);
    Server s = new Server(new MapNode.Processor(handler), (conf.getProperty("port") == null) ? 52982 : Integer.parseInt(conf.getProperty("port")));
    Thread t = new Thread(s);

    t.start();
    t.join();
  }
}
