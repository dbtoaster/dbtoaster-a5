package org.dbtoaster.cumulus.scholar;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.util.Map;

import org.dbtoaster.cumulus.config.CumulusConfig;
import org.dbtoaster.cumulus.net.Client;
import org.dbtoaster.cumulus.net.NetTypes;
import org.dbtoaster.cumulus.net.Server;
import org.dbtoaster.cumulus.net.TException;
import org.dbtoaster.cumulus.net.TProcessor;
import org.dbtoaster.cumulus.net.TProtocol;
import org.dbtoaster.cumulus.net.TProtocol.TProtocolException;

public class ScholarNode
{
  public interface ScholarNodeIFace
  {
    public void push_results(Map<NetTypes.Entry, Double> results, int cmdid)
      throws TException;
  }
  
  public static ScholarNodeClient getClient(InetSocketAddress addr)
  {
    try {
      ScholarNodeClient c = Client.get(addr, ScholarNodeClient.class);
    return c;
    } catch(Exception e){
      e.printStackTrace();
      return null;
    }
  }
  
  public static class ScholarNodeClient extends Client implements ScholarNodeIFace
  {
    public ScholarNodeClient(InetSocketAddress s, Selector selector)
      throws IOException
    {
      super(s, selector);
    }

    public void push_results(Map<NetTypes.Entry, Double> results, int cmdid)
      throws TException
    {
      try {
        oprot.beginMessage();
        oprot.putObject(ScholarNodeMethod.PUSH_RESULTS);
        oprot.putMap(results);
        oprot.putInteger(cmdid);
        oprot.endMessage();
      } catch (TProtocolException e) { throw new TException(e.getMessage()); }
    }
  }
  
  public static enum ScholarNodeMethod { PUSH_RESULTS };
  
  public static class Processor extends TProcessor<ScholarNodeMethod>
  {
    private ScholarNodeIFace handler;
    
    public Processor(ScholarNodeIFace h)
    {
      handler = h;
      handlerMap.put(ScholarNodeMethod.PUSH_RESULTS, new push_results());
    }
    
    private class push_results extends TProcessor.HandlerFunction
    {
      public void process(TProtocol iprot, TProtocol oprot)
        throws TException,TProtocolException
      {
        Map<NetTypes.Entry, Double> result = iprot.getMap(NetTypes.Entry.class, Double.class);
        int cmdid = iprot.getInteger();
        handler.push_results(result, cmdid);
      }
    }
  }

  public static void main(String[] args) throws Exception
  {
      CumulusConfig conf = new CumulusConfig();
      conf.defineOption("p", "port", true);
      conf.configure(args);
      
      String portProperty = conf.getProperty("port");
      Integer scholarPort = portProperty == null? 52983 : Integer.parseInt(portProperty);  
      
      ScholarNodeIFace handler = conf.loadRubyObject("scholar/scholar.rb", ScholarNodeIFace.class);
      Server s = new Server(new ScholarNode.Processor(handler), scholarPort);
      Thread t = new Thread(s);
      
      t.start();
      t.join();
  }
}