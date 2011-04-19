package org.dbtoaster.cumulus.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import org.dbtoaster.cumulus.net.TProtocol.TProtocolException;

import org.apache.log4j.Logger;

public abstract class TProcessor<ProtocolMethods extends Enum>
{
  protected static final Logger logger = Logger.getLogger("dbtoaster.Net.TProcessor");

  public static abstract class HandlerFunction
  {
    public abstract void process(TProtocol iprot, TProtocol oprot) 
      throws TException,TProtocolException,SpreadException;
    
    public String toString(){
      Class me = getClass();
      String nodeTypeName = "";
      if(me.isMemberClass()){
        if(me.getDeclaringClass().isMemberClass()){
          nodeTypeName = me.getDeclaringClass().getDeclaringClass().getName() + ".";
        } else {
          nodeTypeName = me.getDeclaringClass().getName() + ".";
        }
      }
      return nodeTypeName + me.getName();
    }
    
    public void invoke_process(TProtocol iprot, TProtocol oprot) throws TException
    {
      try
      {
        process(iprot, oprot);
      } catch (TProtocolException e) {
        throw new TException("Protocol error for " + toString());
      } catch (SpreadException e) {
        e.printStackTrace();
        throw new TException("Execution error for " + toString());
      }
    }
  }

  protected final HashMap<ProtocolMethods, HandlerFunction> handlerMap =
    new HashMap<ProtocolMethods, HandlerFunction>();
  
  public boolean process(TProtocol iprot, TProtocol oprot)
  {
    ProtocolMethods key;
    try {
      key = (ProtocolMethods)iprot.getObject();
      HandlerFunction fn = handlerMap.get(key);
      if ( fn == null ) {
        // TODO: Thrift sends out an exception here...
        String host = InetAddress.getLocalHost().getHostName();
        String message = host +
          ": no handler found for " + key.getClass().getName() + "::" + key;
        
        logger.error(message);
        
        for (ProtocolMethods k : handlerMap.keySet()) {
            logger.error(host + " handler fn: " + k);
        }
        return false;
      }
      fn.invoke_process(iprot, oprot);
    } catch (TException te) {
    } catch (TProtocolException tpe) {
        // TODO: write out an exception...
    } catch (UnknownHostException e) { e.printStackTrace(); }
    
    return true;
  }
}
