package org.dbtoaster.cumulus.net;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.io.IOException;

public abstract class Client
{
  protected TProtocol iprot;
  protected TProtocol oprot;
  protected Semaphore pendingFrames = new Semaphore(0);

  public Client(InetSocketAddress saddr, Selector selector) throws IOException
  {
    this(new TProtocol(new TTransport(saddr)));
    iprot.getTransport().connect(null);
    connectTransport(this);
    pendingFrames.acquireUninterruptibly();
  }
  public Client(TProtocol prot) { this(prot, prot); }
  public Client(TProtocol in, TProtocol out) { iprot = in; oprot = out; }
  
  public TProtocol getInputProtocol() { return iprot; }
  public TProtocol getOutputProtocol() { return oprot; }
  
  public void processFrame(){
    pendingFrames.release();
  }
  
  public void waitForFrame(){
    pendingFrames.acquireUninterruptibly();
  }
  
  /////////////////////////////////////////////////////////////////////////////////
  protected static HashMap<Class,HashMap<InetSocketAddress,Object>> singletons = 
    new HashMap<Class,HashMap<InetSocketAddress,Object>>();
  protected static SocketMonitor monitor = new SocketMonitor();
  
  public static <T extends Client> T get(InetSocketAddress addr, Class<T> c) throws IOException
  {
    HashMap<InetSocketAddress,Object> cTypeSingletons = singletons.get(c);
    if(cTypeSingletons == null){
      cTypeSingletons = new HashMap<InetSocketAddress,Object>();
      singletons.put(c, cTypeSingletons);
    }
    
    T ret = (T)cTypeSingletons.get(addr);
    if(ret == null){
      try { 
        
        System.out.println(InetAddress.getLocalHost() + " constructor for " + addr.toString() + ", " + c.getName() +
            " monitor: " + (monitor == null? "null" : monitor));
        
        //Thread.dumpStack();

        Constructor<T> cons = c.getConstructor(InetSocketAddress.class, Selector.class);
        
        System.out.println(InetAddress.getLocalHost() + " constructor: " + cons);
        
        ret = cons.newInstance(addr, monitor.selector());
        
        System.out.println(InetAddress.getLocalHost() + " built for " + addr.toString() + ", " + c.getName());
        cTypeSingletons.put(addr, ret);
        
      } catch(NoSuchMethodException e){
        System.err.println("Error: Class '" + c.toString() + "' is not a valid Client subclass");
        e.printStackTrace();
      } catch(InstantiationException e){
        System.err.println("Error: Class '" + c.toString() + "' is not a valid Client subclass");
        e.printStackTrace();
      } catch(IllegalArgumentException e){
        System.err.println("Error: Class '" + c.toString() + "' is not a valid Client subclass");
        e.printStackTrace();
      } catch(InvocationTargetException e){
        System.err.println("Error: Class '" + c.toString() + "' is not a valid Client subclass");
        e.printStackTrace();
      } catch(IllegalAccessException e){
        System.err.println("Error: Class '" + c.toString() + "' is not a valid Client subclass");
        e.printStackTrace();
      } catch(ExceptionInInitializerError e){
        if(IOException.class.isAssignableFrom(e.getException().getClass())){
          throw (IOException)e.getException();
        }
        System.err.println("Error: Class '" + c.toString() + "' is not a valid Client subclass");
        e.printStackTrace();
      } catch (Exception e) {
          System.err.println("Error: Class '" + c.toString() + "' is not a valid Client subclass");
          e.printStackTrace();
      }
    }
    
    System.out.println("Creating client for " + c.getName());
    return ret;
  }
  
  protected static void connectTransport(Client c) throws IOException {
    monitor.connectTransport(c);
  }
  
  protected static class SocketMonitor extends Thread {
    Selector selector;
    boolean terminated;
    ConcurrentLinkedQueue<Client> newClientQueue;
    
    public SocketMonitor()
    {
      try {
        newClientQueue = new ConcurrentLinkedQueue<Client>();
        selector = Selector.open();
        terminated = false;
        start();
      } catch (IOException e){
        e.printStackTrace();
        System.exit(-1);
      }
    }
    
    public Selector selector(){
      return selector;
    }
    
    public void connectTransport(Client c) throws IOException {
      newClientQueue.add(c);
      selector.wakeup();
    }
    
    public void run()
    {
      while (!terminated)
      {
        try 
        {
          Client newClient;
          while((newClient = newClientQueue.poll()) != null){
            newClient.getInputProtocol().getTransport().
              registerSelector(selector, SelectionKey.OP_CONNECT).attach(newClient);
          }
          
          selector.select();
          Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
          while(keys.hasNext())
          {
            SelectionKey key = keys.next();
            Client c = (Client)key.attachment();
            
            boolean processed = false;

            if(key.isConnectable()){
              handleConnect(c);
              processed = true;
            }
            if(key.isReadable()){
              handleRead(c);
              processed = true;
            }
            if(key.isWritable()) {
              handleWrite(c);
              processed = true;
            }
            
            if ( !processed ) {
              System.err.println("Key registered for select in Client, "+
                  "but neither readable nor writable");
            }
          }
        } catch (IOException ioe) {
          ioe.printStackTrace();
//        } catch (TException e) {
//          e.printStackTrace();
        }
      }
    }
    
    void handleConnect(Client c) throws IOException
    {
      c.getOutputProtocol().getTransport().finishConnection();
      if(c.getOutputProtocol() != c.getInputProtocol()){
        c.getOutputProtocol().getTransport().finishConnection();
      }
      c.processFrame(); //client blocks on this completion;
    }
    
    void handleRead(Client c) throws IOException
    {
      int bytesRead = c.getInputProtocol().getTransport().read();
      if ( bytesRead > 0 )
      {
        while ( c.getInputProtocol().getFrame() )
        {
          c.processFrame();
        }
      }
    }
    
    void handleWrite(Client c) throws IOException
    {
      c.getOutputProtocol().getTransport().write();
    }
  }
}
