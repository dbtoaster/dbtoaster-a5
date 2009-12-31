package org.dbtoaster.cumulus.net;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class TProtocol
{
    public class TProtocolException extends Exception
    {
        private static final long serialVersionUID = -2979949841943024603L;
        public TProtocolException(String msg) { super(msg); }
    }

    Integer sizeRead;
    Integer readRemaining;
    ByteBuffer inBuffer;

    ByteArrayInputStream bais;
    ObjectInputStream in;
    
    ByteArrayOutputStream baos;
    ObjectOutputStream out;

    private final int frameBufferSize = 1024*1024;
    private int frameBatchCount = 10;
    private int frameCount = 0;

    public final static Logger logger = Logger.getLogger("dbtoaster.Net.TProtocol");

    TTransport transport;
    
    boolean batch;
    
    Long frameCounter;

    public TProtocol(TTransport t) throws IOException
    {
        transport = t;
        inBuffer = ByteBuffer.allocate(frameBufferSize);

        baos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(baos);

        // Default to sending one object at a time.
        batch = false;
        
        frameCounter = 0L;
    }
    
    public TTransport getTransport() { return transport; }

    public void setFrameBatchSize(Integer s) { frameBatchCount = s; }

    void reset()
    {
        bais = null;
        in = null;
        sizeRead = null;
        readRemaining = null;
        logger.debug("Reset transport, " + toString());
        logger.debug("Resetting transport for input.");
    }

    public String toString()
    {
        String r = "InBuffer: " +
            " p: " + inBuffer.position() +
            " l: " + inBuffer.limit() +
            " r: " + inBuffer.remaining() +
            " c: " + inBuffer.capacity();
        
        r += "\n" + "Read remaining: " + readRemaining + "\n" + "Size read: " + sizeRead;
        return r;
    }

    public Object getObject() throws TProtocolException
    {
        Object r = null;
        
        while ( r == null )
        {
            try {
                if ( in == null ) recvObject();
                if ( in == null ) { throw new TProtocolException("Invalid input protocol."); }
                r = in.readObject();
            } catch (EOFException e) {
                reset();
            } catch (IOException ioe) {
                throw new TProtocolException(
                    "Protocol failed to read object.");
            } catch (ClassNotFoundException e) {
                throw new TProtocolException(
                    "Protocol failed to create object class.");
            }
        }
        
        return r;
    }
    
    public Integer getInteger() throws TProtocolException
    {
        Integer r = null;
        
        while ( r == null )
        {
            try {
                if ( in == null ) recvObject();
                if ( in == null ) { throw new TProtocolException("Invalid input protocol."); }
                r = in.readInt();
            } catch (EOFException e) {
                reset();
            } catch (IOException ioe) {
                throw new TProtocolException("Protocol failed to read integer.");
            }
        }
        
        return r;
    }
    
    public Long getLong() throws TProtocolException
    {
        Long r = null;
        
        while ( r == null )
        {
            try {
                if ( in == null ) recvObject();
                if ( in == null ) { throw new TProtocolException("Invalid input protocol."); }
                r = in.readLong();
            } catch (EOFException e) {
                reset();
            } catch (IOException ioe) {
                throw new TProtocolException("Protocol failed to read long.");
            }
        }
        
        return r;
    }

    public Float getFloat() throws TProtocolException
    {
        Float r = null;
        
        while ( r == null )
        {
            try {
                if ( in == null ) recvObject();
                if ( in == null ) { throw new TProtocolException("Invalid input protocol."); }
                r = in.readFloat();
            } catch (EOFException e) {
                reset();
            } catch (IOException ioe) {
                throw new TProtocolException("Protocol failed to read float.");
            }
        }
        
        return r;
    }

    public Double getDouble() throws TProtocolException
    {
        Double r = null;
        
        while ( r == null )
        {
            try {
                if ( in == null ) recvObject();
                if ( in == null ) { throw new TProtocolException("Invalid input protocol."); }
                r = in.readDouble();
            } catch (EOFException e) {
                reset();
            } catch (IOException ioe) {
                throw new TProtocolException("Protocol failed to read double.");
            }
        }
        
        return r;
    }
    
    public <E> List<E> getList(Class<E> type) throws TProtocolException {
        int len = getLong().intValue();
        List<E> ret = new ArrayList<E>(len);
        for(; len > 0; len--){
          ret.add((E)getObject());
        }
        return ret;
    }

    public <K,V> Map<K,V> getMap(Class<K> keyType, Class<V> valueType) throws TProtocolException {
        int len = getLong().intValue();
        Map<K,V> ret = new HashMap<K,V>();
        for(; len > 0; len--){
          K key = (K)getObject();
          V value = (V)getObject();
          ret.put(key, value);
        }
        return ret;
    }

    public void putObject(Object o) throws TProtocolException
    {
        if ( out == null ) { throw new TProtocolException("Invalid output protocol."); }
        try {
            logger.trace("Putting " + o);
            out.writeObject(o);
            sendObject();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new TProtocolException("Protocol failed to write object.");
        }
    }
    
    public void putInteger(Integer i) throws TProtocolException
    {
        if ( out == null ) { throw new TProtocolException("Invalid output protocol."); }
        try {
            out.writeInt(i);
            sendObject();
        } catch (IOException ioe) {
            throw new TProtocolException("Protocol failed to write object.");
        }
    }

    public void putLong(Long l) throws TProtocolException
    {
        if ( out == null ) { throw new TProtocolException("Invalid output protocol."); }
        try {
            out.writeLong(l);
            sendObject();
        } catch (IOException ioe) {
            throw new TProtocolException("Protocol failed to write object.");
        }
    }

    public void putDouble(Double d) throws TProtocolException
    {
        if ( out == null ) { throw new TProtocolException("Invalid output protocol."); }
        try {
            out.writeDouble(d);
            sendObject();
        } catch (IOException ioe) {
            throw new TProtocolException("Protocol failed to write object.");
        }
    }

    public void putFloat(Float f) throws TProtocolException
    {
        if ( out == null ) { throw new TProtocolException("Invalid output protocol."); }
        try {
            out.writeFloat(f);
            sendObject();
        } catch (IOException ioe) {
            throw new TProtocolException("Protocol failed to write object.");
        }
    }
    
    public <E, T extends List<E>> void putList(T list) throws TProtocolException
    {
        if ( out == null ) { throw new TProtocolException("Invalid output protocol."); }
        try {
            out.writeLong(list.size());
            for(E element : list){
              out.writeObject(element);
            }
            sendObject();
        } catch (IOException ioe) {
            throw new TProtocolException("Protocol failed to write object.");
        }
    }
    
    public <K, V, T extends Map<K,V>> void putMap(T map) throws TProtocolException
    {
        if ( out == null ) { throw new TProtocolException("Invalid output protocol."); }
        try {
            out.writeLong(map.size());
            for(Map.Entry<K,V> entry : map.entrySet()){
              out.writeObject(entry.getKey());
              out.writeObject(entry.getValue());
            }
            sendObject();
        } catch (IOException ioe) {
            throw new TProtocolException("Protocol failed to write object.");
        }
    }

    public void beginMessage() { batch = true; }
    
    public void endMessage() { batch = false; sendObject(); }
    
    // Non-blocking write, returns if we successfully queued for transport.
    boolean sendFrame()
    {
        ++frameCount;
        if ( !batch &&
            (frameBatchCount == 1 ||
                (frameCount < 100 || ((frameCount % frameBatchCount) == 0))) )
        {
            try {
                out.flush();
                byte[] buf = baos.toByteArray();
                int bytesSent = transport.send(buf, 0, buf.length);
                if ( bytesSent != buf.length ) --frameCount;
                return bytesSent == buf.length;
            } catch (IOException e) { e.printStackTrace(); }
        }

        // Pretend as if we succeeded if we're in the middle of a batch.
        return batch
            || (frameBatchCount != 1 &&
                ((frameCount >= 100) && ((frameCount % frameBatchCount) != 0)));
    }
   
    // Non-blocking read, returns if we have a valid frame to read.
    boolean getFrame() throws IOException
    {

        if ( readRemaining == null )
        {
            inBuffer.mark();
            int length = 4 - (sizeRead == null? 0 : sizeRead);
            byte[] buf = new byte[length];
            int bytesRead = transport.recv(buf, 0, length);
            
            if ( bytesRead < length ) {
                sizeRead = (sizeRead == null? bytesRead : sizeRead + bytesRead);
                inBuffer.put(buf, 0, bytesRead);
                return false;
            }
            else {
                sizeRead = 0;
                inBuffer.put(buf);
            }
            
            inBuffer.reset();
            readRemaining = inBuffer.getInt();
            
            if ( readRemaining == 0 ) {
                sizeRead = 0;
                readRemaining = null;
                inBuffer.reset();
                return false;
            }

            inBuffer.mark();
        }
        
        int length = readRemaining; 
        byte[] buf = new byte[length];
        int bytesRead = transport.recv(buf, 0, length);

        // Get an object.
        inBuffer.put(buf, 0, bytesRead);
        if ( readRemaining <= bytesRead )
        {

            // Set limit as the end of data read.
            inBuffer.limit(inBuffer.position());

            // Advance to the end of the frame
            inBuffer.position(inBuffer.position() - (bytesRead-readRemaining));
            
            try {
                // Create an input stream from the frame
                int endPos = inBuffer.position();
                inBuffer.reset();
                int startPos = inBuffer.position();
                byte[] b = new byte[endPos-startPos];
                inBuffer.get(b);
                inBuffer.compact();
                
                bais = new ByteArrayInputStream(b);
                in = new ObjectInputStream(bais);
                sizeRead = 0;
                readRemaining = null;
                ++frameCounter;

                /*
                ByteArrayInputStream dumpb = new ByteArrayInputStream(b);
                ObjectInputStream dump = new ObjectInputStream(dumpb);
                try {
                    System.out.println("Method: " + dump.readObject());
                } catch (EOFException e2) {
                } catch (Exception e2) { e2.printStackTrace(); }
                */
                
            } catch (InvalidMarkException e) {
                byte[] b = new byte[readRemaining];
                inBuffer.get(b, 0, readRemaining);
                inBuffer.compact();

                bais = new ByteArrayInputStream(b);
                in = new ObjectInputStream(bais);
                sizeRead = 0;
                readRemaining = null;

                /*
                ByteArrayInputStream dumpb = new ByteArrayInputStream(b);
                ObjectInputStream dump = new ObjectInputStream(dumpb);
                try {
                    System.out.println("Method: " + dump.readObject());
                } catch (EOFException e2) {
                } catch (Exception e2) { e2.printStackTrace(); }
                */
            }

            return true;
        }
        else {
            readRemaining -= bytesRead;
            logger.trace("Partial read, br: " + bytesRead + ", " + toString());
        }
        
        logger.trace("Insufficient data to create object...");
        return false; 
    }

    // Blocking write: busy waits
    void sendObject()
    {
        try {
            while ( !sendFrame() ) { transport.waitForWrite(); }

            if ( !batch && (frameBatchCount == 1 ||
                    (frameCount < 100 || (frameCount % frameBatchCount) == 0)) )
            {
                baos = new ByteArrayOutputStream();
                out = new ObjectOutputStream(baos);
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    // Blocking read: busy waits, but yields to let other threads do work.
    void recvObject() throws IOException
    {
        while ( !getFrame() ) {
            //try {
                logger.trace("Blocking...");
                //Thread.sleep(1000);
                Thread.yield();
            //} catch (InterruptedException e) { e.printStackTrace(); }
        } 
    }


}
