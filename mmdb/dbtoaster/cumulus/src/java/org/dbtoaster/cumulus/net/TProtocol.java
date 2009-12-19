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

    private final int frameBufferSize = 32*1024;

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

    void reset()
    {
        bais = null;
        in = null;
        sizeRead = null;
        readRemaining = null;
        /*
        System.out.println("Reset transport, " + toString());
        System.out.println("Resetting transport for input.");
        */
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
                //if ( in != null ) System.out.println("getObject avail before: " + in.available());
                if ( in == null ) recvObject();
                if ( in == null ) { throw new TProtocolException("Invalid input protocol."); }
                r = in.readObject();
                //if ( in != null ) System.out.println("getObject avail after: " + in.available());
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

    public void putObject(Object o) throws TProtocolException
    {
        if ( out == null ) { throw new TProtocolException("Invalid output protocol."); }
        try {
            //System.out.println("Putting " + o);
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

    public void beginMessage() { batch = true; }
    
    public void endMessage() { batch = false; sendObject(); }
    
    // Non-blocking write, returns if we successfully queued for transport.
    boolean sendFrame()
    {
        if ( !batch ) {
            try {
                out.flush();
                byte[] buf = baos.toByteArray();
                int bytesSent = transport.send(buf, 0, buf.length);
                return bytesSent == buf.length;
            } catch (IOException e) { e.printStackTrace(); }
        }

        // Pretend as if we succeeded if we're in the middle of a batch.
        return batch || false;
    }
   
    // Non-blocking read, returns if we have a valid frame to read.
    boolean getFrame() throws IOException
    {
        //System.out.println("Read remaining at start: " + readRemaining);
        //System.out.println("inBuffer remaining: " + inBuffer.remaining());

        if ( readRemaining == null )
        {
            inBuffer.mark();
            int length = 4 - (sizeRead == null? 0 : sizeRead);
            byte[] buf = new byte[length];
            int bytesRead = transport.recv(buf, 0, length);
            
            /*
            System.out.println("Read transport: " + bytesRead +
                " requested: " + length +
                " prev sizeRead: " + (sizeRead == null? 0 : sizeRead));
            */

            if ( bytesRead < length ) {
                sizeRead = (sizeRead == null? bytesRead : sizeRead + bytesRead);
                inBuffer.put(buf, 0, bytesRead);
                return false;
            }
            else {
                //System.out.println("Read frame size, br: " + bytesRead + ", " + toString());
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
        
        //System.out.println("Read remaining for obj: " + readRemaining);
        
        int length = readRemaining; 
        byte[] buf = new byte[length];
        int bytesRead = transport.recv(buf, 0, length);

        // Get an object.
        //System.out.println("Transport recv, br: " + bytesRead +
        //    ", req: " + length + ", " + toString());

        inBuffer.put(buf, 0, bytesRead);
        if ( readRemaining <= bytesRead )
        {
            //System.out.println("Reading frame, br: " + bytesRead + " " + toString());

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
                
                //System.out.println("Start:" + startPos + " end: " + endPos);

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

                /*
                System.out.println("Created an object of length: " + b.length +
                    " avail 1: " + bais.available() +
                    " avail 2: " + in.available());
                */
                
                //System.out.println("Got marked frame " + frameCounter + ", " + toString());

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

                /*
                System.out.println("Created an object of length: " + b.length +
                    " avail 1: " + bais.available() +
                    " avail 2: " + in.available());
                */
                
                //System.out.println("Got frame " + frameCounter + ", " + toString());
            }

            return true;
        }
        else {
            readRemaining -= bytesRead;
            //System.out.println("Partial read, br: " + bytesRead + ", " + toString());
        }
        
        //System.out.println("Insufficient data to create object...");
        return false; 
    }

    // Blocking write: busy waits
    void sendObject()
    {
        try {
            while ( !sendFrame() ) { Thread.yield(); }
            if ( !batch ) {
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
                //System.out.println("Blocking...");
                //Thread.sleep(1000);
                Thread.yield();
            //} catch (InterruptedException e) { e.printStackTrace(); }
        } 
    }


}
