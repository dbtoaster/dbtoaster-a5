package org.dbtoaster.cumulus.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

public class TTransport
{
    private Selector selector;

    private InetSocketAddress server;
    private SocketChannel channel;
    
    private SelectionKey key;
    
    private Semaphore pendingWrites;
    
    protected final static Logger logger = Logger.getLogger("dbtoaster.Net.TTransport");

    // Key interest change batching.
    class InterestBuffer
    {
        Timer taskRunner;
        boolean scheduled;
        long delay;

        private int keyOperation;
        boolean enableOperation;
        ConcurrentLinkedQueue<SelectionKey> pendingKeys;

        class KeyTask extends TimerTask
        {
            public void run()
            {
                SelectionKey k;
                while ( (k = pendingKeys.poll()) != null ) {
                    k.interestOps(enableOperation?
                        (k.interestOps() | keyOperation) :
                        (k.interestOps() ^ keyOperation));
                }
                selector.wakeup();
                scheduled = false;
            }
        }

        InterestBuffer(int op, boolean enable, long d) {
            taskRunner = new Timer();
            scheduled = false;
            delay = d;

            keyOperation = op;
            enableOperation = enable;
            pendingKeys = new ConcurrentLinkedQueue<SelectionKey>();
        }

        public void addKey(SelectionKey k) {
            pendingKeys.add(k);
            synchronized ( taskRunner ) {
                if ( !scheduled ) {
                    scheduled = true;
                    taskRunner.schedule(new KeyTask(), delay);
                }
            }
        }

    }
    
    InterestBuffer registerWriteTask;
    //InterestBuffer registerReadTask;
    //InterestBuffer cancelReadTask;

    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    
    private Long totalRead;
    private Long totalWritten;
    
    private final int readBufferSize = 1024*1024; 
    private final int writeBufferSize = 1024*1024;
    private final int socketSendBufferSize = 1024*1024;
    private final int socketRecvBufferSize = 1024*1024;

    public TTransport(InetSocketAddress s)
    {
        server = s;
        readBuffer = ByteBuffer.allocate(readBufferSize);
        writeBuffer = ByteBuffer.allocate(writeBufferSize);
        
        totalRead = 0L;
        totalWritten = 0L;
        
        pendingWrites = new Semaphore(0);
        registerWriteTask = new InterestBuffer(SelectionKey.OP_WRITE, true, 1000);
        //registerReadTask = new InterestBuffer(SelectionKey.OP_READ, true, 200);
        //cancelReadTask = new InterestBuffer(SelectionKey.OP_READ, false, 200);
    }
    
    public TTransport(SocketChannel c) throws IOException
    {
        channel = c;
        server = (InetSocketAddress) c.socket().getRemoteSocketAddress();
        channel.configureBlocking(false);
        readBuffer = ByteBuffer.allocate(readBufferSize);
        writeBuffer = ByteBuffer.allocate(writeBufferSize);
        
        totalRead = 0L;
        totalWritten = 0L;

        pendingWrites = new Semaphore(0);
        registerWriteTask = new InterestBuffer(SelectionKey.OP_WRITE, true, 1000);
        //registerReadTask = new InterestBuffer(SelectionKey.OP_READ, true, 200);
        //cancelReadTask = new InterestBuffer(SelectionKey.OP_READ, false, 200);
    }

    public SelectionKey registerSelector(Selector s, int interests) throws IOException
    {
        selector = s;
        key = channel.register(selector, interests);
        return key;
    }

    public void connect(Selector s) throws IOException
    {
        channel = SocketChannel.open();
        logger.trace("Connecting transport to " + server);
        channel.configureBlocking(false);
        channel.socket().setSendBufferSize(socketSendBufferSize);
        channel.socket().setReceiveBufferSize(socketRecvBufferSize);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoLinger(false, 0);
        channel.socket().setSoTimeout(0);
        channel.connect(server);
        if(s != null){
          key = registerSelector(s, SelectionKey.OP_CONNECT);
          key.attach(this);
        }
    }

    public void finishConnection() throws IOException
    {
        if ( channel.isConnectionPending() ) {
            channel.finishConnect();
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            logger.info("Connected to " + server.toString());
            System.out.flush();
        }
    }

    public int read() throws IOException
    {
        int bytesRead = 0;
        synchronized (readBuffer)
        {
            try {
                bytesRead = channel.read(readBuffer);
            } catch (IOException ioe) {
                key.cancel();
                channel.close();
            }
        }
        
        if ( bytesRead == -1 ) {
            key.cancel();
            channel.close();
        }
        totalRead += bytesRead;
        logger.trace("TTransport read: " + bytesRead + ", " + toString());
        return bytesRead;
    }

    public int write() throws IOException
    {
        int bytesWritten = 0;
        synchronized (writeBuffer)
        {
            writeBuffer = (ByteBuffer) writeBuffer.flip();
            bytesWritten = channel.write(writeBuffer);
            
            // Note we immediately apply the key change here since this
            // should only be called from the selector thread.
            if ( writeBuffer.hasRemaining() )
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            else
                key.interestOps(key.interestOps() ^ SelectionKey.OP_WRITE);
            writeBuffer.compact();
        }
        totalWritten += bytesWritten;
        logger.trace("Transport wrote: " + bytesWritten + ", " + toString());
        
        if ( writeBuffer.remaining() > 0 ) writeAvailable();

        return bytesWritten;
    }
    
    public void waitForWrite() { pendingWrites.acquireUninterruptibly(); }
    
    public void writeAvailable() { pendingWrites.release(); }

    public int send(byte[] buf, int off, int len)
    {
        int bytesSent = 0;
        synchronized(writeBuffer)
        {
            logger.trace(server + ": sending: " + len);

            // Manually detect overflow.
            if ( writeBuffer.remaining() >= (4+len) )
            {
                writeBuffer.putInt(len);
                writeBuffer.put(buf, off, len);
                //key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                registerWriteTask.addKey(key);
                //selector.wakeup();
                bytesSent = len;
            }
        }
        
        return bytesSent;
    }
    
    public int recv(byte[] buf, int off, int len) throws IOException
    {
        int bytesRecv = 0;
        synchronized(readBuffer)
        {
            bytesRecv = readBuffer.position();
            
            logger.trace("TTransport recv: "+len+", " + " br: " + bytesRecv + ", " + toString());

            if ( bytesRecv > 0 )
            {
                readBuffer.flip();

                // Manually detect underflow.
                if ( len <= readBuffer.remaining() )
                {
                    readBuffer.get(buf, off, len);
                    readBuffer.compact();
                } else {
                    bytesRecv = 0;
                    readBuffer.compact();
                    logger.trace("Buffer underflow, req: " + len + ",  "+ toString());
                    //e.printStackTrace();
                }
            }
            
            if ( key.isValid() )
            {
                if ( bytesRecv < len ) {
                    logger.trace("Adding key " + key + " for read.");
                    key.interestOps(key.interestOps() | SelectionKey.OP_READ);
                    //registerReadTask.addKey(key);
                }

                else {
                    logger.trace("Removing key " + key + " for read.");
                    key.interestOps(key.interestOps() ^ SelectionKey.OP_READ);
                    //cancelReadTask.addKey(key);
                }

                //selector.wakeup();
            }
            else {
                logger.warn("Invalid key: " + key);
            }
        }
        
        return Math.min(bytesRecv, len);
    }

    public InetSocketAddress getRemote() { return server; }

    public String toString()
    {
        String r = "ReadBuffer: " +
            " p: " + readBuffer.position() +
            " l: " + readBuffer.limit() + 
            " r: " + readBuffer.remaining() +
            " c: " + readBuffer.capacity();
        
        r += "\n" + "WriteBuffer: " +
            " p: " + writeBuffer.position() +
            " l: " + writeBuffer.limit() + 
            " r: " + writeBuffer.remaining() +
            " c: " + writeBuffer.capacity();

        r += "\n" + "Total read: " + totalRead + ", total written: " + totalWritten;
        return r;
    }
}
