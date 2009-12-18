package org.dbtoaster.cumulus.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class TTransport
{
    private Selector selector;

    private InetSocketAddress server;
    private SocketChannel channel;
    
    private SelectionKey key;
    
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    
    private Long totalRead;
    private Long totalWritten;
    
    private final int readBufferSize = 32*1024; 
    private final int writeBufferSize = 32*1024;
    private final int socketSendBufferSize = 32*1024;
    private final int socketRecvBufferSize = 32*1024;

    public TTransport(InetSocketAddress s)
    {
        server = s;
        readBuffer = ByteBuffer.allocate(readBufferSize);
        writeBuffer = ByteBuffer.allocate(writeBufferSize);
        
        totalRead = 0L;
        totalWritten = 0L;
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
            System.out.println("Connected to " + server.toString());
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
        //System.out.println("TTransport read: " + bytesRead + ", " + toString());
        return bytesRead;
    }

    public int write() throws IOException
    {
        int bytesWritten = 0;
        synchronized (writeBuffer)
        {
            writeBuffer = (ByteBuffer) writeBuffer.flip();
            bytesWritten = channel.write(writeBuffer);
            if ( writeBuffer.hasRemaining() )
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            else
                key.interestOps(key.interestOps() ^ SelectionKey.OP_WRITE);
            writeBuffer.compact();
        }
        totalWritten += bytesWritten;
        //System.out.println("Transport wrote: " + bytesWritten + ", " + toString());
        return bytesWritten;
    }

    public int send(byte[] buf, int off, int len)
    {
        int bytesSent = len;
        synchronized(writeBuffer)
        {
            //System.out.println("Sending: " + len);
            try {
                writeBuffer.mark();
                writeBuffer.putInt(len);
                writeBuffer.put(ByteBuffer.wrap(buf, off, len));
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                selector.wakeup();
            } catch (BufferOverflowException e) {
                writeBuffer.reset();
                bytesSent = 0;
                //System.out.println("Buffer overflow, req: " + len + ", " + toString());
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
            
            //System.out.println("TTransport recv, " + " br: " + bytesRecv + ", " + toString());

            if ( bytesRecv > 0 )
            {
                readBuffer.flip();
                try {
                    readBuffer.get(buf, off, len);
                    readBuffer.compact();
                } catch (BufferUnderflowException e) {
                    bytesRecv = 0;
                    readBuffer.compact();
                    //System.out.println("Buffer underflow, req: " + len + ",  "+ toString());
                    //e.printStackTrace();
                }
            }
            
            if ( key.isValid() )
            {
                if ( bytesRecv < len ) {
                    //System.out.println("Adding key " + key + " for read.");
                    key.interestOps(key.interestOps() | SelectionKey.OP_READ);                    
                }

                else {
                    //System.out.println("Removing key " + key + " for read.");
                    key.interestOps(key.interestOps() ^ SelectionKey.OP_READ);
                }

                selector.wakeup();
            }
            else {
                System.out.println("Invalid key: " + key);
            }
        }
        
        return Math.min(bytesRecv, len);
    }

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
