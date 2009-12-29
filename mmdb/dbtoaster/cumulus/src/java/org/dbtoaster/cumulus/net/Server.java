package org.dbtoaster.cumulus.net;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.*;

import org.dbtoaster.cumulus.node.MapNode.MapNodeIFace;
import org.jruby.CompatVersion;
import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;

import org.apache.log4j.Logger;

public class Server implements Runnable
{
    Selector selector;
    
    TServerTransport server_transport;
    TProcessor server_processor;

    boolean terminated;
    
    Long framesProcessed;
    Long processingStartTime;
    Long lastProcessingTime;
    
    protected static final Logger logger = Logger.getLogger("dbtoaster.Net.Server");

    public Server(TProcessor processor, int port)
        throws IOException
    {
        terminated = false;
        selector = Selector.open();
        
        server_processor = processor;

        InetSocketAddress address = new InetSocketAddress(port); 
        server_transport = new TServerTransport(address);
        server_transport.registerSelector(selector);
        
        framesProcessed = 0L;
    }

    public void run()
    {
        logger.info("Starting Cumulus Server");
        // Wait for something of interest to happen
        while (!terminated)
        {
            try {
                logger.debug("Server invoking select...");
                selector.select();
    
                // Get set of ready objects
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
    
                // Walk through set
                while (keys.hasNext())
                {
                    // Get key from set
                    SelectionKey key = keys.next();
                    logger.trace("Key: " + key);
                    
                    // Remove current entry
                    keys.remove();
                    if (key.isAcceptable()) {
                        TServerTransport kt = (TServerTransport) key.attachment();
                        kt.accept();
                    } else if (key.isReadable()) {
                        TProtocol p = (TProtocol) key.attachment();
                        handleRead(p);
                    } else if (key.isWritable()) {
                        TProtocol p = (TProtocol) key.attachment();
                        handleWrite(p);
                    } else {
                        logger.debug("Ooops");
                    }
                }
                
                logger.debug("Selector keys: " + selector.keys().size());

            } catch (IOException ioe) {
                ioe.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
        // Never ends
    }
    
    void handleRead(TProtocol p) throws TException, IOException
    {
        int bytesRead = p.getTransport().read();
        if ( bytesRead > 0 )
        {
            if ( processingStartTime == null )
                processingStartTime = System.currentTimeMillis();

            logger.trace("Handle read: " + bytesRead);
            while ( p.getFrame() )
            {
                server_processor.process(p, p);
            
//                ++framesProcessed;
//                if ( (framesProcessed % 1000) == 0 ) {
//                    lastProcessingTime = System.currentTimeMillis();
//                    Long span = lastProcessingTime - processingStartTime;
//                    System.out.println("Processed " + framesProcessed +
//                        " frames: " + (span.doubleValue() / 1000));
//                   System.out.println("Protocol state: " + p.toString());
//                }
            }
        }
    }
    
    void handleWrite(TProtocol p) throws IOException
    {
        p.getTransport().write();
    }
}
