package org.dbtoaster.cumulus.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class TServerTransport
{
    private Selector selector;

    private InetSocketAddress address;
    private ServerSocketChannel channel;
    
    public TServerTransport(InetSocketAddress a)
        throws IOException
    {
        address = a;
        selector = Selector.open();
        channel = ServerSocketChannel.open();
        channel.configureBlocking(false);
        channel.socket().bind(address);
    }
    
    public void registerSelector(Selector s)
    {
        try {
            selector = s;
            SelectionKey key = channel.register(selector, SelectionKey.OP_ACCEPT);
            key.attach(this);
        } catch (ClosedChannelException e) { e.printStackTrace(); }
    }
    
    void accept() throws IOException
    {
        SocketChannel socketChannel = channel.accept();
        TTransport client_transport = new TTransport(socketChannel);
        TProtocol client_protocol = new TProtocol(client_transport);
        SelectionKey key = client_transport.registerSelector(selector, SelectionKey.OP_READ);
        key.attach(client_protocol);
    }

}
