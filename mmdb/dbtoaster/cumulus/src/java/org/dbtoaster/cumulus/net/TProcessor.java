package org.dbtoaster.cumulus.net;

public interface TProcessor
{
    public boolean process(TProtocol in, TProtocol out)
        throws TException;
}
