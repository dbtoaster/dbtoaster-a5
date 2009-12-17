package org.dbtoaster.cumulus.net;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dbtoaster.cumulus.net.MapNode.MapNodeIFace;
import org.dbtoaster.cumulus.net.NetTypes.*;

import org.jruby.CompatVersion;
import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;

public class MapNodeLogger implements MapNodeIFace
{
    ScriptingContainer container;
    MapNodeIFace handler;

    private final String rubyImpl = "ruby/mapnode.rb";

    public MapNodeLogger()
    {
        System.setProperty("jruby.home", "/Users/yanif/software/jruby-1.4.0");
        container = new ScriptingContainer();
        container.getProvider().getRubyInstanceConfig().
            setCompatVersion(CompatVersion.RUBY1_9);
        System.out.println("Loading ruby interface impl...");
        Object receiver = container.runScriptlet(PathType.CLASSPATH, rubyImpl);
        handler = container.getInstance(receiver, MapNodeIFace.class);
        System.out.println("Found handler..");
    }

    public Map<Entry, Double> aggreget(List<Entry> target, int agg)
        throws SpreadException, TException
    {
        return handler.aggreget(target, agg);
    }
    
    public String dump() throws TException
    {
        return handler.dump();
    }
    
    public void fetch(List<Entry> target, NodeID destination, long cmdid)
        throws TException
    {
        handler.fetch(target, destination, cmdid);
    }
    
    public Map<Entry, Double> get(List<Entry> target)
        throws SpreadException, TException
    {
        return handler.get(target);
    }
    
    public void localdump() throws TException
    {
        handler.localdump();
    }
    
    public void mass_put(long id, long template, long expected_gets,
        List<Double> params) throws TException
    {
        handler.mass_put(id, template, expected_gets, params);
    }
    
    public void meta_request(long base_cmd, List<PutRequest> put_list,
        List<GetRequest> get_list, List<Double> params) throws TException
    {
        handler.meta_request(base_cmd, put_list, get_list, params);
    }
    
    public void push_get(Map<Entry, Double> result, long cmdid)
        throws TException
    {
        handler.push_get(result, cmdid);
    }
    
    public void put(long id, long template, List<Double> params)
        throws TException
    {
        handler.put(id, template, params);
    }
}
