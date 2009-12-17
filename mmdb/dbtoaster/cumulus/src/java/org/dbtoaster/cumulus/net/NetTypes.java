package org.dbtoaster.cumulus.net;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.LinkedList;

public class NetTypes implements Serializable
{
    private static final long serialVersionUID = -7870435817976204250L;

    enum PutFieldType { VALUE, ENTRY, ENTRYVALUE };
    enum AggregateType { SUM, MAX, AVG };
    
    public class NodeID implements Serializable
    {
        private static final long serialVersionUID = 1707076743312170206L;
        public InetSocketAddress id;
        public NodeID() {}
        public NodeID(InetSocketAddress i) { id = i; }
    }
    
    public class MapID implements Serializable
    {
        private static final long serialVersionUID = 4324708081367859054L;
        public Long id;
        public MapID() {}
        public MapID(Long i) { id = i; }
    }

    public class Version implements Serializable
    {
        private static final long serialVersionUID = 5734311767421788523L;
        public Long version;
        public Version() {}
        public Version(Long v) { version = v; }
    }

    public class Entry implements Serializable
    {
        private static final long serialVersionUID = -2903891636269280218L;
        public MapID source;
        public LinkedList<Long> key;
        public Entry() {}
        public Entry(MapID s, LinkedList<Long> k) { source = s; key = k; }
    }
    
    public class PutField implements Serializable
    {
        private static final long serialVersionUID = -1097435268974958528L;
        public PutFieldType type;
        public String name;
        public Double value;
        public Entry entry;
        public PutField() {}
        public PutField(PutFieldType pt, String n, Double v, Entry e)
        {
            type = pt;
            name = n;
            value = v;
            entry = e;
        }
    }
    
    public class PutParams implements Serializable
    {
        private static final long serialVersionUID = 8494808974470927541L;
        public LinkedList<PutField> params;
        public PutParams() {}
        public PutParams(LinkedList<PutField> p) { params = p; }
    }
    
    public class PutRequest implements Serializable
    {
        private static final long serialVersionUID = 569304468319713239L;
        public Long template;
        public Long id_offset;
        public Long num_gets;
        public PutRequest() {}
        public PutRequest(Long t, Long o, Long n)
        {
            template = t;
            id_offset = o;
            num_gets = n;
        }
    }
    
    public class GetRequest implements Serializable
    {
        private static final long serialVersionUID = -5725021639950315949L;
        public NodeID target;
        public Long id_offset;
        public LinkedList<Entry> entries;
        public GetRequest() {}
        public GetRequest(NodeID t, Long i, LinkedList<Entry> e)
        {
            target = t;
            id_offset = i;
            entries = e;
        }
    }
}
