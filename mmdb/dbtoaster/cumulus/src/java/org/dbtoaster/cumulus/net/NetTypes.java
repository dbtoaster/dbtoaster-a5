package org.dbtoaster.cumulus.net;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.ArrayList;

public class NetTypes implements Serializable
{
    private static final long serialVersionUID = -7870435817976204250L;

    enum PutFieldType { VALUE, ENTRY, ENTRYVALUE };
    enum AggregateType { SUM, MAX, AVG };
        
    public static Long[] computePartition(Long[] key, Long[] partitionSizes)
    {
      Long[] partition = new Long[key.length];
      for(int i = 0; i < key.length; i++){
        if(partitionSizes[i] <= 1) { 
          partition[i] = Long.valueOf(0);
        } else if(key[i] < 0) {
          partition[i] = Long.valueOf(-1);
        } else { 
          partition[i] = key[i] % partitionSizes[i];
        }
      }
      return partition;
    }

    public static class Entry implements Serializable
    {
        private static final long serialVersionUID = -2903891636269280218L;
        public int source;
        public List<Long> key;
        public Entry(int s, List<Long> k) { source = s; key = k; }
        
        public boolean hasWildcards(){
          return key.contains(-1);
        }
        
        public Long[] partition(Long[] partitionSizes)
        {
          return computePartition(key.toArray(partitionSizes), partitionSizes);
        }
    }
    
    public static class PutField implements Serializable
    {
        private static final long serialVersionUID = -1097435268974958528L;
        public PutFieldType type;
        public String name;
        public Double value;
        public Entry entry;
        public PutField(PutFieldType pt, String n, Double v, Entry e)
        {
            type = pt;
            name = n;
            value = v;
            entry = e;
        }
    }
    
    public static class PutParams implements Serializable
    {
        private static final long serialVersionUID = 8494808974470927541L;
        public List<PutField> params;
        public PutParams(List<PutField> p) { params = p; }
    }
    
    public static class PutRequest implements Serializable
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
    
    public static class GetRequest implements Serializable
    {
        private static final long serialVersionUID = -5725021639950315949L;
        public InetSocketAddress target;
        public Long id_offset;
        public List<Entry> entries;
        public GetRequest() {}
        public GetRequest(InetSocketAddress t, Long i, List<Entry> e)
        {
            target = t;
            id_offset = i;
            entries = e;
        }
    }
}
