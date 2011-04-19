package org.dbtoaster.cumulus.net;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.ArrayList;
import java.util.ListIterator;

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
    
    private static final Long[] tempLongArray = new Long[0];
    public static Long[] computePartition(List<Long> key, List<Long> partitionSizes)
    {
      return computePartition(key.toArray(tempLongArray), partitionSizes.toArray(tempLongArray));
    }
    public static Long[] computePartition(Long[] key, List<Long> partitionSizes)
    {
      return computePartition(key, partitionSizes.toArray(tempLongArray));
    }
    public static Long[] computePartition(List<Long> key, Long[] partitionSizes)
    {
      return computePartition(key.toArray(tempLongArray), partitionSizes);
    }
    
    public static class Entry implements Serializable
    {
      private static final long serialVersionUID = -2903891636269280218L;
      public int source;
      public Long[] key;
      public Entry(int s, List<Long> k) { source = s; setKey(k); }
      public Entry(int s, Long[] k) { source = s; key = k; }
      
      public void setKey(List<Long> k){
        key = new Long[k.size()];
        for(int i = 0; i < k.size(); i++){
          key[i] = k.get(i);
        }
      }
      
      public boolean hasWildcards(){
        for(Long k : key){
          if((long)k == -1){ return true; }
        }
        return false;
      }
      
      public Long[] partition(Long[] partitionSizes)
      {
        return computePartition(key, partitionSizes);
      }
      
      public String toString()
      {
        StringBuilder sb = new StringBuilder("Map " + source + "[");
        String sep = "";
        for(Long l : key){
          sb.append(sep+l);
          sep = ",";
        }
        return sb.toString() + "]";
      }
      
      public Entry toEntry(){
        return this;
      }
      
      public boolean equals(Object o){
        try {
          Entry e = (Entry)o;
          if(e.source != source)         { return false; }
          if(e.key.length != key.length) { return false; }
          for(int i = 0; i < key.length; i++){
            if(
              ((key[i] == null) && (e.key[i] != null)) ||
              ((key[i] != null) && ((long)key[i] != (long)e.key[i]))
              ) { return false; }
          }
          return true;
        } catch(ClassCastException e){
           return false;
        }
      }
      
      public String diff(Object o){
        try {
          Entry e = (Entry)o;
          if(e.source != source)               { return "A"; }
          if(e.key.length != key.length)       { return "B"; }
          for(int i = 0; i < key.length; i++){
            if(
              ((key[i] == null) && (e.key[i] != null)) ||
              ((key[i] != null) && ((long)key[i] != (long)e.key[i]))
              ) { return "C"+i; }
          }
          return "-";
        } catch(ClassCastException e){
          return "D";
        }
      }
      
      public int hashCode(){
        int hashCode = source;
        for(Long dim : key) {
          hashCode = 31 * hashCode + (dim == null ? 0 : dim.hashCode());
        }
        return hashCode;
      }
    }
    
    public static List<Long> extractNumericsFromList(List<Object> input){
      List<Long> ret = new ArrayList<Long>(input.size());
      for(Object potential : input){
        if(Number.class.isAssignableFrom(potential.getClass())){
          ret.add(((Number)potential).longValue());
        } else {
          ret.add(null);
        }
      }
      return ret;
    }
    
    public static void regularizeEntryList(List<Entry> list){
      Entry e;
      for(ListIterator<Entry> i = list.listIterator(); i.hasNext(); ){
        i.set(i.next());
      }
    }
    
    public static class ParametrizedEntry extends Entry
    {
      public String[] parameters;
      public ParametrizedEntry(int s, List<Object> k){
        super(s, extractNumericsFromList(k));
        parameters = new String[key.length];
        for(int i = 0; i < key.length; i++){
          if(key[i] == null){
            parameters[i] = k.get(i).toString();
          }
        }
      }
      
      public Entry toEntry(){
        return new Entry(source, key);
      }
      
      public String toString(){
        return super.toString() + "(Parametrized)";
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
        public PutField[] params = new PutField[0];
        public PutParams(List<PutField> p) { params = p.toArray(params); }
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
        public Entry[] entries;
        public Integer[][] replacements;
        public GetRequest() {}
        public GetRequest(InetSocketAddress t, Long ido, List<ParametrizedEntry> e_list)
        {
            target = t;
            id_offset = ido;
            entries = e_list.toArray(new Entry[0]);
            int ei = 0;
            List<Integer[]> tempReplacements = new ArrayList<Integer[]>();
            for(ParametrizedEntry e : e_list){
              int i = 0;
              for(String k : e.parameters){
                if(k != null){
                  Integer[] replacement = {ei, i, Integer.parseInt(k)};
                  tempReplacements.add(replacement);
                }
                i++;
              }
              ei++;
            }
            replacements = tempReplacements.toArray(new Integer[0][]);
        }
    }
}
