package org.dbtoaster.cumulus.config;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.net.InetSocketAddress;

public class CompiledM3Program
{
  protected HashMap<String,RelationProgramComponent> localPrograms = 
    new HashMap<String,RelationProgramComponent>();
  
  public CompiledM3Program(){
  }
  
  public PutComponent installPutComponent(String relation, Object template, int id_offset, List<Long> partition_sizes){
    PutComponent ret = new PutComponent(template, id_offset, partition_sizes);
    getRelationComponent(relation).puts.add(ret);
    return ret;
  }
  
  public FetchComponent installFetchComponent(String relation, Object entry, int id_offset, Object target, List<Long> partition_sizes, Object entry_mapping, Object target_partitions){
    FetchComponent ret = new FetchComponent(entry, id_offset, target, partition_sizes, entry_mapping, target_partitions);
    getRelationComponent(relation).fetches.add(ret);
    return ret;
  }
  
  public RelationProgramComponent getRelationComponent(String relation){
    RelationProgramComponent ret = localPrograms.get(relation);
    if(ret == null){
      ret = new RelationProgramComponent();
      localPrograms.put(relation, ret);
    }
    return ret;
  }
  
  public class RelationProgramComponent {
    public ArrayList<PutComponent> puts = new ArrayList<PutComponent>();
    public ArrayList<FetchComponent> fetches = new ArrayList<FetchComponent>();
    
    public RelationProgramComponent(){
    }
  }
  
  public class Condition {
    protected List<ConditionComparator> partition_values = new ArrayList<ConditionComparator>();
    protected List<Long> partition_sizes;
    
    public void checkClassCast(List<Long> list) {
      long foo;
      for(Long entry : list){
        try {
          foo = (long)entry;
        } catch (NullPointerException e){}
      }
    }
    
    public Condition(List<Long> partition_sizes){
      this.partition_sizes = partition_sizes;
      checkClassCast(partition_sizes);
    }
    
    public void addPartition(List<Long> partition){
      addPartition(partition, new Boolean(true));
    }
    
    public void addPartition(List<Long> partition, Object reference){
      partition_values.add(new ConditionComparator(partition, reference));
      checkClassCast(partition);
    }
    
    public Object match(List<Double> partition){
      for(ConditionComparator cmp : partition_values){
        Object ret = cmp.match(partition);
        if(ret != null) { return ret; }
      }
      return null;
    }
  
    public class ConditionComparator {
      protected List<Long> cmp;
      protected Object reference;
      
      public ConditionComparator(List<Long> cmp, Object reference){
        this.cmp = cmp;
        this.reference = reference;
      }
      
      public Object match(List<Double> partition){
        for(int i = 0; i < partition.size(); i++){
          if(cmp.get(i) != null){
            if(((long)((double)partition.get(i)) % (long)partition_sizes.get(i)) != (long)cmp.get(i)){
              return null;
            }
          }
        }
        return reference;
      }
    }
  }
  
  public class PutComponent {
    public final Object template;
    public final long id_offset;
    public Condition condition;
    
    public PutComponent(Object template, long id_offset, List<Long> partition_sizes){
      this.template = template;
      this.id_offset = id_offset;
      this.condition = new Condition(partition_sizes);
    }
  }
  
  public class FetchComponent {
    public final Object entry;
    public final long id_offset;
    public final Object target;
    public final Object entry_mapping;
    public final Object target_partitions;
    public Condition condition;
    
    public FetchComponent(Object entry, long id_offset, Object target, List<Long> partition_sizes, Object entry_mapping, Object target_partitions){
      this.entry = entry;
      this.id_offset = id_offset;
      this.target = target;
      this.entry_mapping = entry_mapping;
      this.target_partitions = target_partitions;
      this.condition = new Condition(partition_sizes);
    }
  }
}

