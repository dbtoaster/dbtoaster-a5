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
    protected List<List<Long>> partition_values = new ArrayList<List<Long>>();
    protected List<Long> partition_sizes;
    
    public Condition(List<Long> partition_sizes){
      this.partition_sizes = partition_sizes;
    }
    
    public void addPartition(List<Long> partition){
      partition_values.add(partition);
    }
    
    public boolean match(List<Long> partition){
      for(List<Long> cmp : partition_values){
        boolean valid = true;
        for(int i = 0; i < partition.size(); i++){
          if((cmp.get(i) != null) && 
             (((long)partition.get(i) % (long)partition_sizes.get(i)) != (long)cmp.get(i))){
            valid = false;
            break;
          }
        }
        if(valid){
          return true;
        }
      }
      return false;
    }
  }
  
  public class PutComponent {
    public final Object template;
    public final long id_offset;
    public long num_gets = -1;
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

