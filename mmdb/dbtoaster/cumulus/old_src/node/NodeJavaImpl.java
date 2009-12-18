package node;

import java.io.*;
import java.util.*;

public class NodeJavaImpl implements MapNode.Iface {
  
  public NodeJavaImpl(){
    
  }

  public void put(long id, long template, List<Double> params) throws TException {
    
  }

  public void mass_put(long id, long template, long expected_gets, List<Double> params) throws TException {
    
  }

  public Map<MapEntry,Double> get(List<MapEntry> target) throws SpreadException, TException {
    
  }

  public void fetch(List<MapEntry> target, NodeID destination, long cmdid) throws TException {
    
  }

  public void push_get(Map<MapEntry,Double> result, long cmdid) throws TException {
    
  }

  public void meta_request(long base_cmd, List<PutRequest> put_list, List<GetRequest> get_list, List<Double> params) throws TException {
    
  }

  public Map<MapEntry,Double> aggreget(List<MapEntry> target, AggregateType agg) throws SpreadException, TException {
    
  }

  public String dump() throws TException {
    
  }

  public void localdump() throws TException {
    
  }
}