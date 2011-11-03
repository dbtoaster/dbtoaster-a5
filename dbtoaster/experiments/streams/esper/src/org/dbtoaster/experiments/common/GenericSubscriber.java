package org.dbtoaster.experiments.common;

import java.util.Map;

public class GenericSubscriber {
  int resultCount, resultStep;
  
  public GenericSubscriber() { resultCount = 0; resultStep = 100; }
  
  public GenericSubscriber(int step) { resultCount = 0; resultStep = step; }
  
  public void update(Map<?,?> ins) {
    if ( resultCount % resultStep == 0 ) {
      System.out.println("istream: "+ins);
    }
    ++resultCount;
  }
  
  public void updateRStream(Map<?,?> del) {
    System.out.println("rstream: "+del);
  }
}
