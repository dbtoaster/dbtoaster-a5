package org.dbtoaster.experiments.clustermgmt.events;

public class UnifiedEvent {
  private int eventType;
  private int rackid;
  private double load;

  public UnifiedEvent() {}
  
  public UnifiedEvent(UnifiedEvent e) {
    eventType = e.eventType;
    rackid = e.rackid;
    load = e.load;
  }
  
  public UnifiedEvent(int et, int rid, double l) {
    eventType = et;
    rackid = rid;
    load = l;
  }
  
  public int getEventType() { return eventType; }
  public int getRackid() { return rackid; }
  public double getLoad() { return load; }
  
  public void setEventType(int et) { eventType = et; }
  public void setRackid(int rid) { rackid = rid; }
  public void setLoad(double l) { load = l; }
  
  public String toString() {
    return eventType+","+rackid+","+load;
  }

}
