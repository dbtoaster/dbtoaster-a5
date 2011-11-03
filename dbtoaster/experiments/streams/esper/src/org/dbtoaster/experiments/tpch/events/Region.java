package org.dbtoaster.experiments.tpch.events;

public class Region {
  long regionkey;
  String name;
  String comment;

  public Region() {}

  public Region(long rk, String nm, String cm) {
    name = nm;
    regionkey = rk;
    comment = cm;
  }
  
  public Region(Region r) {
    regionkey = r.regionkey;
    name = r.name;
    comment = r.comment;
  }
  
  public long getRegionkey() { return regionkey; }
  public String getName() { return name; }
  public String getComment() { return comment; }
  
  public void setRegionkey(long rk) { regionkey = rk; }
  public void setName(String nm) { name = nm; }
  public void setComment(String cm) { comment = cm; }
  
  public String toString() { 
    return regionkey+","+name+","+comment;
  }
}
