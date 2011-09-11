package org.dbtoaster.experiments.tpch.events;

public class Nation {

  long nationkey;
  String name;
  long regionkey;
  String comment;

  public Nation() {}

  public Nation(long nk, String nm, long rk, String cm) {
    nationkey = nk;
    name = nm;
    regionkey = rk;
    comment = cm;
  }
  
  public Nation(Nation n) {
    nationkey = n.nationkey;
    name = n.name;
    regionkey = n.regionkey;
    comment = n.comment;
  }
  
  public long getNationkey() { return nationkey; }
  public String getName() { return name; }
  public long getRegionkey() { return regionkey; }
  public String getComment() { return comment; }
  
  public void setNationkey(long nk) { nationkey = nk; }
  public void setName(String nm) { name = nm; }
  public void setRegionkey(long rk) { regionkey = rk; }
  public void setComment(String cm) { comment = cm; }
  
  public String toString() { 
    return nationkey+","+name+","+regionkey+","+comment;
  }
}
