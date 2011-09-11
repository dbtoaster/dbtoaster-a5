package org.dbtoaster.experiments.tpch.events;

public class Partsupp {
  long partkey;
  long suppkey;
  int availqty;
  double supplycost;
  String comment;

  public Partsupp() {}
  
  public Partsupp(long pk, long sk, int aq, double sc, String cm) {
    partkey = pk;
    suppkey = sk;
    availqty = aq;
    supplycost = sc;
    comment = cm;
  }
  
  public Partsupp(Partsupp ps) {
    partkey = ps.partkey;
    suppkey = ps.suppkey;
    availqty = ps.availqty;
    supplycost = ps.supplycost;
    comment = ps.comment;
  }
  
  public long getPartkey() { return partkey; }
  public long getSuppkey() { return suppkey; }
  public int getAvailqty() { return availqty; }
  public double getSupplycost() { return supplycost; }
  public String getComment() { return comment; }
  
  public void setPartkey(long pk) { partkey = pk; }
  public void setSuppkey(long sk) { suppkey = sk; }
  public void setAvailqty(int aq) { availqty = aq; }
  public void setSupplycost(double sc) { supplycost = sc; }
  public void setComment(String cm) { comment = cm; }
  
  public String toString() {
    return partkey+","+suppkey+","+availqty+","+supplycost+","+comment;
  }
}
