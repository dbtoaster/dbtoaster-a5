package org.dbtoaster.experiments.tpch.events;

public class Part {

  long partkey;
  String name;
  String mfgr;
  String brand;
  String type;
  int size;
  String container;
  double retailprice;
  String comment;

  public Part() {}
  
  public Part(long pk, String nm, String mf, String br, String ty,
              int sz, String ct, double rp, String cm)
  {
    partkey = pk;
    name = nm;
    mfgr = mf;
    brand = br;
    type = ty;
    size = sz;
    container = ct;
    retailprice = rp;
    comment = cm;
  }
  
  public Part(Part p) {
    partkey = p.partkey;
    name = p.name;
    mfgr = p.mfgr;
    brand = p.brand;
    type = p.type;
    size = p.size;
    container = p.container;
    retailprice = p.retailprice;
    comment = p.comment;
  }

  public long getPartkey() { return partkey; }
  public String getName() { return name; }
  public String getMfgr() { return mfgr; }
  public String getBrand() { return brand; }
  public String getType() { return type; }
  public int getSize() { return size; }
  public String getContainer() { return container; }
  public double getRetailprice() { return retailprice; }
  public String getComment() { return comment; }
  
  public void setPartkey(long pk) { partkey = pk; }
  public void setName(String nm) { name = nm; }
  public void setMfgr(String mf) { mfgr = mf; }
  public void setBrand(String br) { brand = br; }
  public void setType(String ty) { type = ty; }
  public void setSize(int sz) { size = sz; }
  public void setContainer(String ct) { container = ct; }
  public void setRetailprice(double rp) { retailprice = rp; }
  public void setComment(String cm) { comment = cm; }
  
  public String toString() {
    return partkey+","+name+","+mfgr+","+brand+","+type+","+size+","+
           container+","+retailprice+","+comment;
  }
}
