package org.dbtoaster.experiments.tpch.events;

public class Lineitem {
  long orderkey;
  long partkey;
  long suppkey;
  int linenumber;
  double quantity;
  double extendedprice;
  double discount;
  double tax;
  String returnflag;
  String linestatus;
  String shipdate;            // date
  String commitdate;          // date
  String receiptdate;         // date
  String shipinstruct;
  String shipmode;
  String comment;

  public Lineitem() {}
  
  public Lineitem(Lineitem l) {
    orderkey = l.orderkey;
    partkey = l.partkey;
    suppkey = l.suppkey;
    linenumber = l.linenumber;
    quantity = l.quantity;
    extendedprice = l.extendedprice;
    discount = l.discount;
    tax = l.tax;
    returnflag = l.returnflag;
    linestatus = l.linestatus;
    shipdate = l.shipdate;
    commitdate = l.commitdate;
    receiptdate = l.receiptdate;
    shipinstruct = l.shipinstruct;
    shipmode = l.shipmode;
    comment = l.comment;
  }

  public Lineitem(long ok, long pk, long sk, int ln,
                  double q, double ep, double di, double tx,
                  String rf, String ls, String sd, String cd, String rd,
                  String sins, String sm, String cm)
  {
    orderkey = ok;
    partkey = pk;
    suppkey = sk;
    linenumber = ln;
    quantity = q;
    extendedprice = ep;
    discount = di;
    tax = tx;
    returnflag = rf;
    linestatus = ls;
    shipdate = sd;
    commitdate = cd;
    receiptdate = rd;
    shipinstruct = sins;
    shipmode = sm;
    comment = cm;
  }
  
  public long getOrderkey() { return orderkey; }
  public long getPartkey() { return partkey; }
  public long getSuppkey() { return suppkey; }
  public int getLinenumber() { return linenumber; }
  public double getQuantity() { return quantity; }
  public double getExtendedprice() { return extendedprice; }
  public double getDiscount() { return discount; }
  public double getTax() { return tax; }
  public String getReturnflag() { return returnflag; }
  public String getLinestatus() { return linestatus; }
  public String getShipdate() { return shipdate; }
  public String getCommitdate() { return commitdate; }
  public String getReceiptdate() { return receiptdate; }
  public String getShipinstruct() { return shipinstruct; }
  public String getShipmode() { return shipmode; }
  public String getComment() { return comment; }
  
  public void setOrderkey(long ok) { orderkey = ok; }
  public void setPartkey(long pk) { partkey = pk; }
  public void setSuppkey(long sk) { suppkey = sk; }
  public void setLinenumber(int ln) { linenumber = ln; }
  public void setQuantity(double q) { quantity = q; }
  public void setExtendedprice(double ep) { extendedprice = ep; }
  public void setDiscount(double di) { discount = di; }
  public void setTax(double tx) { tax = tx; }
  public void setReturnflag(String rf) { returnflag = rf; }
  public void setLinestatus(String ls) { linestatus = ls; }
  public void setShipdate(String sd) { shipdate = sd; }
  public void setCommitdate(String cd) { commitdate = cd; }
  public void setReceiptdate(String rd) { receiptdate = rd; }

  public void setShipinstruct(String si) { shipinstruct = si; }
  public void setShipmode(String sm) { shipmode = sm; }
  public void setComment(String cm) { comment = cm; }
  
  public String toString() { 
    return orderkey+","+partkey+","+suppkey+linenumber+","+
           quantity+","+extendedprice+","+discount+","+tax+","+
           returnflag+","+linestatus+","+
           shipdate+","+commitdate+","+receiptdate+","+
           shipinstruct+","+shipmode+","+comment;
  }
}
