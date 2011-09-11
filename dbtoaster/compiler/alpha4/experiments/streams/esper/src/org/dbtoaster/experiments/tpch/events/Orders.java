package org.dbtoaster.experiments.tpch.events;

public class Orders {

  long   orderkey;
  long   custkey;
  String orderstatus;
  double totalprice;
  String orderdate;      // date
  String orderpriority;
  String clerk;
  int    shippriority;
  String comment;
  
  public Orders() {}
  
  public Orders(Orders o) {
    orderkey = o.orderkey;
    custkey = o.custkey;
    orderstatus = o.orderstatus;
    totalprice = o.totalprice;
    orderdate = o.orderdate;
    orderpriority = o.orderpriority;
    clerk = o.clerk;
    shippriority = o.shippriority;
    comment = o.comment;
  }
  
  public Orders(long ok, long ck, String os,
                double tp, String od, String op,
                String cl, int sp, String cm)
  {
    orderkey = ok;
    custkey = ck;
    orderstatus = os;
    totalprice = tp;
    orderdate = od;
    orderpriority = op;
    clerk = cl;
    shippriority = sp;
    comment = cm;
  }
  
  public long getOrderkey() { return orderkey; }
  public long getCustkey() { return custkey; }
  public String getOrderstatus() { return orderstatus; }
  public double getTotalprice() { return totalprice; }
  public String getOrderdate() { return orderdate; }
  public String getOrderpriority() { return orderpriority; }
  public String getClerk() { return clerk; }
  public int getShippriority() { return shippriority; }
  public String getComment() { return comment; }
  
  public void setOrderkey(long ok) { orderkey = ok; }
  public void setCustkey(long ck) { custkey = ck; }
  public void setOrderstatus(String os) { orderstatus = os; }
  public void setTotalprice(double tp) { totalprice = tp; }
  public void setOrderdate(String od) { orderdate = od; }
  public void setOrderpriority(String op) { orderpriority = op; }
  public void setClerk(String cl) { clerk = cl; }
  public void setShippriority(int sp) { shippriority = sp; }
  public void setComment(String cm) { comment = cm; }
  
  public String toString() {
    return orderkey+","+custkey+","+orderstatus+","+totalprice+","+
           orderdate+","+orderpriority+","+clerk+","+shippriority+","+comment;
  }
}
