package org.dbtoaster.experiments.tpch.events;

public class Customer {

  long custkey;
  String name;
  String address;
  long nationkey;
  String phone;
  double acctbal;
  String mktsegment;
  String comment;

  public Customer() {}
  
  public Customer(long ck, String nm, String ad, long nk,
                  String ph, double ab, String ms, String cm)
  { 
    custkey = ck;
    name = nm;
    address = ad;
    nationkey = nk;
    phone = ph;
    acctbal = ab;
    mktsegment = ms;
    comment = cm;
  }

  public Customer(Customer c) {
    custkey = c.custkey;
    name = c.name;
    address = c.address;
    nationkey = c.nationkey;
    phone = c.phone;
    acctbal = c.acctbal;
    mktsegment = c.mktsegment;
    comment = c.comment;
  }
  
  public long getCustkey () { return custkey; }
  public String getName() { return name; }
  public String getAddress() { return address; }
  public long getNationkey() { return nationkey; }
  public String getPhone() { return phone; }
  public double getAcctbal() { return acctbal; }
  public String getMktsegment() { return mktsegment; }
  public String getComment() { return comment; }
  
  public void setCustkey(long ck) { custkey = ck; }
  public void setName(String nm) { name = nm; }
  public void setAddress(String addr) { address = addr; }
  public void setNationkey(long nk) { nationkey = nk; }
  public void setPhone(String ph) { phone = ph; }
  public void setAcctbal(double ab) { acctbal = ab; }
  public void setMktsegment(String mk) { mktsegment = mk; }
  public void setComment(String cm) { comment = cm; }
  
  public String toString() {
    return custkey+","+name+","+address+","+nationkey+","+
           phone+","+acctbal+","+mktsegment+","+comment;
  }
}
