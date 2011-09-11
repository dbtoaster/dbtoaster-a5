package org.dbtoaster.experiments.tpch.events;

public class Supplier {

  long suppkey;
  String name;
  String address;
  long nationkey;
  String phone;
  double acctbal;
  String comment;

  public Supplier() {}
  
  public Supplier(long sk, String nm, String ad, long nk,
                  String ph, double ab, String cm)
  {
    suppkey = sk;
    name = nm;
    address = ad;
    nationkey = nk;
    phone = ph;
    acctbal = ab;
    comment = cm;
  }
  
  public Supplier(Supplier s) {
    suppkey = s.suppkey;
    name = s.name;
    address = s.address;
    nationkey = s.nationkey;
    phone = s.phone;
    acctbal = s.acctbal;
    comment = s.comment;
  }
  
  public long getSuppkey() { return suppkey; } 
  public String getName() { return name; }
  public String getAddress() { return address; }
  public long getNationkey() { return nationkey; }
  public String getPhone() { return phone; }
  public double getAcctbal() { return acctbal; }
  public String getComment() { return comment; }
  
  public void setSuppkey(long sk) { suppkey = sk; }
  public void setName(String nm) { name = nm; }
  public void setAddress(String addr) { address = addr; }
  public void setNationkey(long nk) { nationkey = nk; }
  public void setPhone(String ph) { phone = ph; }
  public void setAcctbal(double ab) { acctbal = ab; }
  public void setComment(String cm) { comment = cm; }
  
  public String toString() {
    return suppkey+","+name+","+address+","+nationkey+","+
           phone+","+acctbal+","+comment;
  }
}
