package org.dbtoaster.experiments.tpch.events;

public class UnifiedEvent {
  String streamname;
  int event;
  double acctbal;
  int address;
  int availqty;
  int brand;
  int clerk;
  int comment;
  int commitdate;
  int container;
  int custkey;
  double discount;
  double extendedprice;
  int linenumber;
  int linestatus;
  int mfgr;
  int mktsegment;
  int name;
  int nationkey;
  int orderdate;
  int orderkey;
  int orderpriority;
  int orderstatus;
  int partkey;
  int phone;
  double quantity;
  int receiptdate;
  int regionkey;
  double retailprice;
  int returnflag;
  int shipdate;
  int shipinstruct;
  int shipmode;
  int shippriority;
  int size;
  int suppkey;
  double supplycost;
  double tax;
  double totalprice;
  int type;

  public UnifiedEvent() {}

  public UnifiedEvent(String streamname, int event, double acctbal,
      int address, int availqty, int brand, int clerk, int comment,
      int commitdate, int container, int custkey, double discount,
      double extendedprice, int linenumber, int linestatus, int mfgr,
      int mktsegment, int name, int nationkey, int orderdate, int orderkey,
      int orderpriority, int orderstatus, int partkey, int phone,
      double quantity, int receiptdate, int regionkey, double retailprice,
      int returnflag, int shipdate, int shipinstruct, int shipmode,
      int shippriority, int size, int suppkey, double supplycost, double tax,
      double totalprice, int type)
  {
    this.streamname = streamname;
    this.event = event;
    this.acctbal = acctbal;
    this.address = address;
    this.availqty = availqty;
    this.brand = brand;
    this.clerk = clerk;
    this.comment = comment;
    this.commitdate = commitdate;
    this.container = container;
    this.custkey = custkey;
    this.discount = discount;
    this.extendedprice = extendedprice;
    this.linenumber = linenumber;
    this.linestatus = linestatus;
    this.mfgr = mfgr;
    this.mktsegment = mktsegment;
    this.name = name;
    this.nationkey = nationkey;
    this.orderdate = orderdate;
    this.orderkey = orderkey;
    this.orderpriority = orderpriority;
    this.orderstatus = orderstatus;
    this.partkey = partkey;
    this.phone = phone;
    this.quantity = quantity;
    this.receiptdate = receiptdate;
    this.regionkey = regionkey;
    this.retailprice = retailprice;
    this.returnflag = returnflag;
    this.shipdate = shipdate;
    this.shipinstruct = shipinstruct;
    this.shipmode = shipmode;
    this.shippriority = shippriority;
    this.size = size;
    this.suppkey = suppkey;
    this.supplycost = supplycost;
    this.tax = tax;
    this.totalprice = totalprice;
    this.type = type;
  }

  public UnifiedEvent(UnifiedEvent e) {
    this.streamname = e.streamname;
    this.event = e.event;
    this.acctbal = e.acctbal;
    this.address = e.address;
    this.availqty = e.availqty;
    this.brand = e.brand;
    this.clerk = e.clerk;
    this.comment = e.comment;
    this.commitdate = e.commitdate;
    this.container = e.container;
    this.custkey = e.custkey;
    this.discount = e.discount;
    this.extendedprice = e.extendedprice;
    this.linenumber = e.linenumber;
    this.linestatus = e.linestatus;
    this.mfgr = e.mfgr;
    this.mktsegment = e.mktsegment;
    this.name = e.name;
    this.nationkey = e.nationkey;
    this.orderdate = e.orderdate;
    this.orderkey = e.orderkey;
    this.orderpriority = e.orderpriority;
    this.orderstatus = e.orderstatus;
    this.partkey = e.partkey;
    this.phone = e.phone;
    this.quantity = e.quantity;
    this.receiptdate = e.receiptdate;
    this.regionkey = e.regionkey;
    this.retailprice = e.retailprice;
    this.returnflag = e.returnflag;
    this.shipdate = e.shipdate;
    this.shipinstruct = e.shipinstruct;
    this.shipmode = e.shipmode;
    this.shippriority = e.shippriority;
    this.size = e.size;
    this.suppkey = e.suppkey;
    this.supplycost = e.supplycost;
    this.tax = e.tax;
    this.totalprice = e.totalprice;
    this.type = e.type;
  }

  public String getStreamname() { return streamname; }
  public void setStreamname(String streamname) { this.streamname = streamname; }
  
  public int getEvent() { return event;}
  public void setEvent(int event) { this.event = event; }
  
  public double getAcctbal() { return acctbal; }
  public void setAcctbal(double acctbal) { this.acctbal = acctbal; }
  
  public int getAddress() { return address; }
  public void setAddress(int address) { this.address = address; }
  
  public int getAvailqty() { return availqty; }
  public void setAvailqty(int availqty) { this.availqty = availqty; }
  
  public int getBrand() { return brand; }
  public void setBrand(int brand) { this.brand = brand; }
  
  public int getClerk() { return clerk; }
  public void setClerk(int clerk) { this.clerk = clerk; }
  
  public int getComment() { return comment; }
  public void setComment(int comment) { this.comment = comment; }
  
  public int getCommitdate() { return commitdate; }
  public void setCommitdate(int commitdate) { this.commitdate = commitdate; }
  
  public int getContainer() { return container; }
  public void setContainer(int container) { this.container = container; }
  
  public int getCustkey() { return custkey; }
  public void setCustkey(int custkey) { this.custkey = custkey; }
  
  public double getDiscount() { return discount; }
  public void setDiscount(double discount) { this.discount = discount; }
  
  public double getExtendedprice() { return extendedprice; }
  public void setExtendedprice(double extendedprice) { this.extendedprice = extendedprice; }
  
  public int getLinenumber() { return linenumber; }
  public void setLinenumber(int linenumber) { this.linenumber = linenumber; }

  public int getLinestatus() { return linestatus; }
  public void setLinestatus(int linestatus) { this.linestatus = linestatus; }
  
  public int getMfgr() { return mfgr; }
  public void setMfgr(int mfgr) { this.mfgr = mfgr; }
  
  public int getMktsegment() { return mktsegment; }
  public void setMktsegment(int mktsegment) { this.mktsegment = mktsegment; }
  
  public int getName() { return name; }
  public void setName(int name) { this.name = name; }
  
  public int getNationkey() { return nationkey; }
  public void setNationkey(int nationkey) { this.nationkey = nationkey; }
  
  public int getOrderdate() { return orderdate; }
  public void setOrderdate(int orderdate) { this.orderdate = orderdate; }
  
  public int getOrderkey() { return orderkey; }
  public void setOrderkey(int orderkey) { this.orderkey = orderkey; }
  
  public int getOrderpriority() { return orderpriority;}
  public void setOrderpriority(int orderpriority) { this.orderpriority = orderpriority; }
  
  public int getOrderstatus() { return orderstatus; }
  public void setOrderstatus(int orderstatus) { this.orderstatus = orderstatus; }
  
  public int getPartkey() { return partkey; }
  public void setPartkey(int partkey) { this.partkey = partkey; }
  
  public int getPhone() { return phone; }
  public void setPhone(int phone) { this.phone = phone; }
  
  public double getQuantity() { return quantity; }
  public void setQuantity(double quantity) { this.quantity = quantity; }
  
  public int getReceiptdate() { return receiptdate; }
  public void setReceiptdate(int receiptdate) { this.receiptdate = receiptdate; }
  
  public int getRegionkey() { return regionkey; }
  public void setRegionkey(int regionkey) { this.regionkey = regionkey; }
  
  public double getRetailprice() { return retailprice; }
  public void setRetailprice(double retailprice) { this.retailprice = retailprice; }
  
  public int getReturnflag() { return returnflag; }
  public void setReturnflag(int returnflag) { this.returnflag = returnflag; }
  
  public int getShipdate() { return shipdate; }
  public void setShipdate(int shipdate) { this.shipdate = shipdate; }
  
  public int getShipinstruct() { return shipinstruct; }
  public void setShipinstruct(int shipinstruct) { this.shipinstruct = shipinstruct; }
  
  public int getShipmode() { return shipmode; }
  public void setShipmode(int shipmode) { this.shipmode = shipmode; }
  
  public int getShippriority() { return shippriority; }
  public void setShippriority(int shippriority) { this.shippriority = shippriority; }
  
  public int getSize() { return size; }
  public void setSize(int size) { this.size = size; }
  
  public int getSuppkey() { return suppkey; }
  public void setSuppkey(int suppkey) { this.suppkey = suppkey; }
  
  public double getSupplycost() { return supplycost; }
  public void setSupplycost(double supplycost) { this.supplycost = supplycost; }
  
  public double getTax() { return tax; }
  public void setTax(double tax) { this.tax = tax; }
  
  public double getTotalprice() { return totalprice; }
  public void setTotalprice(double totalprice) { this.totalprice = totalprice; }
  
  public int getType() { return type; }
  public void setType(int type) { this.type = type; }

  @Override
  public String toString() {
    return "UnifiedEvent [streamname=" + streamname + ", event=" + event
        + ", acctbal=" + acctbal + ", address=" + address + ", availqty="
        + availqty + ", brand=" + brand + ", clerk=" + clerk + ", comment="
        + comment + ", commitdate=" + commitdate + ", container=" + container
        + ", custkey=" + custkey + ", discount=" + discount
        + ", extendedprice=" + extendedprice + ", linenumber=" + linenumber
        + ", linestatus=" + linestatus + ", mfgr=" + mfgr + ", mktsegment="
        + mktsegment + ", name=" + name + ", nationkey=" + nationkey
        + ", orderdate=" + orderdate + ", orderkey=" + orderkey
        + ", orderpriority=" + orderpriority + ", orderstatus=" + orderstatus
        + ", partkey=" + partkey + ", phone=" + phone + ", quantity="
        + quantity + ", receiptdate=" + receiptdate + ", regionkey="
        + regionkey + ", retailprice=" + retailprice + ", returnflag="
        + returnflag + ", shipdate=" + shipdate + ", shipinstruct="
        + shipinstruct + ", shipmode=" + shipmode + ", shippriority="
        + shippriority + ", size=" + size + ", suppkey=" + suppkey
        + ", supplycost=" + supplycost + ", tax=" + tax + ", totalprice="
        + totalprice + ", type=" + type + "]";
  }

}

