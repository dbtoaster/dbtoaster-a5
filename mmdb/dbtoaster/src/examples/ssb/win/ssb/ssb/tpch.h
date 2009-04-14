#ifndef TPCH_H
#define TPCH_H

using namespace System;

typedef UInt64 Identifier;
typedef String^ Date;

public ref struct Lineitem {
	Identifier orderkey;
	Identifier partkey;
	Identifier suppkey;
	Int32 linenumber;
	Double quantity;
	Double extendedprice;
	Double discount;
	Double tax;
	String^ returnflag;
	String^ linestatus;
	Date shipdate;
	Date commitdate;
	Date receiptdate;
	String^ shipinstruct;
	String^ shipmode;
	String^ comment;

	Lineitem() {}

	Lineitem(Lineitem% o)
	{
		orderkey = o.orderkey;
		partkey = o.partkey;
		suppkey = o.suppkey;
		linenumber = o.linenumber;
		quantity = o.quantity;
		extendedprice = o.extendedprice;
		discount = o.discount;
		tax = o.tax;
		returnflag = o.returnflag;
		linestatus = o.linestatus;
		shipdate = o.shipdate;
		commitdate = o.commitdate;
		receiptdate = o.receiptdate;
		shipinstruct = o.shipinstruct;
		shipmode = o.shipmode;
		comment = o.comment;
	}

	String^ as_string()
	{
		array<Object^>^ fields = {
			orderkey, partkey, suppkey,
			linenumber, quantity,
			extendedprice, discount, tax,
			returnflag, linestatus,
			shipdate, commitdate, receiptdate,
			shipinstruct, shipmode, comment };

		String^ r = nullptr;
		for each ( Object^ o in fields ) {
			r = (r == nullptr? "" : r + ",") + Convert::ToString(o);
		}

		return r;
	}
};

public ref struct Order {
	Identifier orderkey;
	Identifier custkey;
	String^ orderstatus;
	Double totalprice;
	Date orderdate;
	String^ orderpriority;
	String^ clerk;
	Int32 shippriority;
	String^ comment;

	Order() {}

	Order(Order% o)
	{
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

	String^ as_string()
	{
		array<Object^>^ fields = {
			orderkey, custkey,
			orderstatus, totalprice,
			orderdate, orderpriority,
			clerk, shippriority, comment };

		String^ r = nullptr;
		for each ( Object^ o in fields ) {
			r = (r == nullptr? "" : r + ",") + Convert::ToString(o);
		}
		return r;
	}
};

ref struct Part {
	Identifier partkey;
	String^ name;
	String^ mfgr;
	String^ brand;
	String^ type;
	Int32 size;
	String^ container;
	Double retailprice;
	String^ comment;

	String^ as_string()
	{
		array<Object^>^ fields = {
			partkey, name, mfgr,
			brand, type, size,
			container, retailprice, comment
		};

		String^ r = nullptr;
		for each ( Object^ o in fields ) {
			r = (r == nullptr? "" : r + ",") + Convert::ToString(o);
		}
		return r;
	}
};

ref struct Customer {
	Identifier custkey;
	String^ name;
	String^ address;
	Identifier nationkey;
	String^ phone;
	Double acctbal;
	String^ mktsegment;
	String^ comment;

	String^ as_string()
	{
		array<Object^>^ fields = {
			custkey, name, address, nationkey,
			phone, acctbal, mktsegment, comment };

		String^ r = nullptr;
		for each ( Object^ o in fields ) {
			r = (r == nullptr? "" : r + ",") + Convert::ToString(o);
		}
		return r;
	}
};

ref struct Supplier {
	Identifier suppkey;
	String^ name;
	String^ address;
	Identifier nationkey;
	String^ phone;
	Double acctbal;
	String^ comment;

	String^ as_string()
	{
		array<Object^>^ fields = {
			suppkey, name, address, nationkey,
			phone, acctbal, comment };

		String^ r = nullptr;
		for each ( Object^ o in fields ) {
			r = (r == nullptr? "" : r + ",") + Convert::ToString(o);
		}
		return r;
	}
};

ref struct Nation {
	Identifier nationkey;
	String^ name;
	Identifier regionkey;
	String^ comment;

	Nation() {}
	Nation(Nation% o) {
		nationkey = o.nationkey;
		name = o.name;
		regionkey = o.regionkey;
		comment = o.comment;
	}

	String^ as_string()
	{
		array<Object^>^ fields = { nationkey, name, regionkey, comment };

		String^ r = nullptr;
		for each ( Object^ o in fields ) {
			r = (r == nullptr? "" : r + ",") + Convert::ToString(o);
		}
		return r;
	}
};

ref struct Region {
	Identifier regionkey;
	String^ name;
	String^ comment;

	String^ as_string()
	{
		array<Object^>^ fields = { regionkey, name, comment };

		String^ r = nullptr;
		for each ( Object^ o in fields ) {
			r = (r == nullptr? "" : r + ",") + Convert::ToString(o);
		}
		return r;
	}
};


#endif
