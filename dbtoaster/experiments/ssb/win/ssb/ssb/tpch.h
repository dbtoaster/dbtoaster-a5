#ifndef TPCH_H
#define TPCH_H

using namespace System;

typedef UInt64 Identifier;
typedef String^ Date;

#pragma pack(1)

public value struct Lineitem {
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

	/*
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
	*/

	Int32 getSize()
	{
		return 92 +
			returnflag->Length + linestatus->Length +
			shipdate->Length + commitdate->Length +
			receiptdate->Length + shipinstruct->Length +
			shipmode->Length + comment->Length;
	}

	bool operator==(Lineitem% o)
	{
		return orderkey == o.orderkey &&
			partkey == o.partkey &&
			suppkey == o.suppkey;
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

public value struct Order {
	Identifier orderkey;
	Identifier custkey;
	String^ orderstatus;
	Double totalprice;
	Date orderdate;
	String^ orderpriority;
	String^ clerk;
	Int32 shippriority;
	String^ comment;

	/*
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
	*/

	bool operator==(Order% o) { return orderkey == o.orderkey; }

	Int32 getSize()
	{
		return 48 +
			orderstatus->Length + orderdate->Length +
			orderpriority->Length + clerk->Length + comment->Length;
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

value struct Part {
	Identifier partkey;
	String^ name;
	String^ mfgr;
	String^ brand;
	String^ type;
	Int32 size;
	String^ container;
	Double retailprice;
	String^ comment;

	bool operator==(Part% o) { return partkey == o.partkey; }

	Int32 getSize()
	{
		return 44 +
			name->Length + mfgr->Length +
			brand->Length + type->Length + container->Length +
			comment->Length;
	}

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

value struct Customer {
	Identifier custkey;
	String^ name;
	String^ address;
	Identifier nationkey;
	String^ phone;
	Double acctbal;
	String^ mktsegment;
	String^ comment;

	bool operator==(Customer% o) { return custkey == o.custkey; }

	Int32 getSize()
	{
		return 44 +
			name->Length + address->Length +
			phone->Length + mktsegment->Length + comment->Length;
	}

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

value struct Supplier {
	Identifier suppkey;
	String^ name;
	String^ address;
	Identifier nationkey;
	String^ phone;
	Double acctbal;
	String^ comment;

	bool operator==(Supplier% o) { return suppkey == o.suppkey; }

	Int32 getSize()
	{
		return 40 +
			name->Length + address->Length +
			phone->Length + comment->Length;
	}

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

value struct Nation {
	Identifier nationkey;
	String^ name;
	Identifier regionkey;
	String^ comment;

	/*
	Nation() {}
	Nation(Nation% o) {
		nationkey = o.nationkey;
		name = o.name;
		regionkey = o.regionkey;
		comment = o.comment;
	}
	*/

	bool operator==(Nation% o) { return nationkey == o.nationkey; }

	Int32 getSize()
	{
		return 24 + name->Length + comment->Length;
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

value struct Region {
	Identifier regionkey;
	String^ name;
	String^ comment;

	bool operator==(Region% o) { return regionkey == o.regionkey; }

	Int32 getSize()
	{
		return 16 + name->Length + comment->Length;
	}

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

#pragma pack()

#endif
