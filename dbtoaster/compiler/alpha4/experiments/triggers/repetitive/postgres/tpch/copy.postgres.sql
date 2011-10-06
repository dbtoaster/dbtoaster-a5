-- Loads data into tables required by TPCH queries.

COPY CUSTOMER_STREAM    FROM '@@PATH@@/customer.tbl' WITH DELIMITER '|';
COPY LINEITEM_STREAM    FROM '@@PATH@@/lineitem.tbl' WITH DELIMITER '|';
COPY NATION_STREAM      FROM '@@PATH@@/nation.tbl'   WITH DELIMITER '|';
COPY ORDERS_STREAM      FROM '@@PATH@@/orders.tbl'   WITH DELIMITER '|';
COPY PART_STREAM        FROM '@@PATH@@/part.tbl'     WITH DELIMITER '|';
COPY PARTSUPP_STREAM    FROM '@@PATH@@/partsupp.tbl' WITH DELIMITER '|';
COPY REGION_STREAM      FROM '@@PATH@@/region.tbl'   WITH DELIMITER '|';
COPY SUPPLIER_STREAM    FROM '@@PATH@@/supplier.tbl' WITH DELIMITER '|';
