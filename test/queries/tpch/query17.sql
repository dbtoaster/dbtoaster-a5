/* Result:
    sum    
-----------
 898778.73
(1 row)
 */

CREATE STREAM LINEITEM (
        orderkey       INT,
        partkey        INT,
        suppkey        INT,
        linenumber     INT,
        quantity       DECIMAL,
        extendedprice  DECIMAL,
        discount       DECIMAL,
        tax            DECIMAL,
        returnflag     CHAR(1),
        linestatus     CHAR(1),
        shipdate       DATE,
        commitdate     DATE,
        receiptdate    DATE,
        shipinstruct   CHAR(25),
        shipmode       CHAR(10),
        comment        VARCHAR(44)
    )
  FROM FILE '../../experiments/data/tpch/lineitem.csv'
  LINE DELIMITED CSV (fields := '|');

CREATE STREAM PART (
        partkey      INT,
        name         VARCHAR(55),
        mfgr         CHAR(25),
        brand        CHAR(10),
        type         VARCHAR(25),
        size         INT,
        container    CHAR(10),
        retailprice  DECIMAL,
        comment      VARCHAR(23)
    )
  FROM FILE '../../experiments/data/tpch/part.csv'
  LINE DELIMITED CSV (fields := '|');

SELECT sum(l.extendedprice) AS query17
FROM   lineitem l, part p
WHERE  p.partkey = l.partkey
AND    l.quantity < 0.005 *
       (SELECT sum(l2.quantity)
        FROM lineitem l2 WHERE l2.partkey = p.partkey);
