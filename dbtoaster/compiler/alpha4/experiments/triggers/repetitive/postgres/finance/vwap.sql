DROP TABLE IF EXISTS InsertBids;
CREATE TABLE InsertBids(t float, order_id float, broker_id float, volume float, price float);

DROP TABLE IF EXISTS DeleteBids;
CREATE TABLE DeleteBids(t float, order_id float, broker_id float, volume float, price float);

DROP TABLE IF EXISTS Bids;
CREATE TABLE Bids(volume float, price float);

DROP TABLE IF EXISTS Result;
CREATE TABLE Result(q float);
INSERT INTO Result VALUES (0.0);

CREATE FUNCTION on_insert_bidsf() returns trigger AS $on_insert_bidsf$
  DECLARE
    v1 float;
  BEGIN
    v1 := 
    ( select sum(b1.price * b1.volume) 
      from   bids b1
      where  0.25 * (select case when sum(b3.volume)
                            is null then 0 else sum(b3.volume) end
                     from bids b3)
             > (select case when v is null then 0 else v end
                from (select sum(b2.volume) as v
                      from bids b2 where b2.price > b1.price) as R) );

    update result set q = (select case when v1 is null then 0 else v1 end);
    return new;
  END;
$on_insert_bidsf$ LANGUAGE plpgsql;

-- Repetitive triggers must be executed after insert to Bids.
CREATE TRIGGER on_insert_bids AFTER INSERT ON Bids
    FOR EACH ROW EXECUTE PROCEDURE on_insert_bidsf();

COPY InsertBids FROM '@@PATH@@/testdata/InsertBIDS.dbtdat' WITH DELIMITER ',';
COPY DeleteBids FROM '@@PATH@@/testdata/DeleteBIDS.dbtdat' WITH DELIMITER ',';

INSERT INTO Bids (price, volume)
  SELECT price, volume FROM InsertBids
  EXCEPT ALL SELECT price, volume FROM DeleteBids;

SELECT q FROM Result;

-- Cleanup
DROP TRIGGER on_insert_bids ON Bids;
DROP FUNCTION on_insert_bidsf();

DROP TABLE Result;
DROP TABLE Bids;

DROP TABLE InsertBids;
DROP TABLE DeleteBids;
