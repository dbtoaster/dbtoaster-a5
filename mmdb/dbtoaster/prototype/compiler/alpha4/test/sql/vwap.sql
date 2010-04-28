-- dbtoaster=> create temporary view bids as select * from vwap_5;CREATE VIEW
-- dbtoaster=> select sum(price*volume) 
--             from bids b 
--             where 0.25*(select sum(b1.volume) from bids b1) 
--                > 
--             (select case when sum(b2.volume) is null then 0 
--                          else sum(b2.volume) 
--                     end 
--              from bids b2 
--              where b2.price > b.price);
--    sum    
-- ----------
--  11450000
-- (1 row)

select sum(price*volume)
        from bids b
        where 0.25*(select sum(b1.volume) from bids b1) >
            (select case when sum(b2.volume) is null then 0 else sum(b2.volume) end
             from bids b2 where b2.price > b.price);

CREATE TABLE bids(price float, volume int)
--  FROM POSTGRES dbtoaster.vwap_5(action hash, price float, volume int; 
--                                 events := 'B:insert,D:delete');
  FROM FILE '/Users/yanif/workspace/dbtc_alpha4/vwap5k.csv'
  LINE DELIMITED orderbook (book := 'bids', validate := 'true');

SELECT avg(b1.price * b1.volume) 
FROM   bids b1
WHERE  0.25 * (select sum(b3.volume) from bids b3)
            >
       (select sum(b2.volume) from bids b2 where b2.price > b1.price);
