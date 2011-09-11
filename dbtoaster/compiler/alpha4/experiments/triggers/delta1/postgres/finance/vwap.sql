DROP TABLE IF EXISTS InsertBids;
CREATE TABLE InsertBids(t float, order_id float, broker_id float, volume float, price float);

DROP TABLE IF EXISTS DeleteBids;
CREATE TABLE DeleteBids(t float, order_id float, broker_id float, volume float, price float);

DROP TABLE IF EXISTS Bids;
CREATE TABLE Bids(volume float, price float);

DROP TABLE IF EXISTS Result;
CREATE TABLE Result(q float);
INSERT INTO Result VALUES (0.0);

CREATE FUNCTION on_insert_bids_s1f() returns trigger AS $on_insert_bids_s1f$
  DECLARE
    v1 float;
    v2 float;
    v3 float;
  BEGIN
    -- ((if QUERY_BS1_2[QUERYBIDS_B1__PRICE]<(0.25*QUERY_BS1_3[])
    --   then QUERYBIDS_B1__PRICE else 0)*QUERYBIDS_B1__VOLUME)

	v1 :=
    ( select new.price * new.volume where
        (select case when v is null then 0 else v end
         from (select sum(b2.volume) as v from bids b2
               where b2.price > new.price) as query_bs1_2)
        < (0.25*(select case when sum(b3.volume)
                        is null then 0 else sum(b3.volume) end
                 from bids b3)) );

    -- (if (QUERY_BS1_2[QUERYBIDS_B1__PRICE]+
    --     (QUERYBIDS_B1__VOLUME*(if QUERYBIDS_B1__PRICE<QUERYBIDS_B1__PRICE then 1 else 0))) < 
    --     ((0.25*QUERY_BS1_3[])+(0.25*QUERYBIDS_B1__VOLUME))
    --     and (0.25*QUERY_BS1_3[])<=QUERY_BS1_2[QUERYBIDS_B1__PRICE]
    --  then (QUERYBIDS_B1__PRICE*QUERYBIDS_B1__VOLUME) else 0)

    v2 :=
    ( select new.price * new.volume where
        ( (select case when v is null then 0 else v end
           from (select sum(b2.volume) as v from bids b2
               where b2.price > new.price) as query_bs1_2)
          < ((0.25*(select case when sum(b3.volume)
                           is null then 0 else sum(b3.volume) end
                    from bids b3))
             +(0.25*new.volume)) )
        and
        ((0.25*(select case when sum(b3.volume)
                       is null then 0 else sum(b3.volume) end
                from bids b3))
         <=
          (select case when v is null then 0 else v end
             from (select sum(b2.volume) as v from bids b2
               where b2.price > new.price) as query_bs1_2)) );

    -- ((if QUERY_BS1_2[QUERYBIDS_B1__PRICE]<(0.25*QUERY_BS1_3[])
    --      and ((0.25*QUERY_BS1_3[])+(0.25*QUERYBIDS_B1__VOLUME)) <=
    --           (QUERY_BS1_2[QUERYBIDS_B1__PRICE]+
    --           (QUERYBIDS_B1__VOLUME*(if QUERYBIDS_B1__PRICE<QUERYBIDS_B1__PRICE then 1 else 0)))
    --   then (QUERYBIDS_B1__PRICE*QUERYBIDS_B1__VOLUME) else 0)*-1)

     v3 :=
     ( select -1 * (new.price * new.volume) where
         ( ( (select case when v is null then 0 else v end        
              from (select sum(b2.volume) as v from bids b2
                    where b2.price > new.price) as query_bs1_2)
           < (0.25*(select case when sum(b3.volume)
                           is null then 0 else sum(b3.volume) end  
                    from bids b3)) )
         and
           ( ((0.25*(select case when sum(b3.volume)
                            is null then 0 else sum(b3.volume) end 
                     from bids b3))
              +(0.25*new.volume))
             <= (select case when v is null then 0 else v end
                 from (select sum(b2.volume) as v from bids b2
                       where b2.price > new.price) as query_bs1_2) ) ) );

    update result set q = q +
      (select case when v1 is null then 0 else v1 end) +
      (select case when v2 is null then 0 else v2 end) +
      (select case when v3 is null then 0 else v3 end);
	  
    return new;
  END;
$on_insert_bids_s1f$ LANGUAGE plpgsql;

CREATE FUNCTION on_insert_bids_s2f() returns trigger AS $on_insert_bids_s2f$
  DECLARE
    v1 float;
    v2 float;
  BEGIN
    -- (if (QUERY_BS1_2[B1__PRICE]+
    --     (QUERYBIDS_B1__VOLUME*(if B1__PRICE<QUERYBIDS_B1__PRICE then 1 else 0)))
    --      < ((0.25*QUERY_BS1_3[])+(0.25*QUERYBIDS_B1__VOLUME))
    --    and (0.25*QUERY_BS1_3[])<=QUERY_BS1_2[B1__PRICE]
    --    then QUERY_BS1_1[B1__PRICE] else 0)

    v1 :=
    ( select sum(b1.price*b1.volume) from bids b1
      where ( ((select case when v is null then 0 else v end
                from (select sum(b2.volume) as v from bids b2
                      where b2.price > b1.price) as query_bs1_2)
               + (select case when b1.price < new.price
                         then new.volume else 0 end))
              < ((0.25*(select case when sum(b3.volume)
                           is null then 0 else sum(b3.volume) end
                        from bids b3))
                 +(0.25*new.volume))
             and
             (0.25*(select case when sum(b3.volume)
                           is null then 0 else sum(b3.volume) end
                    from bids b3)
             <= (select case when v is null then 0 else v end
                 from (select sum(b2.volume) as v from bids b2
                       where b2.price > b1.price) as query_bs1_2)) ) );

    -- ((if  QUERY_BS1_2[B1__PRICE]<(0.25*QUERY_BS1_3[])
    --     and ((0.25*QUERY_BS1_3[])+(0.25*QUERYBIDS_B1__VOLUME)) <= 
    --          (QUERY_BS1_2[B1__PRICE]+
    --          (QUERYBIDS_B1__VOLUME*(if B1__PRICE<QUERYBIDS_B1__PRICE then 1 else 0)))
    --     then QUERY_BS1_1[B1__PRICE] else 0)*-1)

    v2 :=
    ( select -1*sum(b1.price*b1.volume) from bids b1
      where ( ((select case when v is null then 0 else v end
                from (select sum(b2.volume) as v from bids b2
                      where b2.price > b1.price) as query_bs1_2)
               < (0.25*(select case when sum(b3.volume)
                               is null then 0 else sum(b3.volume) end
                        from bids b3)) )
              and
              (((0.25*(select case when sum(b3.volume)
                              is null then 0 else sum(b3.volume) end
                        from bids b3))
                 + (0.25*new.volume))
               <= ((select case when v is null then 0 else v end
                    from (select sum(b2.volume) as v from bids b2
                          where b2.price > b1.price) as query_bs1_2)
                   + (select case when b1.price < new.price
                             then new.volume else 0 end))) ) );

    update result set q = q +
      (select case when v1 is null then 0 else v1 end) +
      (select case when v2 is null then 0 else v2 end);

    return new;
  END;
$on_insert_bids_s2f$ LANGUAGE plpgsql;

-- Triggers must be executed before insert to Bids to allow use of old value
-- of Bids in SQL queries.
CREATE TRIGGER on_insert_bids_s1 BEFORE INSERT ON Bids
    FOR EACH ROW EXECUTE PROCEDURE on_insert_bids_s1f();

CREATE TRIGGER on_insert_bids_s2 BEFORE INSERT ON Bids
    FOR EACH ROW EXECUTE PROCEDURE on_insert_bids_s2f();

    
COPY InsertBids FROM '@@PATH@@/testdata/InsertBIDS.dbtdat' WITH DELIMITER ',';
COPY DeleteBids FROM '@@PATH@@/testdata/DeleteBIDS.dbtdat' WITH DELIMITER ',';

INSERT INTO Bids (price, volume)
  SELECT price, volume FROM InsertBids
  EXCEPT ALL SELECT price, volume FROM DeleteBids;

SELECT q FROM Result;

-- Cleanup
DROP TRIGGER on_insert_bids_s2 ON Bids;
DROP TRIGGER on_insert_bids_s1 ON Bids;

DROP FUNCTION on_insert_bids_s2f();
DROP FUNCTION on_insert_bids_s1f();

DROP TABLE Result;
DROP TABLE Bids;

DROP TABLE InsertBids;
DROP TABLE DeleteBids;
