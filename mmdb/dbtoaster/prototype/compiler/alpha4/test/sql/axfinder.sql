CREATE TABLE bids(broker_id int, v float, p float);
CREATE TABLE asks(broker_id int, v float, p float);

SELECT   b.broker_id, sum(a.v + (-1) * (b.p * b.p))
FROM     bids b, asks a
WHERE    b.broker_id = a.broker_id
  AND    ( (a.p + ((-1) * b.p) > 1000) OR
           (b.p + ((-1) * a.p) > 1000) )
GROUP BY b.broker_id;