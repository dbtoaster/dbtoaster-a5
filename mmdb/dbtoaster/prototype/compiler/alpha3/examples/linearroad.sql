CREATE TABLE INPUT(
        type integer,
        time integer, carid integer,
        speed integer, xway integer, lane integer, dir integer,
        seg integer, pos integer,
        qid integer,
        m_init integer, m_end integer,
        dow integer, tod integer, day integer )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::LinearRoadFileStream'
    ARGS '"/Users/yanif/datasets/linearroad/datafile625seconds.dat",10000'
    INSTANCE 'LRInput'
    TUPLE 'DBToaster::DemoDatasets::LinearRoadTuple'
    ADAPTOR 'DBToaster::DemoDatasets::LinearRoadTupleAdaptor';

SELECT  sum(1), in1.dir, in1.seg, in1.pos
FROM    input in1,
        input in2,
        input in11,
        input in22
WHERE   in1.speed = 0  AND in1.type = 0  AND
        in2.speed = 0  AND in2.type = 0  AND
        in11.speed = 0 AND in11.type = 0 AND
        in22.speed = 0 AND in22.type = 0 AND
        in2.carid <> in1.carid AND
        in2.pos = in1.pos AND                       
        in2.lane = in1.lane AND
        in2.dir = in1.dir AND
        in2.time >= in1.time AND 
        in2.time <= in1.time + 30 AND
        in11.carid = in1.carid AND
        in11.pos = in1.pos AND
        in11.lane = in1.lane AND
        in11.dir = in1.dir AND
        in11.time = in1.time + 120 AND
        in22.carid = in2.carid AND
        in22.pos = in1.pos AND                       
        in22.lane = in1.lane AND
        in22.dir = in1.dir AND
        in22.time = in2.time + 120
GROUP BY  in1.dir, in1.seg, in1.pos;