--persist
--switch wl03.cac.cornell.edu
--node Node01@wl04.cac.cornell.edu:52982
--node Node02@wl05.cac.cornell.edu:52982
--node Node03@wl06.cac.cornell.edu:52982
--node Node04@wl07.cac.cornell.edu:52982
--node Node05@wl08.cac.cornell.edu:52982
--node Node06@wl09.cac.cornell.edu:52982
--node Node07@wl10.cac.cornell.edu:52982
--node Node08@wl11.cac.cornell.edu:52982
--node Node09@wl12.cac.cornell.edu:52982
--node Node10@wl13.cac.cornell.edu:52982
--node Node11@wl14.cac.cornell.edu:52982
--node Node12@wl15.cac.cornell.edu:52982
--node Node13@wl16.cac.cornell.edu:52982
--node Node14@wl17.cac.cornell.edu:52982
--node Node15@wl18.cac.cornell.edu:52982
--node Node16@wl19.cac.cornell.edu:52982
--node Node17@wl20.cac.cornell.edu:52982
--node Node18@wl21.cac.cornell.edu:52982
--node Node19@wl22.cac.cornell.edu:52982
--node Node20@wl23.cac.cornell.edu:52982
--node Node21@wl24.cac.cornell.edu:52982
--node Node22@wl25.cac.cornell.edu:52982
--node Node23@wl26.cac.cornell.edu:52982
--node Node24@wl27.cac.cornell.edu:52982
--node Node25@wl28.cac.cornell.edu:52982
--node Node26@wl29.cac.cornell.edu:52982
--node Node27@wl30.cac.cornell.edu:52982
--node Node28@wl31.cac.cornell.edu:52982
--node Node29@wl32.cac.cornell.edu:52982
--node Node30@wl33.cac.cornell.edu:52982
--node Node31@wl34.cac.cornell.edu:52982
--node Node32@wl35.cac.cornell.edu:52982
--node Node33@wl36.cac.cornell.edu:52982
--node Node34@wl37.cac.cornell.edu:52982
--node Node35@wl38.cac.cornell.edu:52982
--node Node36@wl39.cac.cornell.edu:52982
--node Node37@wl40.cac.cornell.edu:52982
--node Node38@wl41.cac.cornell.edu:52982
--node Node39@wl42.cac.cornell.edu:52982
--node Node40@wl43.cac.cornell.edu:52982
--slice transform PARTS[0]@40/10000000
--slice transform PARTS[2]~/^[^#]*#([0-9]+)/\1/
--slice transform PARTS[3]~/^[^#]*#([0-9]+)/\1/
--slice transform PARTS[4]#
--slice transform PARTS[6]#
--slice project   PARTS(0,2,3,4,5,6)
--slice transform SUPP[0]@40/500000
--slice project   SUPP(0,3)
--slice transform PARTSUPP[0]@40/10000000
--slice transform PARTSUPP[1]@40/500000
--slice project   PARTSUPP(0,1,2,3)
--slice transform LINEITEMS[0]@40/30000000
--slice transform LINEITEMS[16]<d10,11
--slice transform LINEITEMS[17]<d10,12
--slice transform LINEITEMS[14]#
--slice project   LINEITEMS(0,16,17,14,1,2)
--slice project   NATIONS(1,3)
--key PARTS[PID] <= LINEITEMS[L_PID]
--key PARTS[PID] <= PARTSUPP[PS_PID]
--key SUPP[SID] <= LINEITEMS[L_SID]
--key SUPP[SID] <= PARTSUPP[PS_SID]
--key NATIONS[SID] <= SUPP[S_NID]
--domain PARTS=10000000,*,*,*,*,*
--domain SUPP=500000,40
--domain PARTSUPP=10000000,500000,*,*
--domain LINEITEMS=30000000,*,*,*,10000000,500000
--domain NATIONS=40,40
--slice source tpch/100m
--preload tpch/long_maps/long.Map1.6:tpch/long_maps/long.Map2.6:tpch/long_maps/long.Map3.6:tpch/long_maps/long.Map4.6:tpch/long_maps/long.Map5.6:tpch/long_maps/long.Map6.6:tpch/long_maps/long.Map7.6:tpch/long_maps/long.Map8.6:tpch/long_maps/long.Map9.6:tpch/long_maps/long.Map10.6:tpch/long_maps/long.Map11.6:tpch/long_maps/long.Map12.6:tpch/long_maps/long.Map13.6:tpch/long_maps/long.Map14.6:tpch/long_maps/long.Map15.6:tpch/long_maps/long.Map16.6:tpch/long_maps/long.Map17.6:tpch/long_maps/long.Map18.6:tpch/long_maps/long.Map19.6:tpch/long_maps/long.Map20.6:tpch/long_maps/long.Map21.6:tpch/long_maps/long.Map22.6:tpch/long_maps/long.Map23.6:tpch/long_maps/long.Map24.6:tpch/long_maps/long.Map25.6:tpch/long_maps/long.Map26.6:tpch/long_maps/long.Map27.6:tpch/long_maps/long.Map28.6:tpch/long_maps/long.Map29.6:tpch/long_maps/long.Map30.6:tpch/long_maps/long.Map31.6:tpch/long_maps/long.Map32.6:tpch/long_maps/long.Map33.6:tpch/long_maps/long.Map34.6:tpch/long_maps/long.Map35.6:tpch/long_maps/long.Map36.6:tpch/long_maps/long.Map37.6:tpch/long_maps/long.Map38.6 2:0:10000000 2:4:500000 3:0:10000000 3:1:500000 4:0:10000000 5:0:10000000 6:0:10000000 6:5:500000 7:0:10000000 7:5:500000 8:4:500000 10:0:500000 13:0:10000000 13:1:500000 14:0:10000000 15:0:500000 16:0:10000000 16:4:500000 17:0:10000000 17:4:500000 18:0:10000000 18:1:500000 19:0:10000000 19:1:500000 20:1:500000 21:0:10000000 21:1:500000 22:0:10000000 23:0:10000000 23:1:500000 24:0:10000000 24:1:500000 25:0:10000000 25:1:500000 26:0:10000000 26:1:500000 27:1:10000000 27:2:500000 28:0:10000000 28:1:500000 29:0:10000000 30:1:10000000 30:2:500000 31:1:10000000 31:2:500000 32:0:10000000 32:1:500000 33:0:10000000 34:0:10000000 34:5:500000 35:0:10000000 35:5:500000 36:0:10000000 36:5:500000 37:0:10000000 37:5:500000 38:0:10000000
--#./bin/client.sh data/tpch_PPSS.unit --quiet --test --use 'SUPP(0,3)' --use 'PARTSUPP(0,1,2,3)' --use 'PARTS(0,2,3,4,5,6)' --transform 'PARTS[2]~/^[^#]*#([0-9]+)/\1/' --transform 'PARTS[3]~/^[^#]*#([0-9]+)/\1/' --transform 'PARTS[4]#' --transform 'PARTS[6]#' -h ~/tpch/100m
create table lineitems(l_oid int, lateship int, latecommit int, shipmode int, l_pid int, l_sid int);
create table parts(pid int, mfgr int, type int, size int, container int, sellprice float);
create table partsupp(ps_pid int, ps_sid int, availqty int, buyprice float);
create table supp(sid int, s_nid int);
create table nations(nid int, rid int);
select 
  sum((sellprice + (-1 * buyprice)) * availqty),
  type, size, container, shipmode, rid
from
  lineitems, parts, partsupp, supp, nations
where
  l_pid = pid AND l_sid = sid AND pid = ps_pid AND sid = ps_sid AND s_nid = nid
group by
  type, size, container, shipmode, rid;