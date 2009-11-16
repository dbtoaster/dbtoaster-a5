--persist
--#key PARTS[PID] <= PARTSUPP[PS_PID]
--#key SUPP[SID] <= PARTSUPP[PS_SID]
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
--domain PARTS=10000000,*,*,*,*,*
--domain SUPP=500000,25
--domain PARTSUPP=10000000,500000,*,*
--slice source tpch/100m
--partition q:6
--preload tpch/ppss_maps/ppss.Map1.7:tpch/ppss_maps/ppss.Map2.7:tpch/ppss_maps/ppss.Map3.7:tpch/ppss_maps/ppss.Map4.7:tpch/ppss_maps/ppss.Map5.7:tpch/ppss_maps/ppss.Map6.7:tpch/ppss_maps/ppss.Map7.7:tpch/ppss_maps/ppss.Map8.7:tpch/ppss_maps/ppss.Map9.7:tpch/ppss_maps/ppss.Map10.7 1:3:500000 1:5:10000000 2:1:500000 2:0:10000000 3:1:500000 3:0:10000000 4:0:10000000 5:0:500000 6:0:10000000 7:5:500000 7:0:10000000 8:1:500000 8:0:10000000 9:1:500000 9:0:10000000 10:1:500000 10:0:10000000
--#./bin/client.sh data/tpch_PPSS.unit --quiet --test --use 'SUPP(0,3)' --use 'PARTSUPP(0,1,2,3)' --use 'PARTS(0,2,3,4,5,6)' --transform 'PARTS[2]~/^[^#]*#([0-9]+)/\1/' --transform 'PARTS[3]~/^[^#]*#([0-9]+)/\1/' --transform 'PARTS[4]#' --transform 'PARTS[6]#' -h ~/tpch/100m
create table parts(pid int, mfgr int, type int, size int, container int, sellprice float);
create table partsupp(ps_pid int, ps_sid int, availqty int, buyprice float);
create table supp(sid int, s_nid int);
select 
  sum((sellprice + (-1 * buyprice)) * availqty),
  pid, mfgr, type, sid, size, container, s_nid
from
  parts, partsupp, supp
where
  pid = ps_pid AND sid = ps_sid
group by
  pid, mfgr, type, sid, size, container, s_nid;