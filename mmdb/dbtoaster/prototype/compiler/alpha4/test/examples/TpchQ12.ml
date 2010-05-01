(*
create table customers(cid int, nation int);
create table orders(
   oid int, o_cid int, opriority int, spriority int,
   foreign key(o cid) references customers(cid)
);
create table lineitems(
   l oid int, lateship int, latedelivery bool, shipmode bool,
   foreign key(l oid) references orders(oid)
);

                           Query
select count( * ),
   nation, cid, oid, opriority, spriority,
   lateship, latedelivery, shipmode
from customers, orders, lineitems
where o cid=cid and l oid=oid
group by cube
   nation, cid, oid, opriority, spriority,
   lateship, latedelivery, shipmode;
*)

open Calculus
open Util;;

let cg = Compiler.generate_unit_test_code;;
let test_query s = Debug.log_unit_test s (String.concat "\n");;

let qr =
   RVal(AggSum(RVal(Const (Int 1)),
          RA_MultiNatJoin[
      RA_Leaf(Rel("C", [("cid", TInt); ("nation", TInt)]));
      RA_Leaf(Rel("O", [("oid", TInt); ("cid", TInt); ("opriority", TInt); ("spriority", TInt)]));
      RA_Leaf(Rel("L", [("oid", TInt); ("lateship", TInt); ("latedelivery", TInt); ("shipmode", TInt)]))
   ]));;

let sch = [("C", [("cid", TInt); ("nation", TInt)]);
           ("O", [("oid", TInt); ("o_cid", TInt); ("opriority", TInt); ("spriority", TInt)]);
           ("L", [("l_oid", TInt); ("lateship", TInt); ("latedelivery", TInt); ("shipmode", TInt)])];;

let grpbycols = [("nation", TInt); ("cid", TInt); ("oid", TInt); ("opriority", TInt); ("spriority", TInt); ("lateship", TInt); ("latedelivery", TInt); ("shipmode", TInt)];;

let grpbycols = [("cid", TInt)];;

test_query "select count() form C,O,L where C.cid=O.cid and O.oid=oid group by <grpbycols>"
(Compiler.compile Calculus.ModeOpenDomain sch
   (make_term qr, (map_term "q" grpbycols)) cg [])
(
["+C(qC_cid, qC_nation): q[qC_cid] += qC1[qC_cid]";
 "-C(qC_cid, qC_nation): q[qC_cid] += (qC1[qC_cid]*-1)";
 "+O(qO_oid, qO_o_cid, qO_opriority, qO_spriority): q[qO_o_cid] += (qO1[qO_o_cid]*qO2[qO_oid])";
 "-O(qO_oid, qO_o_cid, qO_opriority, qO_spriority): q[qO_o_cid] += (qO1[qO_o_cid]*qO2[qO_oid]*-1)";
 "+L(qL_l_oid, qL_lateship, qL_latedelivery, qL_shipmode): foreach cid do q[cid] += qL1[cid, qL_l_oid]";
 "-L(qL_l_oid, qL_lateship, qL_latedelivery, qL_shipmode): foreach cid do q[cid] += (qL1[cid, qL_l_oid]*-1)";
 "+C(qL1C_cid, qL1C_nation): foreach qL_l_oid do qL1[qL1C_cid, qL_l_oid] += qL1C1[qL_l_oid, qL1C_cid]";
 "-C(qL1C_cid, qL1C_nation): foreach qL_l_oid do qL1[qL1C_cid, qL_l_oid] += (qL1C1[qL_l_oid, qL1C_cid]*-1)";
 "+O(qL1O_oid, qL1O_o_cid, qL1O_opriority, qL1O_spriority): qL1[qL1O_o_cid, qL1O_oid] += qL1O1[qL1O_o_cid]";
 "-O(qL1O_oid, qL1O_o_cid, qL1O_opriority, qL1O_spriority): qL1[qL1O_o_cid, qL1O_oid] += (qL1O1[qL1O_o_cid]*-1)";
 "+C(qL1O1C_cid, qL1O1C_nation): qL1O1[qL1O1C_cid] += 1";
 "-C(qL1O1C_cid, qL1O1C_nation): qL1O1[qL1O1C_cid] += -1";
 "+O(qL1C1O_oid, qL1C1O_o_cid, qL1C1O_opriority, qL1C1O_spriority): qL1C1[qL1C1O_oid, qL1C1O_o_cid] += 1";
 "-O(qL1C1O_oid, qL1C1O_o_cid, qL1C1O_opriority, qL1C1O_spriority): qL1C1[qL1C1O_oid, qL1C1O_o_cid] += -1";
 "+L(qO2L_l_oid, qO2L_lateship, qO2L_latedelivery, qO2L_shipmode): qO2[qO2L_l_oid] += 1";
 "-L(qO2L_l_oid, qO2L_lateship, qO2L_latedelivery, qO2L_shipmode): qO2[qO2L_l_oid] += -1";
 "+C(qO1C_cid, qO1C_nation): qO1[qO1C_cid] += 1";
 "-C(qO1C_cid, qO1C_nation): qO1[qO1C_cid] += -1";
 "+O(qC1O_oid, qC1O_o_cid, qC1O_opriority, qC1O_spriority): qC1[qC1O_o_cid] += qC1O1[qC1O_oid]";
 "-O(qC1O_oid, qC1O_o_cid, qC1O_opriority, qC1O_spriority): qC1[qC1O_o_cid] += (qC1O1[qC1O_oid]*-1)";
 "+L(qC1L_l_oid, qC1L_lateship, qC1L_latedelivery, qC1L_shipmode): foreach qC_cid do qC1[qC_cid] += qC1L1[qC1L_l_oid, qC_cid]";
 "-L(qC1L_l_oid, qC1L_lateship, qC1L_latedelivery, qC1L_shipmode): foreach qC_cid do qC1[qC_cid] += (qC1L1[qC1L_l_oid, qC_cid]*-1)";
 "+O(qC1L1O_oid, qC1L1O_o_cid, qC1L1O_opriority, qC1L1O_spriority): qC1L1[qC1L1O_oid, qC1L1O_o_cid] += 1";
 "-O(qC1L1O_oid, qC1L1O_o_cid, qC1L1O_opriority, qC1L1O_spriority): qC1L1[qC1L1O_oid, qC1L1O_o_cid] += -1";
 "+L(qC1O1L_l_oid, qC1O1L_lateship, qC1O1L_latedelivery, qC1O1L_shipmode): qC1O1[qC1O1L_l_oid] += 1";
 "-L(qC1O1L_l_oid, qC1O1L_lateship, qC1O1L_latedelivery, qC1O1L_shipmode): qC1O1[qC1O1L_l_oid] += -1"]
);;




