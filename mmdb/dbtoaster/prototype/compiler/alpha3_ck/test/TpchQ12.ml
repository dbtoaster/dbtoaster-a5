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

open Calculus;;

let qr =
   RVal(AggSum(RVal(Const (Int 1)),
          RA_MultiNatJoin[
      RA_Leaf(Rel("C", ["cid"; "nation"]));
      RA_Leaf(Rel("O", ["oid"; "cid"; "opriority"; "spriority"]));
      RA_Leaf(Rel("L", ["oid"; "lateship"; "latedelivery"; "shipmode"]))
   ]));;

let sch = [("C", ["cid"; "nation"]);
           ("O", ["oid"; "o_cid"; "opriority"; "spriority"]);
           ("L", ["l_oid"; "lateship"; "latedelivery"; "shipmode"])];;

let grpbycols = ["nation"; "cid"; "oid"; "opriority"; "spriority"; "lateship"; "latedelivery"; "shipmode"];;

let grpbycols = ["cid"];;

Compiler.compile Calculus.ModeOpenDomain sch
   (Compiler.mk_external "q" grpbycols) (make_term qr) =
["+C(x_qC_cid, x_qC_nation): q[x_qC_cid] += qC1[x_qC_cid]";
 "+O(x_qO_oid, x_qO_o_cid, x_qO_opriority, x_qO_spriority): q[x_qO_o_cid] += (qO1[x_qO_o_cid]*qO2[x_qO_oid])";
 "+L(x_qL_l_oid, x_qL_lateship, x_qL_latedelivery, x_qL_shipmode): foreach cid do q[cid] += qL1[cid, x_qL_l_oid]";
 "+O(x_qC1O_oid, x_qC1O_o_cid, x_qC1O_opriority, x_qC1O_spriority): qC1[x_qC1O_o_cid] += qC1O1[x_qC1O_oid]";
 "+L(x_qC1L_l_oid, x_qC1L_lateship, x_qC1L_latedelivery, x_qC1L_shipmode): foreach x_qC_cid do qC1[x_qC_cid] += qC1L1[x_qC1L_l_oid, x_qC_cid]";
 "+L(x_qC1O1L_l_oid, x_qC1O1L_lateship, x_qC1O1L_latedelivery, x_qC1O1L_shipmode): qC1O1[x_qC1O1L_l_oid] += 1";
 "+O(x_qC1L1O_oid, x_qC1L1O_o_cid, x_qC1L1O_opriority, x_qC1L1O_spriority): qC1L1[x_qC1L1O_oid, x_qC1L1O_o_cid] += 1";
 "+C(x_qO1C_cid, x_qO1C_nation): qO1[x_qO1C_cid] += 1";
 "+L(x_qO2L_l_oid, x_qO2L_lateship, x_qO2L_latedelivery, x_qO2L_shipmode): qO2[x_qO2L_l_oid] += 1";
 "+C(x_qL1C_cid, x_qL1C_nation): foreach x_qL_l_oid do qL1[x_qL1C_cid, x_qL_l_oid] += qL1C1[x_qL_l_oid, x_qL1C_cid]";
 "+O(x_qL1O_oid, x_qL1O_o_cid, x_qL1O_opriority, x_qL1O_spriority): qL1[x_qL1O_o_cid, x_qL1O_oid] += qL1O1[x_qL1O_o_cid]";
 "+O(x_qL1C1O_oid, x_qL1C1O_o_cid, x_qL1C1O_opriority, x_qL1C1O_spriority): qL1C1[x_qL1C1O_oid, x_qL1C1O_o_cid] += 1";
 "+C(x_qL1O1C_cid, x_qL1O1C_nation): qL1O1[x_qL1O1C_cid] += 1"]
;;




