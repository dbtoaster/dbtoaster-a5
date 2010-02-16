open Calculus;;

let sch =
[
("DATE", ["datekey"; "year"]);
("PART", ["partkey"; "partcat"]);
("LINEORDER", ["datekey"; "partkey"; "revenue"])
];;


let ssb = RVal (AggSum( (RVal (Var "REVENUE")),
       RA_MultiNatJoin
          [RA_Leaf (Rel ("DATE", ["DATEKEY"; "YEAR"]));
           RA_Leaf (Rel ("PART", ["PARTKEY"; "PARTCAT"]));
           RA_Leaf (Rel ("LINEORDER", ["DATEKEY"; "PARTKEY"; "REVENUE"]))])) ;;


Compiler.compile Calculus.ModeExtractFromCond sch
                 (Compiler.mk_external "m" ["PARTCAT"; "YEAR"])
                 (make_term ssb) =
["+DATE(x_mDATE_datekey, x_mDATE_year): foreach PARTCAT do m[PARTCAT, x_mDATE_year] += mDATE1[x_mDATE_datekey, PARTCAT]";
 "+PART(x_mPART_partkey, x_mPART_partcat): foreach YEAR do m[x_mPART_partcat, YEAR] += mPART1[x_mPART_partkey, YEAR]";
 "+LINEORDER(x_mLINEORDER_datekey, x_mLINEORDER_partkey, x_mLINEORDER_revenue): foreach PARTCAT, YEAR do m[PARTCAT, YEAR] += (x_mLINEORDER_revenue*mLINEORDER1[x_mLINEORDER_datekey, YEAR]*mLINEORDER2[x_mLINEORDER_partkey, PARTCAT])";
 "+PART(x_mDATE1PART_partkey, x_mDATE1PART_partcat): foreach x_mDATE_datekey do mDATE1[x_mDATE_datekey, x_mDATE1PART_partcat] += mDATE1PART1[x_mDATE_datekey, x_mDATE1PART_partkey]";
 "+LINEORDER(x_mDATE1LINEORDER_datekey, x_mDATE1LINEORDER_partkey, x_mDATE1LINEORDER_revenue): foreach PARTCAT do mDATE1[x_mDATE1LINEORDER_datekey, PARTCAT] += (x_mDATE1LINEORDER_revenue*mDATE1LINEORDER1[x_mDATE1LINEORDER_partkey, PARTCAT])";
 "+LINEORDER(x_mDATE1PART1LINEORDER_datekey, x_mDATE1PART1LINEORDER_partkey, x_mDATE1PART1LINEORDER_revenue): mDATE1PART1[x_mDATE1PART1LINEORDER_datekey, x_mDATE1PART1LINEORDER_partkey] += x_mDATE1PART1LINEORDER_revenue";
 "+PART(x_mDATE1LINEORDER1PART_partkey, x_mDATE1LINEORDER1PART_partcat): mDATE1LINEORDER1[x_mDATE1LINEORDER1PART_partkey, x_mDATE1LINEORDER1PART_partcat] += 1";
 "+DATE(x_mPART1DATE_datekey, x_mPART1DATE_year): foreach x_mPART_partkey do mPART1[x_mPART_partkey, x_mPART1DATE_year] += mPART1DATE1[x_mPART1DATE_datekey, x_mPART_partkey]";
 "+LINEORDER(x_mPART1LINEORDER_datekey, x_mPART1LINEORDER_partkey, x_mPART1LINEORDER_revenue): foreach YEAR do mPART1[x_mPART1LINEORDER_partkey, YEAR] += (x_mPART1LINEORDER_revenue*mPART1LINEORDER1[x_mPART1LINEORDER_datekey, YEAR])";
 "+LINEORDER(x_mPART1DATE1LINEORDER_datekey, x_mPART1DATE1LINEORDER_partkey, x_mPART1DATE1LINEORDER_revenue): mPART1DATE1[x_mPART1DATE1LINEORDER_datekey, x_mPART1DATE1LINEORDER_partkey] += x_mPART1DATE1LINEORDER_revenue";
 "+DATE(x_mPART1LINEORDER1DATE_datekey, x_mPART1LINEORDER1DATE_year): mPART1LINEORDER1[x_mPART1LINEORDER1DATE_datekey, x_mPART1LINEORDER1DATE_year] += 1";
 "+DATE(x_mLINEORDER1DATE_datekey, x_mLINEORDER1DATE_year): mLINEORDER1[x_mLINEORDER1DATE_datekey, x_mLINEORDER1DATE_year] += 1";
 "+PART(x_mLINEORDER2PART_partkey, x_mLINEORDER2PART_partcat): mLINEORDER2[x_mLINEORDER2PART_partkey, x_mLINEORDER2PART_partcat] += 1"]
;;








(*
+DATE(datekey, year):
 foreach PARTCAT do m[PARTCAT, year] += mD[datekey, PARTCAT];
 foreach partkey do mP[partkey, year] += mDP[datekey, partkey];
 mPL[datekey, year] += 1

+PART(partkey, partcat):
 foreach YEAR do m[partcat, YEAR] += mP[partkey, YEAR];
 foreach datekey do mD[datekey, partcat] += mDP[datekey, partkey];
 mDL[partkey, partcat] += 1

+LINEORDER(datekey, partkey, revenue):
 foreach PARTCAT, YEAR do m[PARTCAT, YEAR] += revenue*mPL[datekey, YEAR]*mDL[partkey, PARTCAT];
 foreach PARTCAT do mD[datekey, PARTCAT] += revenue*mDL[partkey, PARTCAT];
 foreach YEAR do mP[partkey, YEAR] += revenue*mPL[datekey, YEAR];
 mDP[datekey, partkey] += revenue;
*)




(*
+DATE(datekey, year):
 mPL[datekey, year] += 1

+PART(partkey, partcat):
 mDL[partkey, partcat] += 1

+LINEORDER(datekey, partkey, revenue):
 foreach PARTCAT, YEAR do m[PARTCAT, YEAR] += revenue*mPL[datekey, YEAR]*mDL[partkey, PARTCAT];

*)







