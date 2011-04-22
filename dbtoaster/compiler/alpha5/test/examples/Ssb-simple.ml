open Calculus
open Util;;

let cg = Compiler.generate_unit_test_code;;
let test_query s = Debug.log_unit_test s (String.concat "\n");;

let sch =
[
("DATE", [("datekey", TInt); ("year", TInt)]);
("PART", [("partkey", TInt); ("partcat", TInt)]);
("LINEORDER", [("datekey", TInt); ("partkey", TInt); ("revenue", TInt)])
];;


let ssb = RVal (AggSum( (RVal (Var ("REVENUE", TInt))),
       RA_MultiNatJoin
          [RA_Leaf (Rel ("DATE", [("DATEKEY", TInt); ("YEAR", TInt)]));
           RA_Leaf (Rel ("PART", [("PARTKEY", TInt); ("PARTCAT", TInt)]));
           RA_Leaf (Rel ("LINEORDER", [("DATEKEY", TInt); ("PARTKEY", TInt); ("REVENUE", TInt)]))])) ;;


let m = make_term ssb in
test_query ("select partcat, year, sum(revenue) from date,part,lineorder "^
            "where group by partcat, year LI.DK=D.DK and LI.PK=P.PK")
(Compiler.compile Calculus.ModeExtractFromCond sch
                 (m, (map_term "m" [("PARTCAT", TInt); ("YEAR", TInt)])) cg [])
(
["+DATE(mDATE_datekey, mDATE_year): foreach PARTCAT do m[PARTCAT, mDATE_year] += mDATE1[mDATE_datekey, PARTCAT]";
 "-DATE(mDATE_datekey, mDATE_year): foreach PARTCAT do m[PARTCAT, mDATE_year] += (mDATE1[mDATE_datekey, PARTCAT]*-1)";
 "+PART(mPART_partkey, mPART_partcat): foreach YEAR do m[mPART_partcat, YEAR] += mPART1[mPART_partkey, YEAR]";
 "-PART(mPART_partkey, mPART_partcat): foreach YEAR do m[mPART_partcat, YEAR] += (mPART1[mPART_partkey, YEAR]*-1)";
 "+LINEORDER(mLINEORDER_datekey, mLINEORDER_partkey, mLINEORDER_revenue): foreach PARTCAT, YEAR do m[PARTCAT, YEAR] += (mLINEORDER_revenue*mLINEORDER1[mLINEORDER_datekey, YEAR]*mLINEORDER2[mLINEORDER_partkey, PARTCAT])";
 "-LINEORDER(mLINEORDER_datekey, mLINEORDER_partkey, mLINEORDER_revenue): foreach PARTCAT, YEAR do m[PARTCAT, YEAR] += (mLINEORDER_revenue*mLINEORDER1[mLINEORDER_datekey, YEAR]*mLINEORDER2[mLINEORDER_partkey, PARTCAT]*-1)";
 "+PART(mLINEORDER2PART_partkey, mLINEORDER2PART_partcat): mLINEORDER2[mLINEORDER2PART_partkey, mLINEORDER2PART_partcat] += 1";
 "-PART(mLINEORDER2PART_partkey, mLINEORDER2PART_partcat): mLINEORDER2[mLINEORDER2PART_partkey, mLINEORDER2PART_partcat] += -1";
 "+DATE(mLINEORDER1DATE_datekey, mLINEORDER1DATE_year): mLINEORDER1[mLINEORDER1DATE_datekey, mLINEORDER1DATE_year] += 1";
 "-DATE(mLINEORDER1DATE_datekey, mLINEORDER1DATE_year): mLINEORDER1[mLINEORDER1DATE_datekey, mLINEORDER1DATE_year] += -1";
 "+DATE(mPART1DATE_datekey, mPART1DATE_year): foreach mPART_partkey do mPART1[mPART_partkey, mPART1DATE_year] += mPART1DATE1[mPART1DATE_datekey, mPART_partkey]";
 "-DATE(mPART1DATE_datekey, mPART1DATE_year): foreach mPART_partkey do mPART1[mPART_partkey, mPART1DATE_year] += (mPART1DATE1[mPART1DATE_datekey, mPART_partkey]*-1)";
 "+LINEORDER(mPART1LINEORDER_datekey, mPART1LINEORDER_partkey, mPART1LINEORDER_revenue): foreach YEAR do mPART1[mPART1LINEORDER_partkey, YEAR] += (mPART1LINEORDER_revenue*mPART1LINEORDER1[mPART1LINEORDER_datekey, YEAR])";
 "-LINEORDER(mPART1LINEORDER_datekey, mPART1LINEORDER_partkey, mPART1LINEORDER_revenue): foreach YEAR do mPART1[mPART1LINEORDER_partkey, YEAR] += (mPART1LINEORDER_revenue*mPART1LINEORDER1[mPART1LINEORDER_datekey, YEAR]*-1)";
 "+DATE(mPART1LINEORDER1DATE_datekey, mPART1LINEORDER1DATE_year): mPART1LINEORDER1[mPART1LINEORDER1DATE_datekey, mPART1LINEORDER1DATE_year] += 1";
 "-DATE(mPART1LINEORDER1DATE_datekey, mPART1LINEORDER1DATE_year): mPART1LINEORDER1[mPART1LINEORDER1DATE_datekey, mPART1LINEORDER1DATE_year] += -1";
 "+LINEORDER(mPART1DATE1LINEORDER_datekey, mPART1DATE1LINEORDER_partkey, mPART1DATE1LINEORDER_revenue): mPART1DATE1[mPART1DATE1LINEORDER_datekey, mPART1DATE1LINEORDER_partkey] += mPART1DATE1LINEORDER_revenue";
 "-LINEORDER(mPART1DATE1LINEORDER_datekey, mPART1DATE1LINEORDER_partkey, mPART1DATE1LINEORDER_revenue): mPART1DATE1[mPART1DATE1LINEORDER_datekey, mPART1DATE1LINEORDER_partkey] += (mPART1DATE1LINEORDER_revenue*-1)";
 "+PART(mDATE1PART_partkey, mDATE1PART_partcat): foreach mDATE_datekey do mDATE1[mDATE_datekey, mDATE1PART_partcat] += mDATE1PART1[mDATE_datekey, mDATE1PART_partkey]";
 "-PART(mDATE1PART_partkey, mDATE1PART_partcat): foreach mDATE_datekey do mDATE1[mDATE_datekey, mDATE1PART_partcat] += (mDATE1PART1[mDATE_datekey, mDATE1PART_partkey]*-1)";
 "+LINEORDER(mDATE1LINEORDER_datekey, mDATE1LINEORDER_partkey, mDATE1LINEORDER_revenue): foreach PARTCAT do mDATE1[mDATE1LINEORDER_datekey, PARTCAT] += (mDATE1LINEORDER_revenue*mDATE1LINEORDER1[mDATE1LINEORDER_partkey, PARTCAT])";
 "-LINEORDER(mDATE1LINEORDER_datekey, mDATE1LINEORDER_partkey, mDATE1LINEORDER_revenue): foreach PARTCAT do mDATE1[mDATE1LINEORDER_datekey, PARTCAT] += (mDATE1LINEORDER_revenue*mDATE1LINEORDER1[mDATE1LINEORDER_partkey, PARTCAT]*-1)";
 "+PART(mDATE1LINEORDER1PART_partkey, mDATE1LINEORDER1PART_partcat): mDATE1LINEORDER1[mDATE1LINEORDER1PART_partkey, mDATE1LINEORDER1PART_partcat] += 1";
 "-PART(mDATE1LINEORDER1PART_partkey, mDATE1LINEORDER1PART_partcat): mDATE1LINEORDER1[mDATE1LINEORDER1PART_partkey, mDATE1LINEORDER1PART_partcat] += -1";
 "+LINEORDER(mDATE1PART1LINEORDER_datekey, mDATE1PART1LINEORDER_partkey, mDATE1PART1LINEORDER_revenue): mDATE1PART1[mDATE1PART1LINEORDER_datekey, mDATE1PART1LINEORDER_partkey] += mDATE1PART1LINEORDER_revenue";
 "-LINEORDER(mDATE1PART1LINEORDER_datekey, mDATE1PART1LINEORDER_partkey, mDATE1PART1LINEORDER_revenue): mDATE1PART1[mDATE1PART1LINEORDER_datekey, mDATE1PART1LINEORDER_partkey] += (mDATE1PART1LINEORDER_revenue*-1)"]
);;








(* (insert only...)
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




(* (insert trigger incorporating FK constraints)
+DATE(datekey, year):
 mPL[datekey, year] += 1

+PART(partkey, partcat):
 mDL[partkey, partcat] += 1

+LINEORDER(datekey, partkey, revenue):
 foreach PARTCAT, YEAR do m[PARTCAT, YEAR] += revenue*mPL[datekey, YEAR]*mDL[partkey, PARTCAT];

*)







