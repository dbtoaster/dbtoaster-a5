open Algebra
open Codegen
open Compile
open Gui

let relb =
(*    `Relation("B",[("time", "int"); ("id", "int"); ("price", "int"); ("volume", "int")]) *)
    `Relation("B",[("P", "int"); ("V", "int")])

let relb1 =
(*    `Relation("B",[("time1", "int"); ("id1", "int"); ("price1", "int"); ("volume1", "int")]) *)
    `Relation("B",[("P1", "int"); ("V1", "int")])

let tv = 
    `MapAggregate(`Sum,
    `METerm(`Attribute(`Qualified("B", "V"))), relb)

let sv1_pred =
    `BTerm(`GT(
	`ETerm(`Attribute(`Qualified("B", "P1"))),
	`ETerm(`Attribute(`Qualified("B", "P")))))

let select_sv1 = `Select(sv1_pred, relb1)

let k_sv0 = `Product(`METerm(`Float(0.25)), tv)
let sv1 = `MapAggregate(`Sum,
    `METerm(`Attribute(`Qualified("B","V1"))), select_sv1)

let m_p2 = `Sum(k_sv0, sv1)

let vwap =
    `MapAggregate(`Sum,
        `Product(`METerm(`Attribute(`Qualified("B", "P"))),
            `METerm(`Attribute(`Qualified("B", "V")))),
        `Select(`BTerm(`MLT(m_p2)), relb))
;;


(*
let tv = 
    `MapAggregate(`Sum,
      `METerm(`Attribute(`Qualified("B", "V0"))),
        `Relation("B",[("P0", "int"); ("V0", "int")]))

let sv1_pred =
    `BTerm(`GT(
	`ETerm(`Attribute(`Qualified("B", "P1"))),
	`ETerm(`Attribute(`Qualified("B", "P2")))))

let select_sv1 =
    `Select(sv1_pred, `Relation("B",[("P1", "int"); ("V1", "int")]))

let k_sv0 = `Product(`METerm(`Float(0.25)), tv)
let sv1 = `MapAggregate(`Sum,
`METerm(`Attribute(`Qualified("B","V1"))), select_sv1)

let m_p2 = `Sum(k_sv0, sv1)

let vwap =
    `MapAggregate(`Sum,
      `Product(`METerm(`Attribute(`Qualified("B", "P2"))),
      `METerm(`Attribute(`Qualified("B", "V2")))),
        `Select(`BTerm(`MLT(m_p2)),
          `Relation("B", [("P2", "int"); ("V2","int")])))

;;
*)

let print_test_type tt =
    print_endline ((String.make 50 '-')^"\n\n"^tt^" tests\n\n"^(String.make 50 '-'))
;;

(*
print_test_type "compile_code";
compile_code vwap (`Insert ("B", [("p", "int"); ("v", "int")])) "vwap.cc";;
*)

(* relation_sources: 
   source type, source constructor args, tuple type,
   adaptor type, adaptor bindings, thrift tuple namespace, stream name
*)
let relation_sources =
    [("B",
        ("DBToaster::DemoDatasets::VwapFileStream",
        "\"20081201.csv\",1000",
        "DBToaster::DemoDatasets::VwapTuple",
        "DBToaster::DemoDatasets::VwapTupleAdaptor",
        [("T", "t"); ("ID", "id"); ("P", "price"); ("V", "volume")],
        "datasets",
        "VwapBids"))]
in
(*
    print_test_type "compile_standalone_engine";
    compile_standalone_engine vwap relation_sources "vwap.cc" (Some "vwaptrace.catalog");;
*)

    print_test_type "compile_standalone_debugger";
    compile_standalone_debugger [vwap] relation_sources "vwap.cc" (Some "vwaptrace.catalog");;
