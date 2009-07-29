(*linearroad queries*)
open Algebra
open Codegen
open Compile
open Gui

let accident_predicate d s p = 
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "DIR"))), `ETerm (`Variable ( d)) )),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "SEG"))), `ETerm (`Variable ( s)))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "POS"))), `ETerm (`Variable ( p)))),
	`And ( `BTerm ( `NE ( `ETerm (`Attribute ( `Qualified ("Input", "CARID2"))), `ETerm (`Attribute ( `Qualified ("Input", "CARID"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "POS2"))), `ETerm (`Attribute ( `Qualified ("Input", "POS"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "LANE2"))), `ETerm (`Attribute ( `Qualified ("Input", "LANE"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "DIR2"))), `ETerm (`Attribute ( `Qualified ("Input", "DIR"))))),
	`And ( `BTerm ( `GE ( `ETerm (`Attribute ( `Qualified ("Input", "TIME2"))), `ETerm (`Attribute ( `Qualified ("Input", "TIME"))))),
	`And ( `BTerm ( `LE ( `ETerm (`Attribute ( `Qualified ("Input", "TIME2"))), `Sum ( `ETerm (`Attribute ( `Qualified ("Input", "TIME"))), `ETerm (`Int (30)) ))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "ID11"))), `ETerm (`Attribute ( `Qualified ("Input", "ID"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "POS11"))), `ETerm (`Attribute ( `Qualified ("Input", "POS"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "LANE11"))), `ETerm (`Attribute ( `Qualified ("Input", "LANE"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "DIR11"))), `ETerm (`Attribute ( `Qualified ("Input", "DIR"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "TIME11"))), `Sum ( `ETerm (`Attribute ( `Qualified ("Input", "TIME"))), `ETerm (`Int (120)) ))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "ID22"))), `ETerm (`Attribute ( `Qualified ("Input", "ID2"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "POS22"))), `ETerm (`Attribute ( `Qualified ("Input", "POS"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "LANE22"))), `ETerm (`Attribute ( `Qualified ("Input", "LANE"))))),
	`And ( `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "DIR22"))), `ETerm (`Attribute ( `Qualified ("Input", "DIR"))))),
	      `BTerm ( `EQ ( `ETerm (`Attribute ( `Qualified ("Input", "TIME22"))), `Sum ( `ETerm (`Attribute ( `Qualified ("Input", "TIME2"))), `ETerm (`Int (120)) ))
	)))))))))))))))))))

let input = `Relation("Input", [("TYPE", "int"); ("TIME", "int"); ("CARID", "int"); ("SPEED", "int"); ("XWAY", "int"); ("LANE", "int"); ("DIR", "int");
			("SEG", "int"); ("POS", "int"); ("QID", "int"); ("M_INIT", "int"); ("M_END", "int"); ("DOW", "int"); ("TOD", "int"); ("DAY", "int")])
let input2 = `Relation("Input", [("TYPE2", "int"); ("TIME2", "int"); ("CARID2", "int"); ("SPEED2", "int"); ("XWAY2", "int"); ("LANE2", "int"); ("DIR2", "int");
			("SEG2", "int"); ("POS2", "int"); ("QID2", "int"); ("M_INIT2", "int"); ("M_END2", "int"); ("DOW2", "int"); ("TOD2", "int"); ("DAY2", "int")])
let input11 = `Relation("Input", [("TYPE11", "int"); ("TIME11", "int"); ("CARID11", "int"); ("SPEED11", "int"); ("XWAY11", "int"); ("LANE11", "int"); ("DIR11", "int");
			("SEG11", "int"); ("POS11", "int"); ("QID11", "int"); ("M_INIT11", "int"); ("M_END11", "int"); ("DOW11", "int"); ("TOD11", "int"); ("DAY11", "int")])
let input22 = `Relation("Input", [("TYPE22", "int"); ("TIME22", "int"); ("CARID22", "int"); ("SPEED22", "int"); ("XWAY22", "int"); ("LANE22", "int"); ("DIR22", "int");
			("SEG22", "int"); ("POS22", "int"); ("QID22", "int"); ("M_INIT22", "int"); ("M_END22", "int"); ("DOW22", "int"); ("TOD22", "int"); ("DAY22", "int")])
let accident_rels = 
	`Cross ( `Select ( `And ( `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ( "Input", "SPEED" ))), `ETerm ( `Int (0)) )),
				  `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ( "Input", "TYPE" ))), `ETerm ( `Int (0)) ))),
			    input ),
	`Cross ( `Select ( `And ( `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ( "Input", "SPEED2"))), `ETerm ( `Int (0)) )),
				  `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ( "Input", "TYPE2" ))), `ETerm ( `Int (0)) ))),
			    input2 ),
	`Cross ( `Select ( `And ( `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ( "Input", "SPEED11"))), `ETerm ( `Int (0)) )),
				  `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ( "Input", "TYPE11" ))), `ETerm ( `Int (0)) ))),
			    input11 ),
		 `Select ( `And ( `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ( "Input", "SPEED22"))), `ETerm ( `Int (0)) )),
				  `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ( "Input", "TYPE22" ))), `ETerm ( `Int (0)) ))),
			    input22 )
	)))

let accident_select d s p = `Select ( accident_predicate "d" "s" "p", accident_rels)
			      
let accident_carid1 = `MapAggregate( `Min, `METerm ( `Attribute ( `Qualified ( "Input", "CARID"))), accident_select)
let accident_carid2 = `MapAggregate( `Max, `METerm ( `Attribute ( `Qualified ( "Input", "CARID2"))), accident_select)
let accident_firstM = `MapAggregate( `Min, `METerm ( `Attribute ( `Qualified ( "Input", "TIME"))), accident_select)
let accident_lastM = `MapAggregate( `Max, `METerm ( `Attribute ( `Qualified ( "Input", "TIME"))), accident_select)

let statistics_select m s d = 
			`Select( `And ( `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ("Input", "DIR"))), `ETerm (`Variable (d)) ) ),
				 `And ( `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ("Input", "SEG"))), `ETerm (`Variable (s)) ) ),
				 `And ( `BTerm ( `EQ ( `Sum ( `Divide ( `ETerm ( `Attribute ( `Qualified ("Input", "TIME"))), 
									`ETerm (`Int (60)) 
								      ),
							      `ETerm ( `Int (2))
							    ), 
						       `ETerm ( `Variable (m)) 
						     )),
				        `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ("Input", "TYPE"))), `ETerm (`Int (0)) ) )
				      ))), input ) 

let statistics_numvehicles m s d= `MapAggregate( `Sum, `METerm ( `Int (1)), statistics_select m s d)

(*
let statistics_toll_predicate1 m s d = 
			`And ( `BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ("Input", "DIR"))), `ETerm (`Variable (d)) )),
				`BTerm ( `EQ ( `ETerm ( `Attribute ( `Qualified ("Input", "

						
(*let toll = 
	`Product ( `METerm ( `Int ( 2)) ,
	`Product ( `Minus (statistics_numvehicles, `METerm ( `Int (50))),
		   `Minus (statistics_numvehicles, `METerm ( `Int (50))))) *)



(*let _ = print_endline (string_of_map_expression (statistics_numvehicles "m" "s" "d"));()*) 
let _ = print_endline (string_of_plan (statistics_select "m" "s" "d"));()
*)
