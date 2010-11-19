open Util
open K3OCamlgen
open K3
;;

let run_test (name:string) (test:K3O.code_t) 
             (rtype:K3O.type_t) (result:string) =
   Debug.log_unit_test
      ("K3 Ocaml "^name) 
      (fun x -> x)
      (K3O.debug_string test)
      ((K3O.string_of_type rtype)^": "^(result))
;;
let tcode ?(t=K3O.Float) txt = (IP.Leaf("<<"^txt^">>"),t);;
let code_list = List.map tcode ["x";"y";"z"];;
let arg_list = List.map (fun x -> ("<<"^x^">>",K3.SR.TFloat)) ["x";"y";"z"];;

(************************* General Functionality Test **********************)

module IP = IndentedPrinting
module K3OG = K3Compiler.Make(K3CG)
;;

Debug.log_unit_test 
   "K3 Ocaml project a tuple"
   (fun x->x)
   (K3CG.to_string (K3OG.compile_k3_expr
      (SR.Project(
         (SR.Tuple[SR.Const(M3.CFloat(2.));
                   SR.Const(M3.CFloat(3.));
                   SR.Var("bob", SR.TFloat)]),
         [0;2]
      ))
   ))
   "match (((K3Value.Float(2.)),(K3Value.Float(3.)),(var_bob))) with 
       (p_0,_,p_2) -> (p_0,p_2) | _ -> failwith \"compiler error\"\n"
;;

(************************* Thorough Functionality Test **********************)

run_test "simple const" (K3O.const (M3.CFloat(0.5))) 
         K3O.Float "K3Value.Float(0.5)"
;;
run_test "simple var" (K3O.var "foo" K3.SR.TFloat) K3O.Float "var_foo"
;;
run_test "simple tuple" (K3O.tuple code_list) 
         (K3O.Tuple(List.map snd code_list)) 
         "((<<x>>),(<<y>>),(<<z>>))"
;;
run_test "simple project" 
         (K3O.project (IP.Leaf("<t>"),
                       K3O.Tuple([K3O.Float;K3O.Float;K3O.Bool]))
                      [0;2])
         (K3O.Tuple[K3O.Float;K3O.Bool])
         "match (<t>) with (p_0,_,p_2) -> (p_0,p_2) | _ -> failwith \"compiler error\""
;;
run_test "simple singleton"
         (K3O.singleton (tcode "d") K3.SR.TFloat)
         (K3O.Collection(K3O.Inline,[],K3O.Float))
         "(<<d>>)"
;;
run_test ("simple combine (i-i)")
         (K3O.combine (tcode ~t:(K3O.Collection(K3O.Inline,[],K3O.Float)) "a") 
                      (tcode ~t:(K3O.Collection(K3O.Inline,[],K3O.Float)) "b"))
         (K3O.Collection(K3O.Inline,[],K3O.Float))
         "(<<a>>) @ (<<b>>)"
;;
run_test ("simple combine (s-s)")
         (K3O.combine (tcode ~t:(K3O.Collection(K3O.DBEntry,[],K3O.Float)) "a") 
                      (tcode ~t:(K3O.Collection(K3O.DBEntry,[],K3O.Float)) "b"))
         (K3O.Collection(K3O.Inline,[],K3O.Float))
         "(MC.fold ((fun k_wrapped v acc -> let k =  match k_wrapped with [] -> () | _ -> failwith \"boom\"  in  let (_,key_0) = (k,v) in (key_0)::acc)) ([]) (<<a>>)) @ (MC.fold ((fun k_wrapped v acc -> let k =  match k_wrapped with [] -> () | _ -> failwith \"boom\"  in  let (_,key_0) = (k,v) in (key_0)::acc)) ([]) (<<b>>))"
;;
run_test ("simple combine (s-i)")
         (K3O.combine (tcode ~t:(K3O.Collection(K3O.DBEntry,[],K3O.Float)) "a") 
                      (tcode ~t:(K3O.Collection(K3O.Inline,[],K3O.Float)) "b"))
         (K3O.Collection(K3O.Inline,[],K3O.Float))
         "(MC.fold ((fun k_wrapped v acc -> let k =  match k_wrapped with [] -> () | _ -> failwith \"boom\"  in  let (_,key_0) = (k,v) in (key_0)::acc)) ([]) (<<a>>)) @ (<<b>>)"
;;
run_test ("simple combine (i-s)")
         (K3O.combine (tcode ~t:(K3O.Collection(K3O.Inline,[],K3O.Float)) "a") 
                      (tcode ~t:(K3O.Collection(K3O.DBEntry,[],K3O.Float)) "b"))
         (K3O.Collection(K3O.Inline,[],K3O.Float))
         "(<<a>>) @ (MC.fold ((fun k_wrapped v acc -> let k =  match k_wrapped with [] -> () | _ -> failwith \"boom\"  in  let (_,key_0) = (k,v) in (key_0)::acc)) ([]) (<<b>>))"
;;
List.iter (fun (op_str, op, rtype) ->
      run_test ("simple op("^op_str^")")
               (K3O.op op (tcode "a") (tcode "b"))
               rtype
               (let v = "(match (<<a>>) with K3Value.Float(f) -> f | _ -> failwith \"boom\") "^op_str^" (match (<<b>>) with K3Value.Float(f) -> f | _ -> failwith \"boom\")"
               in if rtype = K3O.Float then "K3Value.Float("^v^")" else v)
   )
   [  "+.",  K3O.add_op,   K3O.Float;
      "*.",  K3O.mult_op,  K3O.Float;
      "=",   K3O.eq_op,    K3O.Bool;
      "<>",  K3O.neq_op,   K3O.Bool;
      "<",   K3O.lt_op,    K3O.Bool;
      "<=",  K3O.leq_op,   K3O.Bool
   ]
;;
run_test "simple op(if)"
         (K3O.op K3O.ifthenelse0_op (tcode ~t:K3O.Bool "a") (tcode "b"))
         K3O.Float
         "K3Value.Float(if (<<a>>) then (match (<<b>>) with K3Value.Float(f) -> f | _ -> failwith \"boom\") else 0.)"
;;
run_test "simple ifthenelse"
         (K3O.ifthenelse (tcode ~t:K3O.Bool "a") (tcode "b") (tcode "c"))
         K3O.Float
         "if (<<a>>) then (<<b>>) else (<<c>>)"
;;
run_test "block"
         (K3O.block code_list)
         K3O.Float
         "(<<x>>); (<<y>>); (<<z>>)"
;;
run_test "iterate (i)"
         (K3O.iterate (tcode ~t:(K3O.Fn([K3O.Float],K3O.Float)) "a") 
                      (tcode ~t:(K3O.Collection(K3O.Inline,[],K3O.Float)) "b"))
         K3O.Unit
         "List.iter (<<a>>) (<<b>>)"
;;
run_test "iterate (s)"
         (K3O.iterate (tcode ~t:(K3O.Fn([K3O.Float],K3O.Float)) "a") 
                      (tcode ~t:(K3O.Collection(K3O.DBEntry,[],K3O.Float)) "b"))
         K3O.Unit
         "List.iter (<<a>>) (MC.fold ((fun k_wrapped v acc -> let k =  match k_wrapped with [] -> () | _ -> failwith \"boom\"  in  let (_,key_0) = (k,v) in (key_0)::acc)) ([]) (<<b>>))"
;;
run_test "lambda (var)"
         (K3O.lambda (K3.SR.AVar("<<x>>",K3.SR.TFloat)) (tcode "a"))
         (K3O.Fn([K3O.Float],K3O.Float))
         "(fun var_<<x>> -> (<<a>>))"
;;
run_test "lambda (tuple)"
         (K3O.lambda (K3.SR.ATuple(arg_list)) (tcode "a"))
         (K3O.Fn([K3O.Tuple([K3O.Float;K3O.Float;K3O.Float])],K3O.Float))
         "(fun (var_<<x>>,var_<<y>>,var_<<z>>) -> (<<a>>))"
;;
run_test "assoc_lambda (t-t)"
         (K3O.assoc_lambda (K3.SR.ATuple(arg_list)) 
                           (K3.SR.ATuple(arg_list)) 
                           (tcode "a"))
         (K3O.Fn([K3O.Tuple([K3O.Float;K3O.Float;K3O.Float]);
                  K3O.Tuple([K3O.Float;K3O.Float;K3O.Float])],K3O.Float))
         "(fun (var_<<x>>,var_<<y>>,var_<<z>>) (var_<<x>>,var_<<y>>,var_<<z>>) -> (<<a>>))"
;;
run_test "apply"
         (K3O.apply (tcode ~t:(K3O.Fn([K3O.Tuple([K3O.Float;K3O.Float])],
                                       K3O.Float)) "a")
                    (tcode ~t:(K3O.Tuple([K3O.Float;K3O.Float])) "b"))
         (K3O.Float)
         "(<<a>>) (<<b>>)"
;;
run_test "map (i)"
         (K3O.map (tcode ~t:(K3O.Fn([K3O.Tuple[K3O.Float;K3O.Float]],
                                    (K3O.Tuple[K3O.Float;K3O.Float]))) "a")
                  (K3.SR.TFloat)
                  (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float],
                                            K3O.Float)) "b"))
         (K3O.Collection(K3O.Inline,[K3O.Float],K3O.Float))
         "List.map ((fun (key_0,key_1) -> ((<<a>>) (key_0,key_1)))) (<<b>>)"
;;
run_test "map (s)"
         (K3O.map (tcode ~t:(K3O.Fn([K3O.Tuple[K3O.Float;K3O.Float]],
                                    (K3O.Tuple[K3O.Float;K3O.Float]))) "a")
                  (K3.SR.TFloat)
                  (tcode ~t:(K3O.Collection(K3O.DBEntry,[K3O.Float],
                                            K3O.Float)) "b"))
         (K3O.Collection(K3O.DBEntry,[K3O.Float],K3O.Float))
         "MC.mapi ((fun wrapped_key map_value -> let key_0 = match wrapped_key with [key_0] -> (key_0) | _ -> failwith \"boom\" in  let key_0,key_1 = ((<<a>>) (key_0,map_value)) in  (([ key_0 ]),key_1))) (<<b>>)"
;;
run_test "aggregate (i)"
         (K3O.aggregate (tcode ~t:(K3O.Fn([K3O.Tuple([K3O.Float;
                                                      K3O.Float]);
                                                      K3O.Float],
                                          K3O.Float)) "a")
                        (tcode "b")
                        (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float],
                                                  K3O.Float)) "c"))
         (K3O.Float)
         "List.fold_left ((fun accum (key_0,key_1) -> ((<<a>>) (key_0,key_1) accum))) (<<b>>) (<<c>>)"
;;
run_test "aggregate (s)"
         (K3O.aggregate (tcode ~t:(K3O.Fn([K3O.Tuple([K3O.Float;
                                                      K3O.Float]);
                                                      K3O.Float],
                                          K3O.Float)) "a")
                        (tcode "b")
                        (tcode ~t:(K3O.Collection(K3O.DBEntry,[K3O.Float],
                                                  K3O.Float)) "c"))
         (K3O.Float)
         "MC.fold ((fun wrapped_key key_1 accum -> let key_0 = match wrapped_key with [key_0] -> (key_0) | _ -> failwith \"boom\" in  ((<<a>>) (key_0,key_1) accum))) (<<b>>) (<<c>>)"
;;
run_test "group_by_aggregate (i)"
         (K3O.group_by_aggregate
            (tcode ~t:(K3O.Fn([K3O.Tuple([K3O.Float;
                                          K3O.Float;
                                          K3O.Float]);
                                          K3O.Float],
                              K3O.Float)) "a")
            (tcode "b")
            (tcode ~t:(K3O.Fn([K3O.Tuple([K3O.Float;
                                          K3O.Float;
                                          K3O.Float])],
                              K3O.Float)) "c")
            (tcode ~t:(K3O.Collection(K3O.Inline,
                                      [K3O.Float;K3O.Float],
                                      K3O.Float)) "d"))
         (K3O.Collection(K3O.DBEntry,[K3O.Float],K3O.Float))
         "List.fold_left ((fun accum (key_0,key_1,key_2) -> let (group_0) = (( <<c>> ) (key_0,key_1,key_2)) in let group = [ group_0 ] in  let old_value = (if MC.mem group accum then MC.find group accum else ( <<b>> )) in MC.add group (((<<a>>) (key_0,key_1,key_2) old_value)) accum)) (K3ValuationMap.empty_map ()) (<<d>>)"
;;
run_test "flatten (i-i)"
         (K3O.flatten
            (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float],
                      (K3O.Collection(K3O.Inline,[K3O.Float],K3O.Float)))) "a"))
         (K3O.Collection(K3O.Inline,[K3O.Float;K3O.Float],K3O.Float))
         "List.flatten (List.map (fun (key_0,inner_map) ->  List.map (fun (key_1,key_2) ->  (key_0,key_1,key_2)) (inner_map)) (<<a>>))"
;;
run_test "flatten (s-i)"
         (K3O.flatten
            (tcode ~t:(K3O.Collection(K3O.DBEntry,[K3O.Float],
                      (K3O.Collection(K3O.Inline,[K3O.Float],K3O.Float)))) "a"))
         (K3O.Collection(K3O.Inline,[K3O.Float;K3O.Float],K3O.Float))
         "MC.fold (fun (key_0) inner_map accum -> ( List.map (fun (key_1,key_2) ->  (key_0,key_1,key_2)) (inner_map) ) @ accum) ([]) (<<a>>)"
;;
run_test "flatten (i-s)"
         (K3O.flatten
            (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float],
                      (K3O.Collection(K3O.DBEntry,[K3O.Float],K3O.Float)))) "a"))
         (K3O.Collection(K3O.Inline,[K3O.Float;K3O.Float],K3O.Float))
         "List.fold_left (fun (key_0,inner_map) accum -> ( MC.fold (fun (key_1) key_2 inner_accum ->  (key_0,key_1,key_2)::inner_accum) ([]) (inner_map) ) @ accum) ([]) (<<a>>)"
;;
run_test "flatten (s-s)"
         (K3O.flatten
            (tcode ~t:(K3O.Collection(K3O.DBEntry,[K3O.Float],
                      (K3O.Collection(K3O.DBEntry,[K3O.Float],K3O.Float)))) "a"))
         (K3O.Collection(K3O.Inline,[K3O.Float;K3O.Float],K3O.Float))
         "MC.fold (fun (key_0) inner_map accum -> ( MC.fold (fun (key_1) key_2 inner_accum ->  (key_0,key_1,key_2)::inner_accum) ([]) (inner_map) ) @ accum) ([]) (<<a>>)"
;;
run_test "exists (i)"
         (K3O.exists
            (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float;K3O.Float],
                                      K3O.Float)) "a")
            [(tcode "b");(tcode "c")]
         )
         (K3O.Bool)
         "List.exists (let comparison = ( ((<<b>>),(<<c>>)) ) in (fun (key_0,key_1,_) -> (key_0,key_1) = comparison)) (<<a>>)"
;;
run_test "exists (s)"
         (K3O.exists
            (tcode ~t:(K3O.Collection(K3O.DBEntry,[K3O.Float;K3O.Float],
                                      K3O.Float)) "a")
            [(tcode "b");(tcode "c")]
         )
         (K3O.Bool)
         "MC.mem ([(<<b>>);(<<c>>)]) (<<a>>)"
;;
run_test "lookup (i)"
         (K3O.lookup
            (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float;K3O.Float],
                                      K3O.Float)) "a")
            [(tcode "b");(tcode "c")]
         )
         (K3O.Float)
         "List.find (let comparison = ( ((<<b>>),(<<c>>)) ) in (fun (key_0,key_1,_) -> (key_0,key_1) = comparison)) (<<a>>)"
;;
run_test "lookup (s)"
         (K3O.lookup
            (tcode ~t:(K3O.Collection(K3O.DBEntry,[K3O.Float;K3O.Float],
                                      K3O.Float)) "a")
            [(tcode "b");(tcode "c")]
         )
         (K3O.Float)
         "MC.find ([(<<b>>);(<<c>>)]) (<<a>>)"
;;
run_test "slice (i)"
         (K3O.slice 
            (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float;K3O.Float],
                                      K3O.Float)) "a")
            [(tcode "b")]
            [1])
         (K3O.Collection(K3O.Inline,[K3O.Float;K3O.Float],K3O.Float))
         "let pkey = [(<<b>>)] in List.fold_left ((fun accum (key_0,key_1) ->  if (key_1) = pkey then accum@[key_0,key_1] else accum)) (<<a>>)"
;;
run_test "slice (s)"
         (K3O.slice 
            (tcode ~t:(K3O.Collection(K3O.DBEntry,[K3O.Float;K3O.Float],
                                      K3O.Float)) "a")
            [(tcode "b")]
            [1])
         (K3O.Collection(K3O.DBEntry,[K3O.Float;K3O.Float],K3O.Float))
         "MC.slice ([ 1 ]) ([(<<b>>)]) (<<a>>)"
;;
run_test "get_value"
         (K3O.get_value K3.SR.TFloat "<<a>>")
         K3O.Float
         "match (DB.get_value \"<<a>>\" dbtoaster_db) with Some(x) -> x | None -> K3Value.Float(0.0)"
;;
run_test "get_in_map"
         (K3O.get_in_map ["a",K3.SR.TFloat;"b",K3.SR.TFloat] 
                         K3.SR.TFloat 
                         "<<a>>")
         (K3O.Collection(K3O.DBEntry,[K3O.Float;K3O.Float],K3O.Float))
         "DB.get_in_map \"<<a>>\" dbtoaster_db"
;;
run_test "get_out_map"
         (K3O.get_out_map ["a",K3.SR.TFloat;"b",K3.SR.TFloat]
                         K3.SR.TFloat 
                          "<<a>>")
         (K3O.Collection(K3O.DBEntry,[K3O.Float;K3O.Float],K3O.Float))
         "DB.get_out_map \"<<a>>\" dbtoaster_db"
;;
run_test "get_map"
         (K3O.get_map (["a",K3.SR.TFloat;"b",K3.SR.TFloat],
                       ["c",K3.SR.TFloat;"d",K3.SR.TFloat])
                      K3.SR.TFloat 
                      "<<a>>")
         (K3O.Collection(K3O.DBEntry,[K3O.Float;K3O.Float],
                         K3O.Collection(K3O.DBEntry,[K3O.Float;K3O.Float],
                                        K3O.Float)))
         "DB.get_map \"<<a>>\" dbtoaster_db"
;;
run_test "update_value"
         (K3O.update_value "<<a>>" (tcode "b"))
         K3O.Unit
         "DB.update_value \"<<a>>\" (<<b>>) (dbtoaster_db)"
;;
run_test "update_in_map_value"
         (K3O.update_in_map_value "<<a>>" [(tcode "b")] (tcode "c"))
         (K3O.Unit)
         "DB.update_in_map_value \"<<a>>\" ([(<<b>>)]) (<<c>>) (dbtoaster_db)"
;;
run_test "update_out_map_value"
         (K3O.update_out_map_value "<<a>>" [(tcode "b")] (tcode "c"))
         (K3O.Unit)
         "DB.update_out_map_value \"<<a>>\" ([(<<b>>)]) (<<c>>) (dbtoaster_db)"
;;
run_test "update_map_value"
         (K3O.update_map_value "<<a>>" [(tcode "b")] [(tcode "c")] (tcode "d"))
         (K3O.Unit)
         "DB.update_map_value \"<<a>>\" ([(<<b>>)]) ([(<<c>>)]) (<<d>>) (dbtoaster_db)"
;;
run_test "update_in_map (i)"
         (K3O.update_in_map "<<a>>" 
            (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float],K3O.Float)) "b"))
         (K3O.Unit)
         "DB.update_in_map \"<<a>>\" (MC.from_list (List.map ((fun (key_0,key_1) -> [ key_0; key_1 ])) (<<b>>))) (dbtoaster_db)"
;;
run_test "update_out_map (i)"
         (K3O.update_out_map "<<a>>" 
            (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float],K3O.Float)) "b"))
         (K3O.Unit)
         "DB.update_out_map \"<<a>>\" (MC.from_list (List.map ((fun (key_0,key_1) -> [ key_0; key_1 ])) (<<b>>))) (dbtoaster_db)"
;;
run_test "update_map (i)"
         (K3O.update_map "<<a>>" [(tcode "b")] 
            (tcode ~t:(K3O.Collection(K3O.Inline,[K3O.Float],K3O.Float)) "c"))
         (K3O.Unit)
         "DB.update_map \"<<a>>\" ([(<<b>>)]) (MC.from_list (List.map ((fun (key_0,key_1) -> [ key_0; key_1 ])) (<<c>>))) (dbtoaster_db)"
;;
run_test "update_map (s)"
         (K3O.update_map "<<a>>" [(tcode "b")] 
            (tcode ~t:(K3O.Collection(K3O.DBEntry,[K3O.Float],K3O.Float)) "c"))
         (K3O.Unit)
         "DB.update_map \"<<a>>\" ([(<<b>>)]) (List.fold_left (MC.add_secondary_index) (<<c>>) (DB.get_out_patterns \"<<a>>\" dbtoaster_db)) (dbtoaster_db)"
;;
run_test "main"
   (K3O.main 
      []
      [("<<a>>",[M3.VT_Int;M3.VT_Int],[M3.VT_Int;M3.VT_Int]);
       ("<<b>>",[M3.VT_Int],[M3.VT_Int;M3.VT_Int])]
      [("<<a>>",[M3Common.Patterns.In(["a";"b"],[0;1])])]
      [(IP.Leaf("<<c>>"),None,None)]
      []
      ["<<a>>"]
   )
   (K3O.Unit)
   "open Util
open M3
open M3Common
open Values
open Sources
open Database
open K3
;;

module MC = K3ValuationMap module DB = NamedK3Database module RT = Runtime.Make(DB)
;;

StandardAdaptors.initialize (); let schema = [(\"<<a>>\", [ M3.VT_Int; M3.VT_Int ], [ M3.VT_Int; M3.VT_Int ]);(\"<<b>>\", [ M3.VT_Int ], [ M3.VT_Int; M3.VT_Int ])] in  let patterns = [(\"<<a>>\",[ Patterns.In([ \"a\"; \"b\" ],[ 0; 1 ]) ])] in  let dbtoaster_db = DB.make_empty_db schema patterns in  let mux = List.fold_left (FileMultiplexer.add_stream) (FileMultiplexer.create ()) ([(<<c>>)]) in  let tlqs = List.map (DB.string_to_map_name) ([(\"<<a>>\")]) in  let wrap_datum v = match v with M3.CFloat(f) -> K3Value.Float(f)  in  let dispatcher event = match event with  | Some(M3.Insert,rel,_) -> failwith (\"Unknown event Insert(\"^rel^\")\") | Some(M3.Delete,rel,_) -> failwith (\"Unknown event Delete(\"^rel^\")\") | None -> false in  RT.synch_main (dbtoaster_db) (mux) (tlqs) (dispatcher) (RT.main_args ()) ()"
;;
