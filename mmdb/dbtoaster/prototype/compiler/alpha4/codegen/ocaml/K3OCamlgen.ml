(* K3 OCaml Generator
 * A backend that produces OCaml source code for K3 programs.
 *)

open Util
open M3
open K3
open Database

module IP = IndentedPrinting
 
module K3O =
struct
   type collection_type_t = Inline | DBEntry
   type type_t   = 
      | Float
      | Bool
      | Unit
      | Fn of type_t list * type_t
      | Tuple of type_t list
      | Collection of collection_type_t * type_t list * type_t
      | Trigger of M3.pm_t * M3.rel_id_t * M3.var_t list
   
   type code_t   = IP.t * type_t
   type op_t     = ((IP.t -> IP.t -> IP.t) * type_t * type_t * type_t)
   
   type db_t              = unit
   
   type source_impl_t = IP.t
   
   (********************** Debugging ***********************)
   
   let rec string_of_type t = 
      match t with Float -> "float" | Bool -> "bool" | Unit -> "unit"
         | Fn(arg,ret) -> 
            "fn("^(Util.string_of_list ", " (List.map string_of_type arg))^
            " -> "^(string_of_type ret)^")"
         | Tuple(elems) -> 
            "["^(Util.string_of_list ", " (List.map string_of_type elems))^"]"
         | Collection(ctype,k,v) ->
            (match ctype with Inline -> "inline"
                            | DBEntry -> "dbentry")^
            "_map( ["^(Util.string_of_list ", " (List.map string_of_type k))^
            "] -> "^(string_of_type v)^")"
         | Trigger(pm,rel,vars) ->
            "{on_"^(match pm with Insert -> "insert" | Delete -> "delete")^rel^
            "("^(Util.string_of_list "," vars)^")}"
   
   let debug_string ((x,_):code_t):string =
      (IndentedPrinting.to_debug_line x)
   
   let debugfail (expr:(K3.SR.expr_t option)) (msg:string) =
      failwith ("K3OCamlgen Internal Error ["^msg^"]; Expression: "^
         (match expr with | None -> "{Not Available}"
            | Some(s) -> K3.SR.code_of_expr s))
            
   
   (********************** Utility **********************)
   
   let parens = ("(",")")
   let brackets = ("[","]")
   let num_op opcode = (fun a b -> 
      IP.Node(parens,(") "^opcode," ("),a,b))
   let mk_list (elems:code_t list): IP.t =
      IP.TList(brackets,Some(parens),";",(List.map fst elems)) 
   let mk_tuple (elems:code_t list): IP.t =
      IP.TList(("(",")"),Some(parens),",",(List.map fst elems)) 
   let mk_let var v = 
      IP.Node(("let "," in "),(" = ",""),(IP.Leaf(var)),v)
      
   (* Function invocation *)
   let apply_one fn arg =
      IP.Node(("",")"),(" ","("),IP.Leaf(fn),arg)
   let apply_many fn arg_list =
      IP.Parens((fn^" ",""),IP.TList(("",""),Some(parens)," ",arg_list))
   
   let wrap_function (args:string) (retwrap:(IP.t->IP.t) option) 
                     (params:string) (fn:IP.t) =
      let real_retwrap = 
         match retwrap with Some(s) -> s | None -> (fun x->x)
      in
         (IP.Node(parens,(" -> ",""),
                 IP.Leaf("fun "^args),
                 (real_retwrap (IP.Parens(("((",(") ("^params^"))")), fn)))
         ))
   
   (* Tuple operations *)
   let arg_tuple a_list =
      (Util.string_of_list "," (List.map fst a_list))
   
   let key_tuple ?(init_id = 0) ?(tuplizer = arg_tuple) t_list = 
      tuplizer (snd 
         (List.fold_left 
            (fun (i,accum) _ -> 
               (i+1,accum@["key_"^(string_of_int i),()])) 
            (init_id,[]) t_list
         ))
   
   let pattern_tuple _ pattern = 
      (arg_tuple (List.map (fun x-> ("key_"^(string_of_int x),())) pattern))
   
   let inv_pattern_tuple keys pattern = 
      (arg_tuple 
         (snd (List.fold_left (fun (i,accum) _ -> 
                  if List.mem i pattern 
                  then (i+1,accum)
                  else (i+1,accum@["key_"^(string_of_int i),()])) (0,[]) keys)))
   
   let fold_map map fn =
      (apply_one ("MC.fold (fun k v acc -> "^fn^"::acc) []") map)
   
   let kv_pair_to_keytuple_vars ?(init_id = 0) t_list sep = 
      (if (List.length t_list) = 0 then "_" else 
         "("^(key_tuple ~init_id:init_id t_list)^")"
      )^sep^"key_"^(string_of_int (init_id+(List.length t_list)))

   let merge_key_value t_list kv_pair =
      ("let ("^(kv_pair_to_keytuple_vars t_list ",")^
       ") = "^kv_pair^" in ("^(key_tuple (t_list@[Unit]))^")")
   
   let inline_map map t_list =
      (fold_map map (merge_key_value t_list "(k,v)"))
   
   let map_to_update ((map,mapt):code_t) (patterns:IP.t): IP.t =
      match mapt with 
      | Collection(Inline,kt,vt) -> 
         (apply_one "MC.from_list"
            (apply_many "List.map" [
               IP.Leaf("(fun ("^(key_tuple (kt@[vt]))^") -> "^
                        (key_tuple ~tuplizer:(Util.list_to_string 
                                                         (fun (x,_)->x)) 
                                   (kt@[vt]))^")");
               map
            ]))
      | Collection(DBEntry,kt,vt) -> 
         (apply_many "List.fold_left" [
            IP.Leaf("MC.add_secondary_index");
            map;
            patterns
         ])
      | _ -> debugfail None "Attempting to use a non-collection to update a map"
   
   (* Type operations *)
   let rec map_type t =
      match t with
      |  SR.TUnit -> Unit
      |  SR.TFloat ->Float
      |  SR.TInt -> Float
      |  SR.TTuple(tlist) -> Tuple(List.map map_type tlist)
         (* blech! This type should have the key arity at least *)
      |  SR.Collection(t) -> Collection(Inline,[],map_type t) 
      |  SR.Fn(argtlist,rett) -> 
            Fn(List.map map_type argtlist, map_type rett)
   
   let fn_ret_type fnt =
      let rett,otherargt = match fnt with 
         | Fn(fnargt::otherargt,fnrett) -> (fnrett,otherargt)
         | _ -> debugfail None "invalid apply, applying to non-function"
      in
         if otherargt = [] then rett else Fn(otherargt,rett)
   
   let mapkey_of_codekey (t:code_t list) =
      IP.TList(brackets,Some(parens),";",(List.map fst t))
      
   (********************** Algebra **********************)
   
   (* Generic Constructors *)
   let f_op op = ((num_op op),Float,Float,Float)
   let c_op op = ((num_op op),Float,Float,Bool)
   
   let add_op         : op_t = (f_op "+.")
   let mult_op        : op_t = (f_op "*.")
   let eq_op          : op_t = (c_op "=")
   let neq_op         : op_t = (c_op "<>")
   let lt_op          : op_t = (c_op "<")
   let leq_op         : op_t = (c_op "<=")
   let ifthenelse0_op : op_t = ((fun a b -> 
      IP.Node(("if (",") else 0"),(") ", "then ("), a, b)),
      Bool,Float,Bool)

   (********************** Implementation **********************)

   (* Terminals *)
   let const ?(expr = None) (c:M3.const_t): code_t =
      match c with CFloat(y) -> (IP.Leaf(string_of_float y), Float)
   
   let var ?(expr = None) (v:M3.var_t) (t:K3.SR.type_t): code_t =
      (IP.Leaf("var_"^v), Float)
   
   (* Tuples *)
   let tuple ?(expr = None) (c:code_t list): code_t =
      if (List.length c) = 0 then debugfail expr "Empty List"
      else  let t = (List.map (fun (_,t)->t) c) 
            in (mk_tuple c, Tuple(t))
   
   let project ?(expr = None) ((tuple,tuplet):code_t) (f:int list): code_t =
      let tuple_members = 
         match tuplet with Tuple(t) -> t 
                          | _ -> debugfail expr "projecting a non-tuple"
      in
      let _,in_list,out_list,t = 
         List.fold_left (fun (i,in_list,out_list,t_accum) t -> 
            (i+1, 
             in_list@[if (List.mem i f) then "p_"^(string_of_int i) else "_"],
             out_list@(if (List.mem i f) then ["p_"^(string_of_int i)] else []),
             t_accum@(if (List.mem i f) then [t] else []))) 
            (0,[],[],[]) 
            tuple_members
      in
         ((IP.Node(("match ("," | _ -> failwith \"compiler error\""),
                   (") with ",""),tuple,
                   IP.Node(("",""),(" -> ",""),
                      IP.Leaf("("^(Util.string_of_list "," in_list)^")"),
                      IP.Leaf("("^(Util.string_of_list "," out_list)^")")
                   )
         )), Tuple(t))
   
   (* Native collections *)
   let singleton ?(expr = None) ((d,dt):code_t) (t:K3.SR.type_t): code_t =
      if dt <> (map_type t) 
      then debugfail expr "Type mismatch in singleton constructor"
      else ((IP.Parens(("(",")"),d)), 
            Collection(Inline,[],(map_type t)))
   
   let combine ?(expr = None) (a_code:code_t) (b_code:code_t): code_t =
      match (a_code,b_code) with 
         | ((a,Collection(act,ak,at)),
            (b,Collection(bct,bk,bt))) ->
            (IP.Node(parens,(") @ ","("),
               (if act == Inline then a else (inline_map a ak)),
               (if bct == Inline then b else (inline_map b bk))
            ), Collection(Inline,ak,at))
         | _ -> debugfail expr "Type mismatch for collection combine"
   
   (* Arithmetic, comparision operators *)
   (* op type, lhs, rhs -> op *)
   let op ?(expr = None) ((op,lt,rt,ot):op_t) ((a,at):code_t) ((b,bt):code_t): 
          code_t =
      if at <> lt then 
         debugfail expr ("Type mismatch in arithmetic lvalue: "^
                         (string_of_type at)^" != "^(string_of_type lt)) else
      if bt <> rt then 
         debugfail expr ("Type mismatch in arithmetic rvalue: "^
                         (string_of_type bt)^" != "^(string_of_type rt)) else
            ((op a b),ot)
   
   (* Control flow *)
   (* predicate, then clause, else clause -> condition *) 
   let ifthenelse ?(expr = None) ((i,it):code_t) ((t,tt):code_t) 
                  ((e,et):code_t): code_t =
      if it <> Bool then debugfail expr "Type mismatch: non-bool condition" else
      if tt <> et 
      then debugfail expr "Type mismatch: unaligned condition output" 
      else ((IP.Node(("if (",""),(")",""),i,
              IP.Node(
                 (" then (",")"),(") ","else ("), 
                 t, e
           ))),tt)
   
   (* statements -> block *)    
   let block ?(expr = None) (stmts:code_t list): code_t =
      ((IP.TList(("",""),Some(parens),"; ",(List.map fst stmts))),
         (  if (List.length stmts) < 1 then Unit
            else (snd (List.hd (List.rev stmts)))
         ))
   
   (* iter fn, collection -> iterate *)
   let iterate ?(expr = None) ((fn,fnt):code_t) ((c,ct):code_t): code_t =
      match ct with 
      | Collection(Inline,k,t) -> 
         ((apply_many "List.iterate" [fn;c]),Unit)
         
        (* Currently we convert to a list since SliceableMap doesn't implement
         * an 'iter' function. TODO: we could easily add this. *)
      | Collection(DBEntry,k,t) -> 
         ((apply_many "List.iterate" [fn;(inline_map c k)]),Unit)
      | _ -> debugfail expr ("Iterating over a non-collection")
   
   (* Functions *)

   (* arg, body -> fn *)
   let var_lambda (args:K3.SR.arg_t list) ((body,bodyt):code_t): code_t =
      let (args,argts) = List.split (List.map (fun a ->
         match a with
          | SR.AVar(v,vt) -> ("var_"^v, map_type vt)
          | SR.ATuple(tlist) -> 
            ("("^(arg_tuple (List.map (fun (x,t) -> ("var_"^x,t)) tlist))^")",
             Tuple(List.map (fun (_,t) -> map_type t) tlist))
         ) args)
      in
         ((IP.Node(("(fun ","))"),(" -> (",""),
            IP.Leaf(Util.string_of_list " " args),
            body)),
            Fn(argts,bodyt)
         )
   
   let lambda ?(expr = None) (args:K3.SR.arg_t) (body:code_t): code_t =
      var_lambda [args] body
   
   (* arg1, type, arg2, type, body -> assoc fn *)
   let assoc_lambda ?(expr = None) (arg1:K3.SR.arg_t) (arg2:K3.SR.arg_t)
                    (body:code_t): code_t =
      var_lambda [arg1;arg2] body
   
   (* fn, arg -> evaluated fn *)
   let apply ?(expr = None) ((fn,fnt):code_t) ((arg,argt):code_t): code_t =
      ((IP.Node(("(",")"),(") ","("),fn,arg)),(fn_ret_type fnt))
   
   (* Structural recursion operations *)
   (* map fn, collection -> map *)
   let map ?(expr = None) ((fn,fnt):code_t) (exprt:K3.SR.type_t) 
           ((coll,collt):code_t): code_t =
      match collt with
         | Collection(Inline,k,t) ->
            ((apply_many "List.map" [fn;coll]),
             Collection(Inline,[],(fn_ret_type fnt)))
         | Collection(DBEntry,k,t) ->
            ((apply_many "MC.map" [
               (wrap_function (kv_pair_to_keytuple_vars k " ")
                              None
                              ("("^(key_tuple (k@[t]))^")") 
                              fn);
               coll
             ]),
             Collection(Inline,[],(fn_ret_type fnt)))
         | _ -> debugfail expr ("Mapping a non-collection")
   
   (* agg fn, initial agg, collection -> agg *)
   (* Note: accumulator is the last arg to agg_fn *)
   let aggregate ?(expr = None) ((fn,fnt):code_t) ((init,initt):code_t) 
                 ((coll,collt):code_t): code_t =
      match collt with
         | Collection(Inline,k,t) ->
            ((apply_many "List.fold_left" [
               (wrap_function ("accum ("^(key_tuple (k@[t]))^")")
                              None
                              ("("^(key_tuple (k@[t]))^",accum)") 
                              fn);
               init;
               coll
            ]),(fn_ret_type fnt))
         | Collection(DBEntry,k,t) ->
            ((apply_many "MC.fold" [
               (wrap_function ((kv_pair_to_keytuple_vars k " ")^" accum")
                              None
                              ("("^(key_tuple (k@[t]))^",accum)") 
                              fn);
               init;
               coll
            ]),(fn_ret_type fnt))
         | _ -> debugfail expr ("Aggregating a non-collection")
   
   (* agg fn, initial agg, grouping fn, collection -> agg *)
   let group_by_aggregate ?(expr = None) ((agg,aggt):code_t) 
                          ((init,initt):code_t) ((group,groupt):code_t) 
                          ((coll,collt):code_t): code_t =
      let gb_aux k t = 
         (fun x -> IP.Lines([
               IP.Leaf("let group = ((");
                  group;
                  IP.Leaf(") ("^(key_tuple (k@[t]))^")) in");
               IP.Parens(("let old_value = (",") in"),
                  IP.Lines([
                     IP.Leaf("if MC.mem group accum");
                     IP.Leaf("then MC.find group accum");
                     IP.Leaf("else (");init;IP.Leaf(")")
                  ])
               );
               IP.Parens(("MC.add group (",") accum"),x);
            ]))
      in
      let gb_ret = 
         Collection(DBEntry,(
               match (fn_ret_type groupt) with 
               |  Tuple(t) -> t
               |  _ -> [(fn_ret_type groupt)]
            ), (fn_ret_type aggt))
      in
      match collt with
         | Collection(Inline,k,t) ->
            ((apply_many "List.fold_left" [
               (wrap_function ("accum ("^(key_tuple (k@[t]))^")")
                              (Some((gb_aux k t)))
                              ("("^(key_tuple (k@[t]))^",old_value)")
                              agg); 
               IP.Leaf("K3ValuationMap.empty_map ()");
               coll
            ]),gb_ret)
         | Collection(DBEntry,k,t) ->
            ((apply_many "MC.fold" [
               (wrap_function ((kv_pair_to_keytuple_vars k " ")^" accum")
                              (Some((gb_aux k t)))
                              ("("^(key_tuple (k@[t]))^",old_value)") 
                              agg);
               IP.Leaf("K3ValuationMap.empty_map ()");
               coll
            ]),gb_ret)
         | _ -> debugfail expr ("(GB)Aggregating a non-collection")
   
   (* nested collection -> flatten *)
   let flatten ?(expr = None) ((coll,collt):code_t): code_t =
      match collt with
      |  Collection(Inline,ko,Collection(Inline,ki,t)) ->
            ((apply_one "List.flatten" 
               (apply_many "List.map" [
                  IP.Lines([
                     IP.Leaf("fun ("^(key_tuple ko)^",inner_map) -> ");
                     (apply_many "List.map" [
                        IP.Lines([
                           IP.Leaf("fun ("^(key_tuple ~init_id:(List.length ko)
                                                      (ki@[t]))^") -> ");
                           IP.Leaf("("^(key_tuple (ko@ki@[t]))^")")
                        ]);
                        IP.Leaf("inner_map")
                     ])
                  ]);
                  coll
               ])
            ),Collection(Inline,ki@ko,t))
      |  Collection(DBEntry,ko,Collection(Inline,ki,t)) ->
            ((apply_many "MC.fold" [
               IP.Lines([
                  IP.Leaf("fun ("^(key_tuple ko)^") inner_map accum -> (");
                  (apply_many "List.map" [
                     IP.Lines([
                        IP.Leaf("fun ("^(key_tuple ~init_id:(List.length ko)
                                                   (ki@[t]))^") -> ");
                        IP.Leaf("("^(key_tuple (ko@ki@[t]))^")")
                     ]);
                     IP.Leaf("inner_map")
                  ]);
                  IP.Leaf(") @ accum")
               ]);
               IP.Leaf("[]");
               coll
            ]),Collection(Inline,ki@ko,t))
      |  Collection(Inline,ko,Collection(DBEntry,ki,t)) ->
            ((apply_many "List.fold_left" [
               IP.Lines([
                  IP.Leaf("fun ("^(key_tuple ko)^",inner_map) accum -> (");
                  (apply_many "MC.fold" [
                     IP.Lines([
                        IP.Leaf("fun "^(kv_pair_to_keytuple_vars 
                                                   ~init_id:(List.length ko)
                                                   ki " ")^" inner_accum -> ");
                        IP.Leaf("("^(key_tuple (ko@ki@[t]))^")::inner_accum")
                     ]);
                     IP.Leaf("[]");
                     IP.Leaf("inner_map")
                  ]);
                  IP.Leaf(") @ accum")
               ]);
               IP.Leaf("[]");
               coll
            ]),Collection(Inline,ki@ko,t))
      |  Collection(DBEntry,ko,Collection(DBEntry,ki,t)) ->
            ((apply_many "MC.fold" [
               IP.Lines([
                  IP.Leaf("fun ("^(key_tuple ko)^") inner_map accum -> (");
                  (apply_many "MC.fold" [
                     IP.Lines([
                        IP.Leaf("fun "^(kv_pair_to_keytuple_vars 
                                                   ~init_id:(List.length ko)
                                                   ki " ")^" inner_accum -> ");
                        IP.Leaf("("^(key_tuple (ko@ki@[t]))^")::inner_accum")
                     ]);
                     IP.Leaf("[]");
                     IP.Leaf("inner_map")
                  ]);
                  IP.Leaf(") @ accum")
               ]);
               IP.Leaf("[]");
               coll
            ]),Collection(Inline,ki@ko,t))
      |  _ -> debugfail expr "Trying to flatten a non-nested collection"
   
   (* Tuple collection operators *)
   (* TODO: write documentation on datatypes + implementation requirements
   * for these methods *)
   (* map, key -> bool/float *)
   let exists ?(expr = None) ((map,mapt):code_t) (key:code_t list): 
              code_t =
      match mapt with 
      |  Collection(Inline,kt,vt) ->
         ((apply_many "List.exists" [
            IP.Lines([
               IP.Leaf("let comparison = (");
               IP.TList(parens,Some(parens),",",(List.map fst key));
               IP.Leaf(") in");
               IP.Leaf("(fun ("^(key_tuple kt)^",_) -> ("^
                       (key_tuple kt)^") = cmp)")
            ]);
            map]),Bool)
      |  Collection(DBEntry,kt,vt) ->
         ((apply_many "MC.mem" [(mapkey_of_codekey key);map]),Bool)
      | _ -> debugfail expr "Existence check on a non-collection"
   
   (* map, key -> map value *)
   let lookup ?(expr = None) ((map,mapt):code_t) (key:code_t list): code_t =
      match mapt with 
      | Collection(Inline,kt,vt) ->
         ((apply_many "List.find" [
            IP.Lines([
               IP.Leaf("let comparison = (");
               IP.TList(parens,Some(parens),",",(List.map fst key));
               IP.Leaf(") in");
               IP.Leaf("(fun ("^(key_tuple kt)^",_) -> ("^
                       (key_tuple kt)^") = cmp)")
            ]);
            map]),vt)
      | Collection(DBEntry,kt,vt) ->
         ((apply_many "MC.find" [(mapkey_of_codekey key);map]),vt)
      | _ -> debugfail expr "Lookup on a non-collection"
      
   (* map, partial key, pattern -> slice *)
   (* TODO: see notes on datatype conversions/secondary indexes *)
   let slice ?(expr = None) ((map,mapt):code_t) (pkey:code_t list) 
             (pattern:int list): code_t =
      let sliced_keys klist = (List.map (fun x -> List.nth klist x)
                                        (List.sort compare pattern))
      in
      match mapt with 
      | Collection(Inline,kt,vt) ->
         ((IP.Lines([
            IP.Parens(("let pkey = "," in"),
               IP.TList(brackets,Some(parens),";",(List.map fst pkey)));
            (apply_many "List.fold_left" [
               IP.Lines([
                  IP.Leaf("(fun accum ("^(key_tuple kt)^") -> ");
                  IP.Leaf("if ("^(pattern_tuple kt pattern)^") = pkey");
                  IP.Leaf("then accum@["^(inv_pattern_tuple kt pattern)^"]");
                  IP.Leaf("else accum)")
               ]);
               map
            ])
         ])),Collection(Inline,(sliced_keys kt),vt))
      | Collection(DBEntry,kt,vt) ->
         ((apply_many "MC.slice" [
            IP.Leaf(Util.list_to_string string_of_int pattern);
            IP.TList(brackets,Some(parens),";",(List.map fst pkey));
            map
         ]),Collection(DBEntry,(sliced_keys kt),vt))
      | _ -> debugfail expr "Slice on a non-collection"
   
   (* Persistent collections *)
   
   (* M3 API *)
   
   
   (* Database retrieval methods *)
   let get_value ?(expr = None) (map:K3.SR.coll_id_t): code_t =
      ((IP.Leaf("match (DB.get_value \""^map^"\" dbtoaster_db) "^
              "with Some(x) -> x | None -> 0.0")), Float)

   let get_in_map  ?(expr = None) (schema:K3.SR.schema) (map:K3.SR.coll_id_t): 
                   code_t =
      ((IP.Leaf("DB.get_in_map \""^map^"\" dbtoaster_db")), 
       Collection(DBEntry,(List.map (fun (_,x) -> map_type x) schema),Float))

   let get_out_map ?(expr = None) (schema:K3.SR.schema) (map:K3.SR.coll_id_t): 
                   code_t =
      ((IP.Leaf("DB.get_out_map \""^map^"\" dbtoaster_db")), 
       Collection(DBEntry,(List.map (fun (_,x) -> map_type x) schema),Float))

   let get_map ?(expr = None) ((ins,outs):(K3.SR.schema*K3.SR.schema)) 
               (map:K3.SR.coll_id_t): code_t =
      ((IP.Leaf("DB.get_map \""^map^"\" dbtoaster_db")), 
       Collection(DBEntry,(List.map (fun (_,x) -> map_type x) ins),
                  Collection(DBEntry,(List.map (fun (_,x) -> map_type x) outs),
                             Float)))
   
   
   (* Database udpate methods *)
   (* persistent collection id, value -> update *)
   let update_value ?(expr = None) (map:K3.SR.coll_id_t) (v:code_t): code_t =
      ((apply_many ("DB.update_value \""^map^"\"") [
         (fst v);
         IP.Leaf("dbtoaster_db")
      ]),Unit)
   
   (* persistent collection id, in key, value -> update *)
   let update_in_map_value ?(expr = None) (map:K3.SR.coll_id_t) 
                           (key:code_t list) (v:code_t): code_t =
      ((apply_many ("DB.update_in_map_value \""^map^"\"") [
         IP.TList(brackets,Some(parens),";",(List.map fst key));
         (fst v);
         IP.Leaf("dbtoaster_db")
      ]),Unit)
   
   (* persistent collection id, out key, value -> update *)
   let update_out_map_value ?(expr = None) (map:K3.SR.coll_id_t) 
                            (key:code_t list) (v:code_t): code_t =
      ((apply_many ("DB.update_out_map_value \""^map^"\"") [
         IP.TList(brackets,Some(parens),";",(List.map fst key));
         (fst v);
         IP.Leaf("dbtoaster_db")
      ]),Unit)
   
   (* persistent collection id, in key, out key, value -> update *)
   let update_map_value ?(expr = None) (map:K3.SR.coll_id_t) (inkey:code_t list)
                        (outkey:code_t list) (v:code_t): code_t =
      ((apply_many ("DB.update_map_value \""^map^"\"") [
         IP.TList(brackets,Some(parens),";",(List.map fst inkey));
         IP.TList(brackets,Some(parens),";",(List.map fst outkey));
         (fst v);
         IP.Leaf("dbtoaster_db")
      ]),Unit)
   
   (* persistent collection id, updated collection -> update *)
   let update_in_map ?(expr = None) (map:K3.SR.coll_id_t) (v:code_t): code_t =
      ((apply_many ("DB.update_in_map \""^map^"\"") [
         (map_to_update v 
            (IP.Leaf("DB.get_in_patterns \""^map^"\" dbtoaster_db")));
         IP.Leaf("dbtoaster_db")
      ]),Unit)

   let update_out_map ?(expr = None) (map:K3.SR.coll_id_t) (v:code_t): code_t =
      ((apply_many ("DB.update_out_map \""^map^"\"") [
         (map_to_update v 
            (IP.Leaf("DB.get_out_patterns \""^map^"\" dbtoaster_db")));
         IP.Leaf("dbtoaster_db")
      ]),Unit)
   
   (* persistent collection id, key, updated collection -> update *)
   let update_map ?(expr = None) (map:K3.SR.coll_id_t) (inkey:code_t list) 
                  (v:code_t): code_t =
      ((apply_many ("DB.update_map \""^map^"\"") [
         IP.TList(brackets,Some(parens),";",(List.map fst inkey));
         (map_to_update v 
            (IP.Leaf("DB.get_out_patterns \""^map^"\" dbtoaster_db")));
         IP.Leaf("dbtoaster_db")
      ]),Unit)
   
   (* Toplevel: sources and main *)
   (* event, rel, trigger args, statement code block -> trigger code *)
   let trigger (ins_del:M3.pm_t) (rel:M3.rel_id_t) (vars:M3.var_t list)
               (code:code_t list): code_t =
      (IP.Node(("let ",";;"),(" = ",""),
         IP.Leaf("on_"^(if ins_del = Insert then "insert" else "delete")^
                 "_"^rel^" dbtoaster_db ("^
                 (arg_tuple (List.map (fun x -> ("var_x",())) vars))^
                 ")"),
         IP.Lines(List.map fst code)
      ),Trigger(ins_del, rel, vars))
   
   (* source type, framing type, (relation * adaptor type) list 
   * -> source impl type,
   *    source and adaptor declaration code, (optional)
   *    source and adaptor initialization code (optional) *)
   let source (source:M3.source_t) (framing:M3.framing_t)
              (relations:(string * M3.adaptor_t) list):
              (source_impl_t * code_t option * code_t option) =
      let framing_text = match framing with
         | FixedSize(i) -> "M3.FixedSize("^(string_of_int i)^")"
         | Delimited(str) -> "M3.FixedSize(\""^str^"\")"
         | VarSize(a,b) -> "M3.VarSize("^(string_of_int a)^","^
                                         (string_of_int b)^")"
      in
      let adaptor_text = (List.map (fun (rel,(atype,akeys)) -> 
            IP.Leaf("(\""^rel^"\", (Adaptors.create_adaptor (\""^atype^"\","^
                    (Util.list_to_string (fun (k,v) -> "\""^k^"\",\""^v^"\"")
                                         akeys)^
                    ")))")
         ) relations)
      in 
      let source_init = 
         match source with 
            | M3.FileSource(s) -> 
               (apply_many "FileSource.create" [
                  IP.Leaf("M3.FileSource(\""^s^"\")");
                  IP.Leaf("\""^framing_text^"\"");
                  IP.TList(brackets,Some(parens),";",adaptor_text)
               ])
            | M3.SocketSource(_,_) -> failwith "Sockets not implemented"
            | M3.PipeSource(_) -> failwith "Pipes not implemented"
      in
         (source_init, None, None)
   
   (* schema, patterns, source decls and inits, triggers, toplevel queries
    -> top level code *)
   let main (trigger_params:(string * Calculus.var_t list) list)
            (schema:M3.map_type_t list) (patterns:M3Common.Patterns.pattern_map)
            (sources:(source_impl_t * code_t option * code_t option) list)
            (triggers:code_t list) (tlqs:string list): code_t =
      let vt_string t = 
         match t with 
         | VT_String -> "M3.VT_String" 
         | VT_Int    -> "M3.VT_Int" 
         | VT_Float  -> "M3.VT_Float" 
      in
      (IP.TList(("",""),Some("",""),";;\n\n",[
         IP.Lines((List.map (fun x->IP.Leaf("open "^x)) [
            "Util";
            "M3";
            "M3Common";
            "Values";
            "Sources";
            "K3"
         ]));
         IP.Lines((List.map (fun (x,y)->IP.Leaf("module "^x^" = "^y)) [
            "MC","K3ValuationMap";
            "DB","NamedK3Database";
            "RT","Runtime.Make(DB)";
         ]))
         ]@(List.map fst triggers)@[
         IP.Lines([
            (mk_let "schema" (IP.TList(brackets,Some(parens),";",
               (List.map (fun (id,ivars,ovars) ->
                  IP.Leaf("\""^id^"\", "^
                          (Util.list_to_string (fun x->x)
                                               (List.map vt_string ivars))^", "^
                          (Util.list_to_string (fun x->x)
                                               (List.map vt_string ovars)))
               ) schema)
            )));
            (mk_let "patterns" (IP.TList(brackets,Some(parens),";",
               (List.map (fun (rel,plist) ->
                  IP.Leaf("\""^rel^"\","^
                     (Util.list_to_string (fun (pat) ->
                        let map_pattern (vars,ids) = 
                           (Util.list_to_string (fun x->x) vars)^","^
                           (Util.list_to_string string_of_int ids)
                        in match pat with 
                           | M3Common.Patterns.In(p) -> 
                              "Patterns.In("^(map_pattern p)^")"
                           | M3Common.Patterns.Out(p) -> 
                              "Patterns.Out("^(map_pattern p)^")"
                     ) plist))
               ) patterns)
            )));
            (mk_let "db" (IP.Leaf("DB.make_empty_db schema patterns")));
            (mk_let "mux" (apply_many "List.fold_left" [
               IP.Leaf("FileMultiplexer.add_stream");
               IP.Leaf("FileMultiplexer.create ()");
               IP.TList(brackets,Some(parens),";",
                        (List.map (fun (s,_,_) -> s) sources))
            ]));
            (mk_let "tlqs" (apply_many "List.map" [
               IP.Leaf("DB.string_to_map_name");
               IP.TList(brackets,Some(parens),";",
                        (List.map (fun x -> IP.Leaf("\""^x^"\"")) tlqs))
            ]));
            (mk_let "dispatcher event" (IP.Lines(
               [IP.Leaf("match event with ")]@
               (List.map (fun (_,trigger) ->
                  match trigger with Trigger(pm, rel, vars) ->
                     IP.Leaf("| ("^(match pm with | M3.Insert -> "M3.Insert" 
                                                  | M3.Delete -> "M3.Delete")^
                             ", \""^rel^"\", vars) -> on_"^
                                   (match pm with | M3.Insert -> "insert" 
                                                  | M3.Delete -> "delete")^
                             "_"^rel^" dbtoaster_db vars; true")
                  | _ -> failwith "Non-trigger code passed to main"
               ) triggers)@
(*               [IP.Leaf("| _ -> false")]*)
               [IP.Leaf("| (M3.Insert,rel,_) -> failwith "^
                        "(\"Unknown event Insert(\"^rel^\")\")");
                IP.Leaf("| (M3.Delete,rel,_) -> failwith "^
                        "(\"Unknown event Delete(\"^rel^\")\")")]
            )));
            (apply_many "RT.synch_main" [
               IP.Leaf("db");
               IP.Leaf("mux");
               IP.Leaf("tlqs");
               IP.Leaf("dispatcher");
               IP.Leaf("main_args ()");
               IP.Leaf("")
            ])
         ])
      ]), Unit)
   
   let output ((code,_):code_t) (out:out_channel): unit =
      output_string out (IP.to_string 80 code)
      
   let to_string ((code,_):code_t): string =
      (IP.to_string 80 code)

end

module K3CG : K3Codegen.CG = K3O
