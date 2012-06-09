(** K3 Scala Generator
 * A backend that produces Scala source code for K3 programs.
 *)

(* TODO:
 * - Test to find bugs
 * - Improve code quality in general
 * - Add comments
 *)

open M3
open K3
open Database
open Format
open Types
open Patterns

exception K3SException of string
;;

module K3S =
struct
  type collection_type_t = Unknown | Persistent | Intermediate

  type argn_t = 
  | ArgN of string
  | ArgNTuple of argn_t list

  type type_t   = 
  | Float
  | Bool
  | Int
  | String
  | Any
  | External of string
  | Unit
  | Fn of argn_t * type_t * type_t
  | ExtFn of string * type_t * type_t 
  | Tuple of type_t list
  | Collection of collection_type_t * type_t list * type_t
  | Trigger of Schema.event_t

  type source_code_t = string
  type code_t = source_code_t * type_t
  type op_t = (type_t -> type_t -> 
    (source_code_t -> source_code_t -> source_code_t) * type_t * string option)
  type db_t = unit

  let line_sep = "\n"
  let max_tuple_elems = 22
  
  let source_count = ref 0
  
  let list_split l n = 
    let rec _list_split l1 l2 n =
      if(n > 0) then
        match l2 with
        | x :: xs -> _list_split (l1 @ [x]) xs (n - 1)
        | [] -> (l1, [])
      else
        (l1, l2)
    in
    _list_split [] l n

  let list_vars prefix n = 
    let rec _list_vars prefix n = 
      if n = 0 then [] 
      else 
        (prefix ^ (string_of_int n)) :: (_list_vars prefix (n - 1)) in
    List.rev (_list_vars prefix n)

  let make_list ?(empty="") ?(parens=("(", ")")) ?(sep=",") l = 
    let rec make_list_ l = 
      match l with
      | [x] -> x
      | x :: xs -> x ^ sep ^ (make_list_ xs)
      | [] -> empty
    in
    (fst parens) ^ (make_list_ l) ^ (snd parens)

  let rec make_tuple t : string = 
    let nb_elems = (List.length t) in 
    let elems = 
      if nb_elems > max_tuple_elems then
        let (t1, t2) = (list_split t (max_tuple_elems - 1)) in
        t1 @ [make_tuple t2]
      else
        t
    in
    match t with 
    | []      -> "Unit"
    | x :: [] -> "(" ^ x ^ ")"
    | _       -> "Tuple" ^ (string_of_int (List.length elems)) ^ 
                 (make_list elems)

  let rec string_of_argn a : string = 
    match a with
    | ArgN(n) -> n
    | ArgNTuple(ns) -> make_tuple (List.map string_of_argn ns)

  let rec string_of_tuple_type t : string =
    let nb_elems = (List.length t) in 
    let elems = 
      if nb_elems > max_tuple_elems then
        let (t1, t2) = (list_split t (max_tuple_elems - 1)) in
        t1 @ [string_of_tuple_type t2]
      else
        t
    in
    match t with 
    | []      -> "Unit"
    | x :: [] -> "(" ^ x ^ ")"
    | _       -> "Tuple" ^ (string_of_int (List.length elems)) ^ 
                 (make_list ~parens:("[", "]") elems)

  let rec string_of_type t : string = 
    match t with 
    | Float -> "Double"
    | Bool -> "Boolean"
    | Int -> "Int"
    | String -> "String"
    | Any -> "Any"
    | External(s) -> s
    | Unit -> "Unit"
    | Fn(_, arg, ret) -> (string_of_type arg) ^ " => " ^ (string_of_type ret)
    | ExtFn(_, arg, ret) -> (string_of_type arg) ^ " => " ^ (string_of_type ret)
    | Tuple(elems) -> (string_of_tuple_type (List.map string_of_type elems))
    | Collection(tpe, keys_t, val_t) -> 
      let type_string = match tpe with 
      | Intermediate -> "K3IntermediateCollection"
      | Persistent -> "K3PersistentCollection"
      | Unknown -> "K3Collection" in
      type_string ^ "[" ^
      (string_of_tuple_type (List.map string_of_type keys_t)) ^ ", " ^ 
      (string_of_type val_t) ^ "]"
    | Trigger(event_t) -> "Trigger"

  let string_of_collection_type keys_t val_t =
    "K3Collection[" ^ (make_list (List.map string_of_type keys_t)) ^ "," ^ 
      (string_of_type val_t) ^ "]"

  let debug_string ((x,t):code_t) : string =
    (string_of_type t)^": "^ x

  let debugfail (expr:(K3.expr_t option)) (msg:string) =
    print_string
      ("Error: "^msg^"\nExpression: "^
        (match expr with | None -> "{Not Available}"
                         | Some(s) -> K3.code_of_expr s));
    raise (K3SException("K3Scalagen Internal Error ["^msg^"]"))
	
	let rec map_base_type bt = 
    match bt with
		| TBool -> Bool
    | TInt -> Int
    | TFloat -> Float
    | TString -> String
    | TDate -> Int (* TODO change with an appropriate type in scala *)
    | TAny -> Any
    | TExternal(s) -> External(s)

  let rec map_type t =
    match t with
    | K3.TUnit -> Unit
    | K3.TBase(bt) -> map_base_type bt
    | K3.TTuple(tlist) -> Tuple(List.map map_type tlist)
    | K3.Collection(ctype) -> 
        begin match (map_type ctype) with 
        | Tuple([]) -> Collection(Intermediate, [], Unit)
        | Tuple(klist) -> 
          Collection(Unknown, (List.rev (List.tl (List.rev klist))), 
            (List.hd (List.rev klist)))
        | vtype -> Collection(Unknown, [], vtype)
        end
    | K3.Fn(argtlist,rett) -> Fn(ArgNTuple([]), map_type (List.hd argtlist), 
      map_type (K3.Fn(List.tl argtlist, rett)))

  let fn_ret_type fnt = match fnt with 
  | Fn(_, _, fnrett) -> fnrett
  | ExtFn(_, _, fnrett) -> fnrett
  | _ -> debugfail None ("Function excpected found: " ^ (string_of_type fnt))

  type source_impl_t = source_code_t
  let identity a = a
  let mk_bool_to_float a = "(if(" ^ a ^ ") 1.0 else 0.0)"
  let num_op ?(conva = identity) ?(convb = identity) opcode = (fun a b -> "(" ^ 
    conva(a) ^ ") " ^ opcode ^ " (" ^ convb(b) ^ ")")

  let f_op op_f op_b = 
    (fun a b -> 
      match (a, b) with
      | (Float, Float) -> ((num_op op_f), Float, None)
      | (Bool, Bool)   -> ((num_op op_b), Bool, None)
      | (Bool, Float) -> ((num_op op_f), Float, None)
      | (Float, Bool) -> ((num_op op_f), Float, None)
      | (Float, Int) -> ((num_op op_f), Float, None)
      | (Int, Float) -> ((num_op op_f), Float, None)
      | (Int, Int) -> ((num_op op_f), Int, None)
      | (Int, Bool) -> ((num_op op_f), Int, None)
      | (Bool, Int) -> ((num_op op_f), Int, None)
      | (_, _) -> 
          ((num_op op_f), Unit, 
          Some((string_of_type a) ^ " [" ^ op_f ^ "||" ^ op_b ^ "] " ^ 
            (string_of_type b)))
    )

  let fixed_op (t1:type_t) (t2:type_t) (rett:type_t) (op:string->string->string) =
    (fun a b -> if (a = t1) && (b = t2) 
      then (op, rett, None)  
      else (op, Unit, Some((string_of_type a) ^ " [cmp] " ^ (string_of_type b))))

  let c_op op = (fun a b -> 
      match (a, b) with
      | (Float, Float) | (String, String) | (Bool, Bool) | (Int, Int)
      | (Int, Float) | (Float, Int) -> 
        ((num_op op), Bool, None)
      (* Sometimes there is K3 code that compares booleans to floats, so code
         is being generated to convert the boolean to a float
      *)
      | (Bool, Float) -> ((num_op op ~conva:mk_bool_to_float), Bool, None)
      | (Float, Bool) -> ((num_op op ~convb:mk_bool_to_float), Bool, None)
      | (_, _) -> 
          ((num_op op), Unit, 
          Some((string_of_type a) ^ " [" ^ op ^ "] " ^ (string_of_type b)))
    )

  let add_op         : op_t = (f_op "+" "||")
  let mult_op        : op_t = (f_op "*" "&&")
  let eq_op          : op_t = (c_op "==")
  let neq_op         : op_t = (c_op "!=")
  let lt_op          : op_t = (c_op "<")
  let leq_op         : op_t = (c_op "<=")
  let ifthenelse0_op : op_t = 
    (fun at bt -> 
      match at with 
      | Bool -> 
        ((fun a b -> ("if(" ^ a ^ ") { " ^ b ^ " } else { 0.0 }")), bt, 
          None)
      | Float -> 
        ((fun a b -> ("if(" ^ a ^ " != 0.0) { " ^ b ^ " } else { 0.0 }")), bt, 
          None)
      | _ -> 
        ((fun a b -> "()"), Unit, Some("ifthenelse("^(string_of_type at)^")"))
    )

  let const ?(expr = None) (c:const_t) : code_t = 
    match c with 
    | CBool(y) -> (string_of_bool y, Bool)
    | CInt(y) -> (string_of_int y, Int)
    | CFloat(y) -> (string_of_float y, Float)
    | CString(y) -> ("\"" ^ y ^ "\"", String)
    | CDate(y,m,d) -> (string_of_int (y*10000+m*100+d), Int)
   (* TODO must be change with appropriate object in scala *)
   
  let var ?(expr = None) (v:K3.id_t) (t:K3.type_t) : code_t = 
    ("var_" ^ v, map_type t)

  let tuple ?(expr = None) (c:code_t list) : code_t =
    if (List.length c) = 0 then debugfail expr "Empty List"
    else let t = (List.map (fun (_,t)->t) c) in
          (make_tuple
            (List.map (fun (s,_) -> "(" ^ s ^ ")") c), Tuple(t))

  let project ?(expr = None) ((tuple,tuplet):code_t) (f:int list) : code_t =
    let tuple_members = 
       match tuplet with 
       | Tuple(t) -> t 
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
       ("(" ^ tuple ^ ") match { case " ^ (make_list in_list) ^ " => " ^ 
        (make_list out_list) ^ 
        "; case _ => throw new IllegalArgumentException(\"boom\") }", Tuple(t))

  let singleton ?(expr = None) ((d,dt):code_t) (t:K3.type_t) : code_t =
    let kt, vt, v = 
      match dt with
      | Tuple(t) -> 
        let vars = (list_vars "v._" (List.length t)) in
        let kv, vv = list_split vars ((List.length vars) - 1) in
        let kt, vt = list_split t ((List.length t) - 1) in
        (kt, vt, make_tuple([make_tuple kv; List.hd vv]))
      | _ -> [], [dt], "v"
    in
    let tpe = Collection(Intermediate, kt, List.hd vt) in
    ("{ val v = " ^ d ^ ";" ^ (string_of_type tpe) ^ "(List(" ^ v ^ ")) }", tpe)
   
  let combine ?(expr = None) (a_code:code_t) (b_code:code_t): code_t =
    match (a_code, b_code) with 
    | ((a, Collection(_, ak, at)), (b, Collection(_, bk, bt))) ->
        if ak <> bk or at <> bt then 
          debugfail expr "Cannot combine collections of different types"
        else 
          let tpe = Collection(Intermediate, ak, at) in
          ((string_of_type tpe) ^ "(" ^ a ^ ".toList ::: " ^ b ^ ".toList)", tpe)
    | _ -> debugfail expr "Type mismatch for collection combine"

  let op ?(expr = None) ((mk_op):op_t) ((a,at):code_t) ((b,bt):code_t) : code_t =
    let (op_f,rt,err) = (mk_op at bt) in
      (match err with None -> () 
        | Some(err_msg) -> 
          debugfail expr ("Type mismatch in arithmetic: " ^ err_msg));
      ((op_f a b), rt)

  let ifthenelse ?(expr = None) ((i,it):code_t) ((t,tt):code_t) ((e,et):code_t) : code_t =
    if it <> Bool then debugfail expr "Type mismatch: non-bool condition" 
    else
      if tt <> et 
      then debugfail expr "Type mismatch: unaligned condition output" 
      else ("if(" ^ i ^ ") { " ^ t ^ " } else { " ^ e ^ " }", tt)

  let block ?(expr = None) (stmts:code_t list) : code_t =
    (make_list ~parens:("{","}") ~sep:"; " (List.map (fun (s,_) -> s) stmts),
      if (List.length stmts) < 1 then Unit else (snd (List.hd (List.rev stmts))))

  let wrap_function argt fnt f: string = 
    match fnt with
    | Fn(argsn, argst, rett) ->
      "(x:" ^ (string_of_type argt) ^ ") => { x match { case " ^ 
      (string_of_argn argsn) ^ " => {" ^ f ^ "} } }" 
    | ExtFn(n, _, _) -> n ^ " _"
    | _ -> debugfail None "Expected a function"

  let wrap_function_key_val (kt:type_t list) (vt:type_t) fnt f: string = 
    let ktt, vtt, f_body = 
    match fnt with
    | Fn(argsn1, argst1, Fn(argsn2, argst2, rett)) ->
      ([List.hd kt], List.hd (List.tl kt), wrap_function (fn_ret_type vt) (Fn(argsn2, argst2, rett)) f)
    | _ -> (kt, vt, f) in
    match fnt with
    | Fn(argsn, argst, rett) ->
      let argsnkv = match ktt with 
      | [] -> ArgNTuple(ArgN("()") :: [argsn])
      | Tuple(t) :: ts -> (
        match argsn with 
        | ArgNTuple(ns) -> 
          let k, v  = list_split ns ((List.length ns - 1)) in
          ArgNTuple(ArgNTuple(k) :: v)
        | _ -> debugfail None "Excpected tuple of argument names"
      )
      | _  -> ( 
        match argsn with 
        | ArgNTuple(ns) -> 
          let k, v  = list_split ns ((List.length ns - 1)) in
          ArgNTuple(ArgNTuple(k) :: v)
        | _ -> debugfail None "Excpected tuple of argument names"
      ) in
      "(x:Tuple2[" ^ (string_of_type (Tuple(ktt))) ^ ", " ^ (string_of_type vtt) ^ 
      "]) => { x match { case " ^ (string_of_argn argsnkv) ^ 
      " => {" ^ f_body ^ "} } }" 
    | ExtFn(n, _, _) -> n ^ " _"
    | _ -> debugfail None "Expected a function"

  let iterate ?(expr = None) ((fn,fnt):code_t) ((c, ct):code_t) : code_t =
    match ct with
    | Collection(_, kt, vt) -> ("(" ^ c ^ ").foreach { " ^ 
        (wrap_function_key_val kt vt fnt fn) ^ " }", Unit)
    | _ -> debugfail expr "Iteration of a non-collection"

  let map_args_type args rt =       
    match args with
    | K3.AVar(v, vt) -> (ArgN("var_" ^ v), rt)
    | K3.ATuple(tlist) -> (ArgNTuple(List.map (fun (v, _) -> ArgN("var_" ^ v)) tlist), rt)
  
  let lambda ?(expr = None) (args:K3.arg_t) ((b,bt):code_t) : code_t =
    let argsn, argst = map_args_type args bt in
    (b, Fn(argsn, argst, bt))

  let assoc_lambda ?(expr = None) (arg1:K3.arg_t) (arg2:K3.arg_t) (b:code_t) : code_t =
    lambda arg1 (lambda arg2 b)
	
  let external_lambda ?(expr = None) (name:K3.id_t) (args:K3.arg_t) (ret:K3.type_t) : code_t =
    let strlen = String.length name in
    let rec clean_name name i buffer: string =
      if (strlen = i) then 
        Buffer.contents buffer
      else (
        match name.[i] with
        | '/' -> Buffer.add_string buffer "div"; clean_name name (i + 1) buffer
        | c -> Buffer.add_char buffer c; clean_name name (i + 1) buffer
      ) in
    let _, argst = map_args_type args (map_type ret) in
    ("", ExtFn(clean_name name 0 (Buffer.create 0), argst, map_type ret))
   
  let apply ?(expr = None) ((fn,fnt):code_t) ((arg,argt):code_t) : code_t =
    ("(" ^ (wrap_function argt fnt fn) ^ ").apply(" ^ arg ^ ")", fn_ret_type fnt)

  let map ?(expr = None) ((fn,fnt):code_t) (exprt:K3.type_t) ((c, ct):code_t) : code_t =
    let (keyt,valt) = 
       match (fn_ret_type fnt) with
       |  Tuple(retlist) ->
            if retlist = []
            then debugfail expr "Mapping function with empty ret"
            else ((List.rev (List.tl (List.rev retlist))),
                  (List.hd (List.rev retlist)))
       |  x -> ([], x)
    in
    let kt, vt, wrapped_fn = 
      match ct with 
      | Collection(_, kt, vt) -> (kt, vt, (wrap_function_key_val kt vt fnt fn))
      | _ -> debugfail expr "Mapping of a non-collection" in
    let key_length = (List.length keyt) in
    let convert_to_key_value = 
      match keyt with
      | [] -> make_tuple ("()" :: "v" :: [])
      | x -> (string_of_type (Tuple((Tuple(keyt)) :: [valt]))) ^ "(" ^ 
        (make_list (list_vars "v._" key_length)) ^ ", v._" ^ 
        (string_of_int (key_length + 1)) ^ ")"
    in
    (c ^ ".map((y:Tuple2[" ^ (string_of_tuple_type (List.map string_of_type kt)) ^ 
      "," ^ (string_of_type vt) ^ "]) => { val v = ({ " ^ wrapped_fn ^ " })(y); " ^ 
      convert_to_key_value ^ " })", Collection(Intermediate, keyt,valt))

  let aggregate ?(expr = None) ((fn,fnt):code_t) ((init,initt):code_t) ((c, ct):code_t) : code_t =
    match ct with
    | Collection(_, kt, vt) ->
      (c ^ ".fold(" ^ init ^ ", { " ^ 
        (wrap_function_key_val (Tuple(kt) :: [vt]) (fn_ret_type fnt) fnt fn) ^ 
        " })", (fn_ret_type (fn_ret_type fnt)))
    | _ -> debugfail expr ("Aggregate on a non-collection: " ^ c ^ " has type " ^ 
      (string_of_type ct))

  let group_by_aggregate ?(expr = None) ((agg,aggt):code_t) ((init,initt):code_t) ((group,groupt):code_t) ((c, ct):code_t) : code_t =
    let key_type = 
       match (fn_ret_type groupt) with 
       | Tuple(tlist) -> tlist
       | _ -> [(fn_ret_type groupt)]
    in
    match ct with
    | Collection(_, kt, vt) ->
      (c ^ ".groupByAggregate(" ^ init ^ ", { " ^ 
        (wrap_function_key_val kt vt groupt group) ^ " }, { " ^ 
        (wrap_function_key_val (Tuple(kt) :: [vt]) (fn_ret_type aggt) aggt agg) ^ 
        " })", Collection(Intermediate, key_type,(fn_ret_type (fn_ret_type aggt))))
    | _ -> debugfail expr ("groupByAggregate on a non-collection: " ^ c ^ 
      " has type " ^ (string_of_type ct))

  let flatten ?(expr = None) ((c,ct):code_t) : code_t =
    match ct with
    | Collection(_, [],Collection(_, ki,t)) ->
      (("(" ^ c ^ ").flatten()"), Collection(Intermediate, ki, t))
    | _ -> debugfail expr ("Flatten on a non-collection: " ^ c ^ " has type " ^ 
      (string_of_type ct))

  let exists ?(expr = None) ((c,ct):code_t) (key:code_t list) : code_t =
    match ct with 
    |  Collection(_, kt,vt) when kt <> [] ->
       ("(" ^ c ^ ").contains(" ^ (make_tuple (List.map (fun (s,_) -> s) key)) ^ 
        ")",Bool)
    | _ -> debugfail expr "Existence check on a non-collection"

  let lookup ?(expr = None) ((c,ct):code_t) (key:code_t list) : code_t =
    match ct with 
    | Collection(_, kt, vt) when kt <> [] ->
      ("(" ^ c ^ ").lookup(" ^ (make_tuple (List.map (fun (s,_) -> s) key)) ^ ")", vt)
    | _ -> debugfail expr ("Lookup on a non-collection: " ^ c ^ " has type " ^ 
      (string_of_type ct))
      
  (* map, partial key, pattern -> slice *)
  let slice ?(expr = None) ((c,ct):code_t) (pkey:code_t list) 
           (pattern:int list) : code_t =
    if List.length pattern <> List.length pkey then
       debugfail expr ("Error: slice pattern and key length mismatch")
    else
    if List.length pattern = 0 then (c,ct)
    else
    match ct with 
    | Collection(Persistent, kt,vt) when kt <> [] ->
      (c ^ ".slice(" ^ (make_tuple (List.map fst pkey)) ^ ", List" ^ 
        (make_list (List.map string_of_int pattern)) ^ ")", 
      Collection(Intermediate, kt, vt))
    | Collection(_, kt,vt) when kt <> [] ->
      (c ^ ".slice(" ^ (make_tuple (List.map fst pkey)) ^ ", List" ^ 
        (make_list (List.map string_of_int pattern)) ^ ")", 
      Collection(Intermediate, kt, vt))
    | _ -> debugfail expr "Slice on a non-collection"


  (* Database retrieval methods *)  
  let get_value ?(expr = None) (t:K3.type_t) (map:K3.coll_id_t): code_t =
    (map ^ ".get()",  map_type t)

  let get_in_map  ?(expr = None) (schema:K3.schema) (t:K3.type_t)   
                 (map:K3.coll_id_t): code_t =
    (map, Collection(Persistent, (List.map (fun (_,x) -> map_type x) schema),  map_type t))

  let get_out_map ?(expr = None) (schema:K3.schema) (t:K3.type_t)
                 (map:K3.coll_id_t): code_t =
    (map, Collection(Persistent, (List.map (fun (_,x) -> map_type x) schema),  map_type t))

  let get_map ?(expr = None) ((ins,outs):(K3.schema*K3.schema))  
             (t:K3.type_t) (map:K3.coll_id_t): code_t =
    (map, Collection(Persistent, (List.map (fun (_,x) -> map_type x) ins),
             Collection(Persistent, (List.map (fun (_,x) -> map_type x) outs),
                         map_type t)))

  let update_value ?(expr = None) (map:K3.coll_id_t) ((v,vt):code_t): code_t =
    (map ^ ".update(" ^ v ^ ")",Unit)

  let update_in_map_value ?(expr = None) (map:K3.coll_id_t) 
                         (key:code_t list) ((v,vt):code_t): code_t =
    (map ^ ".updateValue(" ^ (make_tuple (List.map fst key)) ^ ", " ^ v ^ ")",Unit)

  let update_out_map_value ?(expr = None) (map:K3.coll_id_t) 
                          (key:code_t list) ((v,vt):code_t): code_t =
    (map ^ ".updateValue(" ^ (make_tuple (List.map fst key)) ^ ", " ^ v ^ ")",Unit)

  let update_map_value ?(expr = None) (map:K3.coll_id_t) (inkey:code_t list)
                      (outkey:code_t list) ((v,vt):code_t): code_t =
    (map ^ ".updateValue(" ^ (make_tuple (List.map fst inkey)) ^ ", " ^ 
      (make_tuple (List.map fst outkey)) ^ ", " ^ v ^ ")", Unit)

  let update_in_map ?(expr = None) (map:K3.coll_id_t) (v:code_t): code_t =
    (map ^ ".updateInMap()",Unit)

  let update_out_map ?(expr = None) (map:K3.coll_id_t) (v:code_t): code_t =
    (map ^ ".updateOutMap()",Unit)

  let update_map ?(expr = None) (map:K3.coll_id_t) (inkey:code_t list) 
                ((v,vt):code_t): code_t =
    let conversion = match vt with
    | Collection(_, _, _) -> ".toPersistentCollection()"
    | _ -> "" in
    (map ^ ".updateValue(" ^ (make_tuple (List.map fst inkey)) ^ ", " ^ v ^ conversion ^ ")",Unit)
	
  let remove_in_map_element ?(expr = None) (map:K3.coll_id_t) (inkey:code_t list) : code_t =
      (map ^ ".remove(" ^ (make_tuple (List.map fst inkey)) ^ ")", Unit)
  
  (* persistent collection id, out key -> remove *)
  let remove_out_map_element ?(expr = None) (map:K3.coll_id_t) (outkey:code_t list) : code_t =
      (map ^ ".remove(" ^ (make_tuple (List.map fst outkey)) ^ ")", Unit)
  
  (* persistent collection id, in key, out key -> remove *)
  let remove_map_element ?(expr = None) (map:K3.coll_id_t) (inkey:code_t list) (outkey:code_t list) : code_t =
      (map ^ ".remove(" ^ (make_tuple (List.map fst inkey)) ^ ", " ^ (make_tuple (List.map fst outkey)) ^ ")", Unit)
      
  (* unit operation which has no effect *)
  let unit_operation : code_t = ("()", Unit) 
   
  let trigger (eventt:Schema.event_t) (code:code_t list): code_t =
    let prefix, vars =
      match eventt with
      | Schema.InsertEvent(rel, vars, tpe) -> ("Insert" ^ rel, vars) 
      | Schema.DeleteEvent(rel, vars, tpe) -> ("Delete" ^ rel, vars)
      | Schema.SystemInitializedEvent -> ("SystemInitialized", []) 
    in
    let var_def = (make_list (List.map (fun (n, t) -> "var_" ^ n ^ ": " ^ (string_of_type (map_base_type t))) vars)) in
    let fn_def = "def on" ^ prefix ^ var_def in
    let stmts = (make_list ~parens:("{","}") ~sep:"; " (List.map (fun (s,_) -> s) code)) in
    (fn_def ^ " = " ^ stmts, Trigger(eventt))
   
  let source (source:Schema.source_t) (adaptors:(Schema.adaptor_t * Schema.rel_t) list):
            (source_impl_t * code_t option * code_t option) =
    let sc = source_count := !source_count + 1; string_of_int !source_count in 
    let string_of_framing_type framing = 
      match framing with
      | Schema.FixedSize(i) -> "FixedSize(" ^ (string_of_int i) ^ ")"
      | Schema.Delimited(str) -> "Delimited(\"" ^ str ^ "\")"
    in
    let adaptors_strings = (List.map (fun ((atype, akeys), (rel, _ , _)) -> 
      match atype with
      | "csv" -> "new CSVAdaptor(\"" ^ rel ^ "\", " ^ (make_list ~parens:("","") 
        (List.map (fun (k, v) -> k ^ " = \"" ^ v ^ "\"") akeys)) ^ ")"
      | "orders" | "part" | "partsupp" | "customer" | "supplier" | "nation" | "region" | "lineitem" -> 
        "new " ^ atype ^ "Adaptor(\"" ^ rel ^ "\", " ^ (make_list ~parens:("","") 
        (List.map (fun (k, v) -> k ^ " = \"" ^ v ^ "\"") akeys)) ^ ")"
      | _ ->
          "createAdaptor(\"" ^ atype ^ "\", \"" ^ rel ^ "\", List" ^
            (make_list (List.map (fun (k,v) -> "(\"" ^ k ^ "\", \"" ^ v ^ "\")") akeys)) ^
          ")"
    ) adaptors)
    in
    let source_init = 
      match source with 
        | Schema.FileSource(s, f) -> 
          "createInputStreamSource(new FileInputStream(\"" ^ s ^ "\"), List" ^ 
          (make_list adaptors_strings) ^ ", " ^ (string_of_framing_type f) ^ ")"
        | Schema.SocketSource(_, _, _) -> debugfail None "Sockets not implemented"
        | Schema.PipeSource(_, _) -> debugfail None "Pipes not implemented"
        | Schema.NoSource -> debugfail None "Empty source not implemented"
    in
    let init_code = 
      let fn_event_to_tuple = 
        "x => x match {" ^ 
        (make_list ~sep:";" ~parens:("",";") (List.map (fun (adaptor, (rel, vars, _)) -> 
          (*let k, v = (list_split (List.map (fun (n, t) -> "var_" ^ n) vars) ((List.length vars) - 1)) in*)
          "case StreamEvent(InsertTuple, o, \"" ^ rel ^ "\", List" ^ 
          let k = (make_list (List.map (fun (n, _) -> "var_" ^ n) vars)) in
          let knt = (make_list (List.map (fun (n, t) -> "var_" ^ n ^ ":" ^ string_of_type (map_base_type t)) vars)) in
          knt ^ ") => " ^ 
          "if(" ^ rel ^ ".contains(" ^ k ^ ")) { val count = " ^ rel ^ ".lookup(" ^ k ^ ") + 1; " ^ 
          rel ^ ".updateValue(" ^ k ^ ", count) } else { " ^
          rel ^ ".updateValue(" ^ k ^ ", 1) }"
        ) adaptors)) ^
        "}"
      in
      ".forEachEvent({" ^ fn_event_to_tuple ^ "})"
    in
    let source_n = "s" ^ sc in 
    ("val " ^ source_n ^ " = " ^ source_init, Some(init_code, Unit), Some(source_n, Unit))
	   
  let main (schemas:K3.map_t list) (patterns:pattern_map) 
    (tables:(source_impl_t * code_t option * code_t option) list)
    (streams:(source_impl_t * code_t option * code_t option) list)
    (triggers:code_t list) (tlqs:(string * K3.expr_t * code_t) list): code_t = 
    let str_tlqs = 
      (make_list ~sep:";" ~parens:("",";") (List.map (fun (n, _, (c, ct)) -> 
        "def get" ^ n ^ "():" ^ (string_of_type ct) ^ " = {" ^ c ^ "}"
      ) tlqs))
    in
    let print_results = "def printResults(): Unit = { " ^
      "val pp = new PrettyPrinter(80, 2);" ^
      (make_list ~sep:";" ~parens:("",";") (List.map (fun (x, _, (c, ct)) -> 
        match ct with
        | Collection(_, _, _) -> "println(pp.format(<" ^ x ^ ">{ get" ^ x ^ "().toXML() }</" ^ x ^ ">))"
        | _ -> "println(pp.format(<" ^ x ^ ">{ get" ^ x ^ "() }</" ^ x ^ ">))") tlqs)) ^ 
      " }" in
    let str_schema =
      let make_type_list t: string = string_of_tuple_type (List.map (fun (n, t) -> string_of_type (map_base_type t)) t) in
      let sndIdx rel tpe valt =
        try
          (* find patterns for a certain map *)
          let map_patterns = snd (List.find (fun (n, _) -> n = rel) patterns) in
          let idxs: string list = List.fold_left 
            (fun agg pat -> 
              let rec list_project l ids = 
                match ids with 
                | [] -> []
                | x :: xs -> (List.nth l x) :: (list_project l xs)
              in
              let map_pattern (vars,ids): string = (
                let vars = (list_vars "x" (List.length tpe)) in 
                let sids = (List.map string_of_int ids) in
                (make_list ~sep:"_" ~parens:("\"","\"") sids) ^ 
                " -> SecondaryIndex[" ^ (make_type_list (list_project tpe ids)) ^ 
                "," ^ (make_type_list tpe) ^ ", " ^ valt ^ "](x => x match " ^
                 "{ case " ^ (make_tuple vars) ^ " => " ^ 
                 (make_tuple (list_project vars ids)) ^ " })"
              ) in 
              match pat with 
              | Patterns.In(([],[])) -> agg
              | Patterns.Out(([],[])) -> agg 
              | Patterns.In(p) -> (map_pattern p) :: agg
              | Patterns.Out(p) -> (map_pattern p) :: agg
            ) [] map_patterns in
            match idxs with
            | [] -> "None"
            | _  -> "Some(Map" ^ (make_list idxs) ^ ")"
        with
        | Not_found -> "None"
      in
      (make_list ~parens:("",";") ~sep:";" (List.map (fun (id,ivars,ovars,t) ->
        let str_val_t = string_of_type (map_base_type t) in
        match (ivars, ovars) with
        | ([], []) -> "var " ^ id ^ " = SimpleVal[" ^ str_val_t ^ "](0)"
        | ([], os) -> "var " ^ id ^ " = new K3PersistentCollection[" ^ 
          (make_type_list os) ^ ", " ^ str_val_t ^ "](Map(), " ^ (sndIdx id os str_val_t) ^ 
          ") /* out */"
        | (is, []) -> "var " ^ id ^ " = new K3PersistentCollection[" ^ 
          (make_type_list is) ^ ", " ^ str_val_t ^ "](Map(), " ^ (sndIdx id is str_val_t) ^ ") /* in */"
        | (is, os) -> "var " ^ id ^ " = new K3FullPersistentCollection[" ^ 
          (make_type_list is) ^ "," ^ (make_type_list os) ^ ", " ^ str_val_t ^ "](Map(), " ^ 
          (sndIdx id is str_val_t) ^ ") /* full */"
      ) schemas))
    in
    let run = "def run(onEventProcessedHandler: Unit => Unit = (_ => ())): Unit = { fillTables(); dispatcher(StreamEvent(SystemInitialized, 0, \"\", List()), onEventProcessedHandler) }"
    in
    let imports = (make_list ~sep:";" ~parens:("",";") (List.map (fun x -> "import " ^ x)
      [ 
        "java.io.FileInputStream";
        "org.dbtoaster.dbtoasterlib.StreamAdaptor._";
        "org.dbtoaster.dbtoasterlib.K3Collection._";
        "org.dbtoaster.dbtoasterlib.Source._";
        "org.dbtoaster.dbtoasterlib.dbtoasterExceptions._";
        "org.dbtoaster.dbtoasterlib.ImplicitConversions._";
        "org.dbtoaster.dbtoasterlib.StdFunctions._";
        "scala.collection.mutable.Map";
        "xml._";
      ])
    )
    in
    let dispatcher = 
      "def dispatcher(event: DBTEvent, onEventProcessedHandler: Unit => Unit): Unit = {" ^ 
      "event match { " ^
      (make_list ~parens:("",";") ~sep:";" 
        (List.map (fun x -> 
          (match (snd x) with 
            | Trigger(eventt) -> 
              let event_type, trigger_type, rel, vars = 
                match eventt with 
                | Schema.InsertEvent(rel, vars, tpe) -> ("InsertTuple", "Insert", rel, vars)
                | Schema.DeleteEvent(rel, vars, tpe) -> ("DeleteTuple", "Delete", rel, vars)
                | Schema.SystemInitializedEvent -> ("SystemInitialized", "SystemInitialized", "", []) in
              "case StreamEvent(" ^ event_type ^ ", o, \"" ^ rel ^ "\", " ^ 
              (make_list ~parens:("List(", ")") (List.map (fun (n, t) -> 
              "(var_" ^ n ^ ": " ^ (string_of_type (map_base_type t)) ^ ")") vars)) ^ ") => on" ^ 
              trigger_type ^ rel ^ (make_list (List.map (fun (n, t) -> "var_" ^ n) vars))
          | t -> debugfail None ("Trigger expected but found " ^ (string_of_type t))
        )) triggers)) 
      ^ "case EndOfStream => ();"
      ^ "case _ => throw ShouldNotHappenError(\"Event could not be dispatched: \" + event)"
      ^ "} "
      ^ "onEventProcessedHandler();"
      ^ "if(sources.hasInput()) dispatcher(sources.nextInput(), onEventProcessedHandler)"
      ^ "}" 
    in
    let str_sources = 
      (make_list ~parens:("",";") ~sep:";" (List.map (fun (s, _, _) -> s) streams)) ^
      (make_list ~parens:("",";") ~sep:";" (List.map (fun (s, _, _) -> s) tables))
    in
    let str_streams = 
      "val sources = new SourceMultiplexer(List" ^ 
      (make_list (List.map (fun (_, _, os) -> 
        match os with 
        | Some(s, _) -> s 
        | _ -> debugfail None "Missing source name"
      ) streams)) ^ ");"
    in
    let triggers = (make_list ~parens:("",";") ~sep:";" (List.map (fun (b, bt) -> b) triggers)) in
    let filltables = 
      "def fillTables(): Unit = {" ^
      (make_list ~parens:("",";") ~sep:";" (List.map (fun (s, init, _) -> 
        match init with 
        | Some(i, _) -> s ^ i
        | None -> ""
      ) tables)) ^
      "}"
    in
    (imports ^
     " package org.dbtoaster { object Query { " ^
     str_sources ^ str_streams ^ str_schema ^ str_tlqs ^ triggers ^ filltables ^ dispatcher ^ run ^ print_results ^ " }}", 
      Unit)


  (** This function formats a piece of scala code *)
  let format_code (str:string) = 
    let strlen = String.length str in
    let rec _format_code (pos:int) (ind:int) (newline:bool) buffer: string = 
      if (strlen = pos) then 
        Buffer.contents buffer
      else
        let rec make_indent (len:int): string = 
          if len = 0 then "" else " " ^ (make_indent (len - 1)) in
        let npos = pos + 1 in
        match str.[pos] with
        | '{' -> let nind = ind + 2 in ( 
            Buffer.add_char buffer '{'; 
            _format_code npos nind true buffer
          )
        | '}' -> let nind = ind - 2 in ( 
            Buffer.add_string buffer (line_sep ^ (make_indent nind) ^ "}"); 
            _format_code npos nind true buffer
          )
        | ';' -> (
            Buffer.add_string buffer  (";" ^ line_sep); 
            _format_code npos ind true buffer
          )
        | x -> if x == ' ' && newline then 
            _format_code npos ind true buffer
          else ( 
            Buffer.add_string buffer ((if newline then line_sep ^ 
              (make_indent ind) else "") ^ (Char.escaped x)); 
            _format_code npos ind false buffer
          )
    in
    _format_code 0 0 true (Buffer.create 1)
       
   let output ((code,_):code_t) (out:out_channel): unit =
      (print_endline ("Formatting code... (" ^ (string_of_int (String.length code)) ^ 
        " bytes)"));
      output_string out (format_code code)
      
   let to_string ((code,_):code_t): string =
      format_code code

end

module K3CG : K3Codegen.CG = K3S