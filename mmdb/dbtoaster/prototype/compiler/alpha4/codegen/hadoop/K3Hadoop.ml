open Util
open SourceCode

open M3
open K3.SR
open Values
open Database


module CG (*: K3Codegen.CG*) =
struct
type op_t = source_code_op_t
type data_t = HDFS of string | HBase of string
type input_t = data_t
type output_t = data_t

(*type arg_t = K3.SR.arg_t*)

(*
type x = A of int | B of int * y
and y = ... | D of int * x list
*)


type code_t =
  | Const of string
  | Var of string
  | Tuple of code_t list
  | Op of op_t * code_t * code_t
  
  | Exists of code_t * code_t list
  
  | Bind of arg_t list * code_t
  | IfThen of code_t * code_t                                  (* condition, code accessing map *)
  (* TODO: collection constructors which write out HDFS/HBase collections*)
  | Mapper of arg_t * code_t * code_t * arg_t list
  | Reducer of arg_t * arg_t * code_t * code_t * code_t list
    (* initial value, leader code (leader takes care of aggregate initial value), partial aggregation code *)
  | Stage of input_t_n * output_t * code_t * code_t
                                                                       (* input, output, map job, reduce job *)
  | Lookup of data_t * code_t list
  | Slice of data_t * code_t list * int list
  | Update of data_t * code_t list * code_t
  | Trigger of input_t * code_t list * arg_t list                  (* relation input, stages *)
  | Main of source_code_t list                                    (* list of stages to write out to files *)
  (* TODO: for general K3 programs *)
  | Block of code_t list
  and input_t_n = InputCodeT of code_t | InputT of input_t


type db_t = NamedK3Database.db_t
type source_impl_t = source_code_t
let ifthenelse0_op = "ifte"

(* Operators *)
let add_op  = "+"
let mult_op = "*"
let eq_op   = "="
let neq_op  = "<>"
let lt_op   = "<"
let leq_op  = "<="

let string_of_data_t c =
	begin match c with 
    | HDFS c -> "HDFS("^c^")"
    | HBase v -> "HBase("^v^")"
	end
let name_of_data_t c =
	begin match c with 
    | HDFS c -> c
    | HBase v -> v
	end


let rec get_var arg = match arg with
  | AVar(v,_) -> Var v
  | ATuple(vt_l) -> Tuple (
	let varTemp vt = 
		match vt with
			a,b -> Var a
	in List.map varTemp vt_l)

let get_vars arg = match arg with
  | AVar(v,_) -> [v]
  | ATuple(vt_l) -> List.map fst vt_l  

let rec string_of_code_t c =
	begin match c with 
    | Const c -> "Const("^c^")"
    | Var v -> "Var("^v^")"
    | Tuple t -> "Tuple("^String.concat ";" (List.map string_of_code_t t)^")"
	| Op(op,l,r) -> "Op("^op^","^(string_of_code_t l)^","^(string_of_code_t r)^")"
	
	| Exists(m,k) -> "Exists("^(string_of_code_t m)^","^String.concat "," (List.map string_of_code_t k)^")"
	
	| Bind(a,b) -> "Bind("^String.concat "," (List.map string_of_code_t (List.map get_var a))^","^string_of_code_t b^")"
	| IfThen(i,t) -> "IfThen("^(string_of_code_t i)^","^(string_of_code_t t)^")"
	| Mapper(arg,t,i, alst) -> "Mapper("^string_of_code_t (get_var arg)^","^string_of_code_t t^","^string_of_code_t i^","^String.concat ":" (List.map string_of_code_t (List.map get_var alst))^")"
	| Reducer(arg1,arg2,i,l,p) -> "Reducer("^string_of_code_t (get_var arg1)^","^string_of_code_t (get_var arg2)^","^string_of_code_t i^","^string_of_code_t l^","^String.concat "," (List.map string_of_code_t  p)^")"
	| Stage(i,o,m,r) -> 
		begin match i with
		| InputCodeT a -> 
			begin match o with
			| HDFS s -> "Stage("^string_of_code_t a^",HDFS "^s^","^string_of_code_t m^","^string_of_code_t r^")"
			| HBase s -> "Stage("^string_of_code_t a^",HBase "^s^","^string_of_code_t m^","^string_of_code_t r^")"
			end
		| InputT b ->
			begin match b with
			| HDFS h -> 
				begin match o with
				| HDFS s -> "Stage("^"HDFS "^h^",HDFS "^s^","^string_of_code_t m^","^string_of_code_t r^")"
				| HBase s -> "Stage("^"HDFS "^h^",HBase "^s^","^string_of_code_t m^","^string_of_code_t r^")"
				end
			| HBase h -> 
				begin match o with
				| HDFS s -> "Stage("^"HBase "^h^",HDFS "^s^","^string_of_code_t m^","^string_of_code_t r^")"
				| HBase s -> "Stage("^"HBase "^h^",HBase "^s^","^string_of_code_t m^","^string_of_code_t r^")"
				end
			end
		end
	| Lookup(a,b) -> 		
		begin match a with
		| HDFS s -> "Lookup(HDFS "^s^","^String.concat "," (List.map string_of_code_t b)^")"
		| HBase s -> "Lookup(HBase "^s^","^String.concat "," (List.map string_of_code_t b)^")"
		end
	| Slice(a,b,c) -> 
		begin match a with
		| HDFS s -> "Slice(HDFS "^s^","^String.concat ":" (List.map string_of_code_t b)^","^String.concat ":" (List.map string_of_int c)^")"
		| HBase s -> "Slice(HBase "^s^","^String.concat ":" (List.map string_of_code_t b)^","^String.concat ":" (List.map string_of_int c)^")"
		end
	| Update(a,b,c) -> 
		begin match a with
		| HDFS s -> "Update(HDFS "^s^","^String.concat ":" (List.map string_of_code_t b)^","^string_of_code_t c^")"
		| HBase s -> "Update(HBase "^s^","^String.concat ":" (List.map string_of_code_t b)^","^string_of_code_t c^")"
		end
	| Trigger(a,b,alst) -> 
		begin match a with
		| HDFS s -> "Trigger(HDFS "^s^","^String.concat ":" (List.map string_of_code_t b)^","^String.concat ":" (List.map string_of_code_t (List.map get_var alst))^")"
		| HBase s -> "Trigger(HBase "^s^","^String.concat ":" (List.map string_of_code_t b)^","^String.concat ":" (List.map string_of_code_t (List.map get_var alst))^")"
		end
	| _ -> ""	
	end
		
let inl x = Inline x
let cdsc = concat_and_delim_source_code


(* Recursively lifts nested stacks into a single one, while respecting 
 * dependencies.  *)
let rec linearize_code c =
  match c with
  | _ -> c
  
let ssc = string_of_source_code

(* Source code generation, for pretty stringification during main function
 * construction. Note that unlike previous code generator backends, this one
 * keeps its internal form until the very end, to facilitiate internal late-stage 
 * optimizations such as linearize_code and memoize *)
let rec source_code_of_code c =
  begin match c with
    | Const c -> inl c
    | Var v -> inl v
    | Tuple t -> inl((String.concat "," (List.map ssc (List.map source_code_of_code t))))
	| Op(op,l,r) ->
        begin match op with
        | x when x = eq_op || x = neq_op || x = lt_op || x = leq_op ->
            inl ((ssc (source_code_of_code l))^op^(ssc (source_code_of_code r)))
        | _ -> cdsc op (source_code_of_code l) (source_code_of_code r)
        end
	| Exists(m,l) -> inl("writer.exists("^ssc (source_code_of_code m)^","^(String.concat "," (List.map ssc (List.map source_code_of_code l)))^" == true")
	
	| Bind(a,b) -> inl("bind "^(String.concat ";" (List.map string_of_code_t (List.map get_var a)))^","^ssc (source_code_of_code b)^"\n")
	| Lookup(a, b) -> 
		begin match b with
		| [] -> 
			begin match a with
			| HDFS s -> inl("writer.lookup(\"" ^ s ^ "\",\"-\",\"-\")")
			| HBase s -> inl("writer.lookup(\"" ^ s ^ "\",\"-\",\"-\")")
			end
		| [x] -> source_code_of_code (Lookup(a,[Const "\"-\"";x]))
		| _ -> 
			begin match a with
			| HDFS s -> inl("writer.lookup(\"" ^ s ^ "\"," ^ (String.concat "," (List.map ssc (List.map source_code_of_code b)))^")")
			| HBase s -> inl("writer.lookup(\"" ^ s ^ "\"," ^ (String.concat "," (List.map ssc (List.map source_code_of_code b)))^")")
			end
		end
	| IfThen(a, b) -> (*inl("if "^ssc (source_code_of_code a)^":\n		"^ssc (source_code_of_code b)^" \n")*)
		inl(ssc (source_code_of_code b)^" \n")
	| Update(a, b, c) -> 
		begin match b with
		| [] -> 
			begin match a with
			| HDFS s -> inl("writer.update(\"" ^ s ^ "\",\"\",\"\","^ssc (source_code_of_code c)^")")
			| HBase s -> inl("writer.update(\"" ^ s ^ "\",\"\",\"\","^ssc (source_code_of_code c)^")")
			end	
		| [x] -> source_code_of_code (Update(a,[Const "\"\"";x],c))
		| _ -> 
			begin match a with
			| HDFS s -> inl("writer.update(\"" ^ s ^ "\"," ^ (String.concat "," (List.map ssc (List.map source_code_of_code b)))^","^ssc (source_code_of_code c)^")")
			| HBase s -> inl("writer.update(\"" ^ s ^ "\"," ^ (String.concat "," (List.map ssc (List.map source_code_of_code b)))^","^ssc (source_code_of_code c)^")")
			end	
		end
	| Slice(a,b,c) -> 
		begin match a with
		| HDFS s -> inl("writer.slice(\""^s^"\","^(String.concat "," (List.map ssc (List.map source_code_of_code b)))^","^String.concat ":" (List.map string_of_int c)^")")
		| HBase s -> inl("writer.slice(\""^s^"\","^(String.concat "," (List.map ssc (List.map source_code_of_code b)))^","^String.concat ":" (List.map string_of_int c)^")")
		end
	| Mapper(arg,t,i, alst) -> 
		begin match t with
		| Stage(_,_,_,_) -> 
			inl(ssc (source_code_of_code t))
		| tp ->
		  inl("#Mapper\n#!/usr/bin/env python\nimport sys\nimport HbaseWriter\n\nwriter = HbaseWriter(\"127.0.0.1\", 9090)\n"^(
		  begin match alst with
			| [] -> "\n"
			| _ -> "for line0 in sys.stdin:\n	line0 = line0.strip()\n	"^
							String.concat "," (List.map ssc (List.map source_code_of_code (List.map get_var alst)))^" = line0.split()\n	"^
								String.concat "\n\t" (List.map (fun x -> ssc(source_code_of_code x)^" = int("^ssc(source_code_of_code x)^")") 
																		(List.map get_var alst))^"\n\t"
		  end
		  )
		  ^(
		  begin match arg with
			| ATuple(["null",TUnit]) -> "\n	"
			| _ -> "for line in "^ssc (source_code_of_code i)^":\n	line = line.strip()\n	"^
							ssc (source_code_of_code (get_var arg))^" = line.split()\n	"^
							String.concat "\n\t" (List.map (fun x -> ssc(source_code_of_code (Var(x)))^" = int("^ssc(source_code_of_code (Var(x)))^")") 
																		(get_vars arg))
		  end)^
		  (
		  begin match arg, tp with
			| ATuple([key,_;value,_]), IfThen(_,Tuple [_;b]) ->
				value^" = "^ssc (source_code_of_code b)^"\n	print '%s\\t%s'% ("^key^", "^value^")"^"  \n"
			| ATuple([key,_;value,_]), _ -> 
				ssc (source_code_of_code t)^"\n	print '%s\\t%s'% ("^key^", "^value^")"^"  \n"
			| _, _ -> ssc (source_code_of_code t)^"\n"
		  end
		  ))
		end
	| Reducer(key,value,initial,t,_) -> 
		inl("#Reducer Input key "^ssc (source_code_of_code (get_var key))^", value "^ssc (source_code_of_code (get_var value))^
			 "\n#!/usr/bin/env python\nimport sys\nimport HbaseWriter\n\nvalues = {}\n\nwriter = HbaseWriter(\"127.0.0.1\", 9090)\nfor line in sys.stdin:\n	line = line.strip()\n	"^
			 ssc (source_code_of_code (get_var key))^","^ssc (source_code_of_code (get_var value))^" = line.split()\n	"^
			 ssc (source_code_of_code initial)^""^
			 "values["^ssc (source_code_of_code (get_var key))^"] = "^ssc (source_code_of_code t)^"\n\n"^
			 "for key,value in values:\n		print '%s\\t%s'% ("^ssc (source_code_of_code (get_var key))^", "^ssc (source_code_of_code (get_var value))^")\n")
	| Stage(i,o,m,r) -> 
		begin match i with
			| InputCodeT s -> inl(ssc (source_code_of_code s)^"\n\n"^ssc (source_code_of_code m)^"\n\n"^ssc (source_code_of_code r)^"###############################\n")
			| _ -> inl(ssc (source_code_of_code m)^"\n\n"^ssc (source_code_of_code r)^"###############################\n")
		end
		
		
		(*begin match r with
			| None -> inl("ssc (source_code_of_code m)^"  ")
			| Some r -> inl("ssc (source_code_of_code m)^ssc (source_code_of_code r))
		end
		*)
	| Trigger(a,b,alst) -> inl("#hdfs input "^string_of_data_t a^"\n"^(String.concat "\n" (List.map ssc (List.map source_code_of_code b)))^"\n"^String.concat "\n" (List.map string_of_code_t b)^"\n")
	| Main sc -> concat_and_delim_source_code_list sc
	| _ -> failwith "no source"
  end 


(* Source code stringification aliases *)
let inline_var_list sl = List.map (fun x -> inl x) sl
 
let ssc = string_of_source_code
let csc = concat_source_code
let esc = empty_source_code
let dsc = delim_source_code
let cdsc = concat_and_delim_source_code
let cscl = concat_and_delim_source_code_list
let isc = indent_source_code
let rec iscn ?(n=1) delim bc =
  if n = 0 then bc else iscn ~n:(n-1) delim (isc delim bc) 
let ivl = inline_var_list

let sequence ?(final=false) ?(delim=stmt_delimiter) cl =
  cscl ~final:final ~delim:delim (List.filter (fun x -> not(esc x)) cl)

  
(* Temp *)
let sql_string_of_float f =
let r = string_of_float f in
if r.[(String.length r)-1] = '.' then r^"0" else r

let counters = Hashtbl.create 0

let var_c = "vars"
let var_prefix = "var"

let agg_c = "aggs"
let agg_prefix = "agg"

let record_c = "records"
let record_prefix = "tup"

let rel_c = "relations"
let rel_prefix = "rel"

let rel_alias = "ralias"
let rel_alias_prefix = "R"
let init_counters =
Hashtbl.add counters var_c 0;
Hashtbl.add counters record_c 0;
Hashtbl.add counters agg_c 0;
Hashtbl.add counters rel_c 0;
Hashtbl.add counters rel_alias 0

let incr_counter n =
let v = try Hashtbl.find counters n with Not_found -> 0
in Hashtbl.replace counters n (v+1)

let get_counter n = try Hashtbl.find counters n with Not_found -> 0

let reset_syms() = Hashtbl.clear counters

let build_key_condition key_l =
  snd (List.fold_left (fun (cnt,acc) c -> match c with
    | Const c -> cnt+1, acc@[inl("a"^(string_of_int cnt)^"="^c)]
    | Var v -> cnt+1, acc@[inl("a"^(string_of_int cnt)^"="^v)]
    | _ -> failwith "invalid key") (1,[]) key_l)

let pop_back l =
  let x,y = List.fold_left
    (fun (acc,rem) v -> match rem with 
      | [] -> (acc, [v]) | _ -> (acc@[v], List.tl rem)) 
    ([], List.tl l) l
  in x, List.hd y


let rec substitute c (replacements :(code_t * code_t) list) =
	begin match c with
		| Bind([arg], b) -> substitute b replacements
		| Const(x) -> 
			if List.mem_assoc (Const(x))  replacements
			then List.assoc (Const(x))  replacements
			else Const(x)
		| Var(x) -> 
			if List.mem_assoc (Var(x))  replacements
			then List.assoc (Var(x))  replacements
			else (Var(x))
		| Tuple(x) -> 
			if List.mem_assoc (Tuple(x))  replacements
			then List.assoc (Tuple(x))  replacements
			else Tuple(x)
		| Op(op,l,r) -> Op(op, substitute l replacements, substitute r replacements)
		| Lookup(a, b) ->
			Lookup(a, 
				let tempSub x = substitute x replacements 
				in	List.map tempSub b)
		| Update(a, b, c) -> let tempSub x = substitute x replacements 
						  in
							Update(a, List.map tempSub b, tempSub c)
		| _ -> Const "test"
	end
  
  
(* Terminals *)
    let const ?(expr = None) c = match c with
		CFloat x -> Const(sql_string_of_float x)

	let var ?(expr = None) id _ = Var(id)

    (* Tuples *)
    let tuple ?(expr=None) fields = Tuple(fields)
	
	
    let project ?(expr = None) tuple idx = Const("PROJECT")
		
    (* Native collections *)
    let singleton ?(expr = None) cel cel_t =
		begin match cel with
			| Const _ | Var _ -> Lookup(HBase(string_of_code_t cel), [Const "\"\"";Const "\"\""])
			|  _ -> failwith "invalid singleton input"
		end
  
    let combine ?(expr = None) c1 c2 =
		begin match c1,c2 with 
		| _,_ -> failwith "invalid collections in combine" 
  end

    (* Arithmetic, comparision operators *)
    (* op type, lhs, rhs -> op *)
    let op ?(expr = None) op l r = Op(op,l,r)

    (* Control flow *)
    (* predicate, then clause, else clause -> condition *) 
    let ifthenelse ?(expr = None) p t e = IfThen(p,t)

    (* statements -> block *)    
    let block ?(expr = None) cl = Const("BLOCK!!!!!!!!!!!!!!!!!!")

    (* iter fn, collection -> iterate *)
    let iterate ?(expr = None) fn c = 
		begin match fn,c with
			| Bind([x],b), y ->
					begin match y with
					(*Stage(_,_,_,Reducer(AVar(key,_),AVar(value,_),_,_,_)) -> *)
					Stage(_,_,Mapper(ATuple([key,_;value,_]),_,_,_),_) ->
						Stage(InputCodeT y, HBase "", Mapper(ATuple([key,TUnit;value,TUnit]),  substitute b [Var("updated_v"), Var(value)], Const "sys.stdin", []), Const "")
					end
			| _,_ -> failwith "invalid iterate function/collection" 
		end
	  
    (* Functions *)
    (* arg, body -> fn *)
    let lambda ?(expr = None) arg b = Bind([arg],b)
    
    (* arg1, type, arg2, type, body -> assoc fn *)
	let assoc_lambda ?(expr = None) arg1 arg2 b = Bind([arg1;arg2],b)
	
    (* fn, arg -> evaluated fn *)
    let apply ?(expr = None) fn arg = 
		begin match fn, arg with
			| Bind([x],b), y -> substitute fn [get_var x, y]
			| _,_ -> failwith "invalid apply function/collection"
		end
    
    (* Structural recursion operations *)
    (* map fn, collection -> map *)
    let map ?(expr = None) fn fn_rv_t c = 
		begin match fn, c with
			| Bind([x],b), y -> 
				begin match x, y with 
					ATuple([key,_;value,_]), Stage(_,_,_,_) -> 
						(*Stage(InputT (HBase ""), HBase "", Mapper(ATuple([key,TUnit;"value",TUnit]), y, Const "sys.stdin"), Reducer(AVar(key,TUnit),AVar(value,TUnit),Const "",b, []))*)
						Stage(InputCodeT y, HBase "", Mapper(ATuple([key,TUnit;value,TUnit]), b, Const "sys.stdin", []), Const "")
				end
			| _,_ -> failwith "invalid map function/collection"
		end
    
    (* agg fn, initial agg, collection -> agg *)
   let aggregate ?(expr = None) fn init c = 
		begin match fn, c with
			| Bind([x],b), y -> Const ""
			| Bind([arg1;arg2],b), y -> Const ""
			| _,_ -> failwith "invalid aggregate function/collection"
		end
    
    (* agg fn, initial agg, grouping fn, collection -> agg *)
	let group_by_aggregate ?(expr = None) fn init gbfn c =
		begin match fn, gbfn, c with
			| Bind([arg1;arg2],b), Bind([x],d), y -> 
				begin match arg1 with
				ATuple([key,_;_,_;value,_]) ->			
					Stage(InputT (HBase ""), HBase "", 
							 Mapper(ATuple([key,TUnit;"value",TUnit]), Const "", y, []), 
							 Reducer(AVar(key,TUnit),AVar(value,TUnit),Const "",substitute b [Var("accv"), Const ("values.get("^key^",0)")], []))
				end
			| _,_,_ -> failwith "invalid group_by_aggregate function/collection"
		end
    
    (* nested collection -> flatten *)
    let flatten ?(expr = None) c = Const("flatten")

    (* Tuple collection operators *)
    (* TODO: write documentation on datatypes + implementation requirements
     * for these methods *)
    (* map, key -> bool/float *)
   let exists ?(expr = None) map key_l = Exists(map, key_l)

    (* map, key -> map value *)
    let lookup ?(expr = None) map key_l = 
		begin match map with
			| Const s -> Lookup(HBase(s), key_l)
			| _ -> Lookup(HBase(string_of_code_t map), key_l)
		end
    
    (* map, partial key, pattern -> slice *)
    (* TODO: see notes on datatype conversions/secondary indexes *)
    let slice ?(expr = None) map pk pat = 
		begin match map with
			| Const s -> Slice(HBase(s),pk,pat)
			| _ -> Slice(HBase(string_of_code_t map),pk,pat)
		end
	

    (* Persistent collections *)
    
    (* K3 API *)
    (*
   let get_value ?(expr = None) _ id = Lookup(HBase(Const(id)),_)
    
    (* Database udpate methods *)
    let update_value ?(expr = None) c = match c with
		CFloat x -> Const(sql_string_of_float x)
    let update_map ?(expr = None) c = match c with
		CFloat x -> Const(sql_string_of_float x)
    let update_io_map ?(expr = None) c = match c with
		CFloat x -> Const(sql_string_of_float x)
    *)

    (* M3 API *)

    (* Database retrieval methods *)
   let get_value ?(expr = None) _ id = Lookup(HBase(id),[])

    let get_in_map  ?(expr = None) _ _ id = Const(id)
	let get_out_map ?(expr = None) _ _ id = Const(id)
	let get_map     ?(expr = None) (_,_) _ id = Const(id)
    
    
    (* Database udpate methods *)
    (* persistent collection id, value -> update *)
	(* persistent collection id, value -> update *)
	let update_value ?(expr = None) id vc =
		Update(HBase(id), [], vc)

	(* persistent collection id, in key, value -> update *)
	let update_in_map_value ?(expr = None) id keys vc =
		Update(HBase(id), keys, vc)

	(* persistent collection id, out key, value -> update *)
	let update_out_map_value ?(expr = None) id keys vc =
		Update(HBase(id), keys, vc)

	(* persistent collection id, in key, out key, value -> update *)
	let update_map_value ?(expr = None) id in_keys out_keys vc =
		Update(HBase(id), in_keys@out_keys, vc)


		
		
		
		
(* These updates should not be needed
 * -- TODO: should we remove them from the codegen API?
 *)
(* persistent collection id, updated collection -> update *)
let update_in_map ?(expr = None) id cc = failwith "NYI"

let update_out_map ?(expr = None) id cc = failwith "NYI"

(* persistent collection id, key, updated collection -> update *)
let update_map ?(expr = None) id keys cc = failwith "NYI"

    (* TODO: add update to n-th level in, as given by the key list
     * this should be typechecked to ensure updated collection has
     * same type as exsiting (n+1)-th level
     * persistent collection id, key list, updated collection -> update*)
    let update_nested_map ?(expr = None) c = match c with
		CFloat x -> Const(sql_string_of_float x)

		
		
		
		
    (* fn id -> code
     * -- code generator should be able to hooks to implementations of
     *    external functions, e.g. invoke function call *)
    (*
    let ext_fn ?(expr = None) c = match c with
		CFloat x -> Const(sql_string_of_float x)
    *)



    (* Toplevel: sources and main *)
    (* event, rel, trigger args, statement code block -> trigger code *)
    let trigger event rel trig_args stmt_block =
		let tempStage a = 
			begin match a with
			| Stage(_,_,_,_) ->
Stage(InputCodeT (a),HDFS "",Const "",Const "")
			| _ -> Stage(InputT (HDFS rel),HDFS "",Mapper((*ATuple(let tempInl x = ssc(inl(x)),TUnit in List.map tempInl trig_args)*)ATuple(["null",TUnit]), a, Const "sys.stdin", []),Const "")
			end
		in
		Trigger(HDFS rel, List.map tempStage stmt_block, List.map (fun x -> AVar(ssc(inl(x)), TUnit)) trig_args)



(* Sources *)
let valid_adaptors =
  ["csv",","; "orderbook",","; 
   "lineitem", "|"; "orders", "|";
   "part", "|"; "partsupp", "|";
   "customer", "|"; "supplier", "|"]

let make_file_source filename rel_adaptors =
  let va = List.filter (fun (_,(aname,_)) ->
    List.mem_assoc aname valid_adaptors) rel_adaptors
  in
  let aux (r,(aname,aparams)) =
    let delim =
      if List.mem_assoc "fields" aparams then
        List.assoc "fields" aparams
      else List.assoc aname valid_adaptors
    in inl("--copy "^r^" from '"^filename^"' with delimiter '"^delim^"'")
  in sequence ~final:true (List.map aux va)

(* source type, framing type, (relation * adaptor type) list 
 * -> source impl type,
 *    source and adaptor declaration code, (optional)
 *    source and adaptor initialization code (optional) *)
let source src fr rel_adaptors = 
  let source_stmts =
    match src,fr with
    | FileSource fn, Delimited "\n" -> make_file_source fn rel_adaptors
    | _ -> failwith "Unsupported data source" 
  in (source_stmts, None, None)
  
  
  
  

				 
				 
let sql_type_of_calc_type_t t = match t with
  | Calculus.TInt -> "integer"
  | Calculus.TLong -> "bigint"
  | Calculus.TDouble -> "double precision"
  | Calculus.TString -> "text"
 



let main dbschema schema patterns sources triggers toplevel_queries =
	let rec get_dependencies c n = 
			begin match c with
				| Tuple(lst) -> 
					List.concat (List.map (fun x -> get_dependencies x n) lst)
				| Op(op,l,r) -> List.concat [get_dependencies l n; get_dependencies r n]
				| IfThen(i,t) -> List.concat [get_dependencies i n; get_dependencies t n]
				| Lookup(a, b) -> [name_of_data_t a]@(List.concat (List.map (fun x -> get_dependencies x n) b))
				| Update(a, b, c) -> 
					(if (n = 0) then [] else [name_of_data_t a])@(List.concat (List.map (fun x -> get_dependencies x (n+1)) b))@(get_dependencies c (n+1))
				| Mapper(_,t,i,_) -> List.concat [get_dependencies t n; get_dependencies i n]
				| Reducer(_,_,a,b,_) -> List.concat [get_dependencies a n; get_dependencies b n]
				| Stage(_,_,m,r) -> List.concat [get_dependencies m 0; get_dependencies r 0]
				| _ -> []
			end
	in
	let rec get_update c= 
			begin match c with
				| Update(a, b, c) -> name_of_data_t a
				| IfThen(i,t) -> get_update t
				| Mapper(_,t,_,_) -> get_update t 
				| Stage(_,_,m,Const _) -> get_update m
				| Stage(_,_,m,r) -> get_update r
				| _ -> ""
			end
	in
	let get_first_stblock trigger =
		match trigger with
			| Trigger(rel, stmt_block, args) -> List.hd stmt_block
	in
	let rec get_dep_stblock stmtblocks dep =
		match stmtblocks with
			| hd::tl -> if (List.mem (get_update hd) (dep)) then hd else (get_dep_stblock tl dep)
	in
	let rec create_batch_program triggers dep =
		begin match triggers with
			| [] -> []
			| hd::tl ->
				begin match dep, hd with
					| [],Trigger(rel, stmt_block, args) -> 
						let stchosen = get_first_stblock hd
						in
						(Trigger(rel,[stchosen],args))::(create_batch_program (tl) (List.filter (fun x -> x<> (get_update stchosen)) (get_dependencies stchosen 0)))
					| _,Trigger(rel, stmt_block, args) ->
						let stchosen = get_dep_stblock stmt_block dep
						in
						(Trigger(rel,[stchosen],args))::(create_batch_program (tl) (get_dependencies (stchosen) 0))
				end
		end
	in
	let batch_triggers = create_batch_program triggers []
	in
  let nl = Lines[""] in
  let base_rels = sequence ~final:true (List.flatten (List.map (fun (id, sch) ->
     [inl ("writer.deleteTable("^id^")");
        inl ("writer.createTable("^id^")")]) dbschema))
  in
  let maps = sequence ~final:true (List.flatten (List.map
    (fun (id,ins,outs) ->
      let csv = String.concat ", " in
      let field (i,t) = "a"^(string_of_int i) in
      let field_and_type (i,t) = "a"^(string_of_int i)^" real" in
      let add_counter st l = snd(
        List.fold_left (fun (i,acc) x -> (i+1,acc@[(i,x)])) (st,[]) l) in
      let csv_ins f = csv (List.map f (add_counter 1 ins)) in
      let csv_outs f =
        csv (List.map f (add_counter (1+(List.length ins)) outs)) in
      let ivl_f, ivl_ft = csv_ins field, csv_ins field_and_type in
      let ovl_f, ovl_ft = csv_outs field, csv_outs field_and_type in
      let v_ft = "av real" in
      let concat_ne l = String.concat "," (List.filter (fun x -> x <> "") l) in
      let tbl_params = concat_ne [ivl_ft; ovl_ft; v_ft] in
      let key_params = concat_ne [ivl_f; ovl_f] in
      let secondaries =
        let pats =
          if List.mem_assoc id patterns then List.assoc id patterns else []
        in let idxname l = "idx"^(String.concat "" (List.map string_of_int l)) 
        in let col off l = String.concat ","
          (List.map (fun i -> "a"^(string_of_int (i+off))) l)
        in let aux name columns =
          if columns = "" then inl "" else inl "" 
        in List.map (fun p -> match p with 
            | M3Common.Patterns.In(x,y) -> aux (idxname y) (col 1 y) 
            | M3Common.Patterns.Out(x,y) ->
                aux (idxname y) (col (1+(List.length ins)) y)) pats
      in
      let singleton_init = if ins = [] && outs = []
        then [] else []
      in
        [inl ("writer.deleteTable("^id^")");
         inl ("writer.createTable("^id^")")]@
         secondaries@singleton_init)
    schema)) in
  let triggers_w_relargs = List.map (fun c -> match c with
    | Trigger(rel, stmt_block, args) -> 
		Trigger(rel,
			(let rec passarg s =
				begin match s with
				| Stage(InputCodeT (Stage(a,b,c,d)),o,m,r) -> Stage(InputCodeT (passarg (Stage(a,b,c,d))),o,m,r)
				| Stage(i,o,Mapper(a,t,ini,alst),r) -> Stage(i,o,Mapper(a,t,ini,args),r) 
				end
			in 
			List.map passarg stmt_block), args)
    | _ -> failwith "invalid trigger") (List.rev batch_triggers) 
	in
	let stmt_triggers = 
		List.concat 
		(List.map 
			(fun x ->
				let rec flatten_stages rel stmt args = 
					begin match stmt with
						| Stage(InputCodeT (s),_,_,_) ->
							(flatten_stages rel s args)@[Trigger(rel,[stmt],args)]
						| _ ->
							[Trigger(rel,[stmt],args)]
					end 
				in
				match x with 
					| Trigger(rel,[stmt],args) -> 
						flatten_stages rel stmt args)
		triggers_w_relargs)
	in
  let body = 
    List.map (fun x -> [source_code_of_code x]) stmt_triggers 
	in
	let shellscript = 
		let rec create_shell trigger_list n =
			begin match trigger_list with
				| [] -> []
				| hd::tl ->
					(match hd with
						|  Trigger(rel, [s], args) ->
							let string_of_shell input output reducer = inl(
								"bin/hadoop fs -rmr output"^output^"\n\n"^
								"bin/hadoop jar contrib/streaming/hadoop-0.20.2-streaming.jar "^
								"-file /Users/imbanubis/DBToaster/HadoopStreaming/Test/hbaseWriter.py "^
								"-file /Users/imbanubis/DBToaster/HadoopStreaming/Test/mapper"^output^".py "^
								"-mapper /Users/imbanubis/DBToaster/HadoopStreaming/Test/mapper"^output^".py "^
								"-reducer "^
								(begin match reducer with 
									| Const _ -> "/bin/cat"
									| _ -> "/Users/imbanubis/DBToaster/HadoopStreaming/Test/reducer"^output^".py"
								 end
								)
								^" -input "^input^" -output output"^output^"\n\n")
							in
							begin match s with
							| Stage(InputCodeT (Stage(a,b,c,d)),o,m,r) -> 
									string_of_shell ("output"^(string_of_int n)) (string_of_int n) r
							| Stage(i,o,Mapper(a,t,ini,alst),r) ->
									string_of_shell (name_of_data_t rel) (string_of_int n) r
							end
					)::(create_shell tl (n+1))
			end 
		in
		(inl("./CreateTables.py\ncd /Users/imbanubis/Hadoop/hadoop-0.20.2\n\n"))::(create_shell stmt_triggers 0)
	in
	let source_scl = List.map (fun (x,y,z) -> x) sources in
  let footer = sequence ~final:true
    ((List.flatten (List.map (fun c -> match c with    
		| Trigger(_,stmt_block,_) ->
			let rec getStage a =
				begin match a with 
					| Stage(i,o,m,r) -> 
						begin match i with
							| InputCodeT s -> (getStage s)^(string_of_code_t (Stage(InputT (HDFS "From last stage"), o, m, r)))^"\n"
							| _ -> 
								begin match m with
									| Mapper(ap,Stage(ip,op,mp,rp),inp,args) -> (getStage (Stage(ip,op,mp,rp)))(*^(string_of_code_t (Stage(i, o, Mapper(ap, Const "", inp,args), r)))^"\n"*)
									| _ -> (string_of_code_t a)^"\n"
								end
						end
					| _ -> (string_of_code_t a)^"\n"
				end
			in List.map inl (List.map getStage stmt_block)
        | _ -> failwith "invalid trigger code") batch_triggers))@
     (List.map (fun (id,_,_) -> inl("drop table "^id)) schema)@
     (List.map (fun (id,_) -> inl("drop table "^id)) dbschema))
	in 
	(*let deps = 
		match List.hd triggers with
			| Trigger(_,stmt_block,_) -> List.filter (fun x -> x <>(get_update (List.hd stmt_block))) (get_dependencies (List.hd stmt_block) 0)
	in
	let realStatement = 
		match List.hd (List.tl triggers) with
			| Trigger(_,stmt_block,_) -> inl(string_of_code_t (get_dep_stblock stmt_block deps))
	in*)
	Main ([(cscl ([base_rels;maps]));(cscl (shellscript))]@(List.map cscl body))

let output code out_chan = 
	let getUnit x = ()
	in
	let runOutput out =
		getUnit (List.map 
			(fun x -> 
				match x with 
					| (sc,out_chan) -> (output_string out_chan (ssc sc); flush out_chan)
			) out)
	in
	let runOutput1 out = 
		let open_outmapper n = 
			open_out ("mapper"^(string_of_int n)^".py")
		in 
		getUnit ( 
				match out with
					| (sc,out_chan) -> 
						let rec write_to_file lst n = 
							begin match lst with
								| [] -> []
								| hd::tl -> 
									(output_string (open_outmapper n) (ssc hd); flush (open_outmapper n))::write_to_file tl (n+1)
							end
						in write_to_file sc 0
		)
	in
	match code with
  | Main (cmap::shell::sc) -> 
		(runOutput [(cmap, (open_out "CreateTables.py"));(shell, open_out "runHadoop.sh")];(runOutput1 (sc, out_chan)))
	| _ -> failwith "invalid code"



let to_string code = 
  failwith "Can't stringify plsql for now"
let debug_string code = 
  failwith "Can't stringify plsql for now"

end
