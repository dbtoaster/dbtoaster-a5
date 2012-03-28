(***********************************************
 * K3, a simple collection language
 * admitting structural recursion optimizations
 ************************************************)
(* Notes:
 * -- K3 is a simple functional language with tuples, temporary and persistent
 *    collections, collection operators (iterate, map, flatten, aggregate,
 *    group-by aggregate), and persistence operations (get, update).
 * -- Here "persistent" means global for now, as needed by DBToaster triggers,
 *    rather than being long-lived over multiple program invocations. This
 *    suffices because DBToaster's continuous queries are long-running themselves.
 * -- Maps are persistent collections, where the collection is a tuple of
 *    keys and value. 
 * -- For tuple collection accessors:
 *   ++ we assume a call-by-value semantics, thus for persistent collections,
 *      the collections are repeatedly retrieved from the persistent store,
 *      and our code explicitly avoids this by passing them as arguments to
 *      functions.
 *   ++ these can operate on either temporary or persistent collections.
 *      It is the responsibility of the code generator to produce the correct
 *      implementation on the actual datatype representing the collection.
 * -- For persistent collections based on SliceableMaps:
 *   ++ code generator should instantiate the collection datastructure used
 *      during evaluation, whether they are lists or ValuationMaps.
 *   ++ code generator should strip any secondary indexes as needed.
 *   ++ currently in the interpreter, during slicing we convert a persistent
 *      collection into a temporary one, from a SliceableMap to TupleList
 *   ++ updates should add any secondary indexes as needed
 *)

open Format


type k3_var_t = string
type k3_map_type_t = string * (Types.type_t list) * (Types.type_t list)
	
(*
type m3schema  = m3_var_t list
type extension = m3_var_t list
type pattern   = int list
*)

(* Signatures *)
module type SRSig =
sig
		type id_t = string
		type coll_id_t = string
		
    (* Daniel
    type prebind   = M3.Prepared.pprebind_t
    type inbind    = M3.Prepared.pinbind_t
    *)
    (*
    type fn_id_t = string
    type ext_fn_id = Symbol of fn_id_t
    *)

    type type_t =
	      TUnit 
			| TBase      of Types.type_t      
      | TTuple     of type_t list          (* unnamed records *)
	    | Collection of type_t               (* collections *)
	    | Fn         of type_t list * type_t (* args * body *)

    type schema = (id_t * type_t) list

    type arg_t = AVar of id_t * type_t | ATuple of (id_t * type_t) list

    type expr_t =
   
   
       (* Terminals *)
         Const         of Types.const_t
       | Var           of id_t        * type_t

       (* Tuples, i.e. unnamed records *)
       | Tuple         of expr_t list
       | Project       of expr_t      * int list
    
       (* Collection construction *)
       | Singleton     of expr_t
       | Combine       of expr_t      * expr_t 
    
       (* Arithmetic and comparison operators, conditionals *) 
       | Add           of expr_t      * expr_t
       | Mult          of expr_t      * expr_t
       | Eq            of expr_t      * expr_t
       | Neq           of expr_t      * expr_t
       | Lt            of expr_t      * expr_t
       | Leq           of expr_t      * expr_t
       | IfThenElse0   of expr_t      * expr_t

       (* Control flow: conditionals, sequences, side-effecting
        * iteration over collections *)
       | Comment       of string      * expr_t
       | IfThenElse    of expr_t      * expr_t   * expr_t
       | Block         of expr_t list
       | Iterate       of expr_t      * expr_t
    
       (* Functions *)
       | Lambda        of arg_t       * expr_t
       | AssocLambda   of arg_t       * arg_t    * expr_t
       | Apply         of expr_t      * expr_t
    
       (* Structural recursion operators *)
       | Map              of expr_t      * expr_t 
       | Flatten          of expr_t 
       | Aggregate        of expr_t      * expr_t   * expr_t
       | GroupByAggregate of expr_t      * expr_t   * expr_t * expr_t

       (* Tuple collection operators *)
       | Member      of expr_t      * expr_t list  
       | Lookup      of expr_t      * expr_t list
       | Slice       of expr_t      * schema      * (id_t * expr_t) list

       (* Persistent collections *)
       | SingletonPC   of coll_id_t   * type_t
       | OutPC         of coll_id_t   * schema   * type_t
       | InPC          of coll_id_t   * schema   * type_t
       | PC            of coll_id_t   * schema   * schema    * type_t

       | PCUpdate      of expr_t      * expr_t list * expr_t
       | PCValueUpdate of expr_t      * expr_t list * expr_t list * expr_t 
	   (*| External      of ext_fn_id*)

    (* K3 methods *)


    (* Traversal helpers *)
    val get_branches : expr_t -> expr_t list list
    val rebuild_expr : expr_t -> expr_t list list -> expr_t
    val descend_expr : (expr_t -> expr_t) -> expr_t -> expr_t
    
    (* Tree traversal *)

    (* map: pre- and post-order traversal of an expression tree, applying the
     * map function at every node
     *)
    val pre_map_expr : (expr_t -> expr_t) -> expr_t -> expr_t
    val post_map_expr : (expr_t -> expr_t) -> expr_t -> expr_t

    (* fold f pre acc init e
     * applies f to each subexpr of e, with both a top-down and bottom-up
     * accumulator (acc, return value respectively), using init as the
     * bottom-up accumulator at leaves.
     * pre transforms the top-down accumulator prior to recursive calls
     * the bottom-up accumulation is a list of lists, representing
     * list branches at internal nodes (i.e. where a child can be a list
     * itself, as with tuples, tuple collection accessors, etc) 
     *)
    val fold_expr :
      ('b -> 'a list list -> expr_t -> 'a) ->
      ('b -> expr_t -> 'b) -> 'b -> 'a -> expr_t -> 'a

    (* Returns whether second expression is a subexpression of the first *)
    val contains_expr : expr_t -> expr_t -> bool

    val string_of_type : type_t -> string
    val string_of_arg  : arg_t -> string
    val string_of_expr : expr_t -> string
    val code_of_expr   : expr_t -> string
    
    (* Helpers *)
    val collection_of_list : expr_t list -> expr_t
    val collection_of_float_list : float list -> expr_t

    (* Incremental section *)
    type statement = expr_t * expr_t
		type ev_type_t = Insert | Delete
    type trigger = ev_type_t * string * k3_var_t list * statement list
    type program = k3_map_type_t list * Patterns.pattern_map * trigger list
    
end


module SR : SRSig =
struct

(* Metadata for code generation *)

type id_t = string
type coll_id_t = string

(* K3 Typing.
 * -- collection type is used for both persistent and temporary collections.
 * -- maps are collections of tuples, where the tuple includes key types
 *    and the value type. 
 *)
type type_t =
      TUnit 
		| TBase      of Types.type_t
    | TTuple     of type_t list
    | Collection of type_t
    | Fn         of type_t list * type_t

(* Schemas are carried along with persistent map references,
 * and temporary slices *)
type schema = (id_t * type_t) list


type arg_t = AVar of id_t * type_t | ATuple of (id_t * type_t) list

(* External functions *)
(*
type ext_fn_type_t = type_t list * type_t   (* arg, ret type *)

type fn_id_t = string
type ext_fn_id = Symbol of fn_id_t

type symbol_table = (fn_id_t, ext_fn_type_t) Hashtbl.t
let ext_fn_symbols : symbol_table = Hashtbl.create 100
*)

(* Expression AST *)
type expr_t =
   
   (* Terminals *)
     Const         of Types.const_t
   | Var           of id_t        * type_t

   (* Tuples, i.e. unnamed records *)
   | Tuple         of expr_t list
   | Project       of expr_t      * int list

   (* Collection construction *)
   | Singleton     of expr_t
   | Combine       of expr_t      * expr_t 

   (* Arithmetic and comparison operators, conditionals *) 
   | Add           of expr_t      * expr_t
   | Mult          of expr_t      * expr_t
   | Eq            of expr_t      * expr_t
   | Neq           of expr_t      * expr_t
   | Lt            of expr_t      * expr_t
   | Leq           of expr_t      * expr_t
   | IfThenElse0   of expr_t      * expr_t

   (* Control flow: conditionals, sequences, side-effecting iterations *)
   | Comment       of string      * expr_t
   | IfThenElse    of expr_t      * expr_t   * expr_t
   | Block         of expr_t list 
   | Iterate       of expr_t      * expr_t  
     
   (* Functions *)
   | Lambda        of arg_t       * expr_t
   | AssocLambda   of arg_t       * arg_t    * expr_t
   | Apply         of expr_t      * expr_t

   (* Structural recursion operators *)
   | Map              of expr_t      * expr_t 
   | Flatten          of expr_t
   | Aggregate        of expr_t      * expr_t   * expr_t
   | GroupByAggregate of expr_t      * expr_t   * expr_t * expr_t

   (* Tuple collection accessors *)
   | Member      of expr_t      * expr_t list  
   | Lookup      of expr_t      * expr_t list
   | Slice       of expr_t      * schema      * (id_t * expr_t) list
   
   (* Persistent collection types w.r.t in/out vars *)
   | SingletonPC   of coll_id_t   * type_t
   | OutPC         of coll_id_t   * schema   * type_t
   | InPC          of coll_id_t   * schema   * type_t
   | PC            of coll_id_t   * schema   * schema    * type_t

   (* map, key (optional, used for double-tiered), tier *)
   | PCUpdate      of expr_t      * expr_t list * expr_t

   (* map, in key (optional), out key, value *)   
   | PCValueUpdate of expr_t      * expr_t list * expr_t list * expr_t 

   (*| External      of ext_fn_id*)


(* Expression traversal helpers *)

let get_branches (e : expr_t) : expr_t list list =
    begin match e with
    | Const            c                    -> []
    | Var              (id,t)               -> []
    | Tuple            e_l                  -> List.map (fun e -> [e]) e_l
    | Project          (ce, idx)            -> [[ce]]
    | Singleton        ce                   -> [[ce]]
    | Combine          (ce1,ce2)            -> [[ce1];[ce2]]
    | Add              (ce1,ce2)            -> [[ce1];[ce2]]
    | Mult             (ce1,ce2)            -> [[ce1];[ce2]]
    | Eq               (ce1,ce2)            -> [[ce1];[ce2]]
    | Neq              (ce1,ce2)            -> [[ce1];[ce2]]
    | Lt               (ce1,ce2)            -> [[ce1];[ce2]]
    | Leq              (ce1,ce2)            -> [[ce1];[ce2]]
    | Comment          (_, ce1)             -> [[ce1]]
    | IfThenElse0      (ce1,ce2)            -> [[ce1];[ce2]]
    | IfThenElse       (pe,te,ee)           -> [[pe];[te];[ee]]
    | Block            e_l                  -> [e_l]
    | Iterate          (fn_e, ce)           -> [[fn_e];[ce]]
    | Lambda           (arg_e,be)           -> [[be]]
    | AssocLambda      (arg1_e,arg2_e,be)   -> [[be]]
    | Apply            (fn_e,arg_e)         -> [[fn_e];[arg_e]]
    | Map              (fn_e,ce)            -> [[fn_e];[ce]]
    | Flatten          ce                   -> [[ce]]
    | Aggregate        (fn_e,i_e,ce)        -> [[fn_e];[i_e];[ce]]
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> [[fn_e];[i_e];[ge];[ce]]
    | SingletonPC      (id,t)               -> []
    | OutPC            (id,outs,t)          -> []
    | InPC             (id,ins,t)           -> []
    | PC               (id,ins,outs,t)      -> []
    | Member           (me,ke)              -> [[me];ke]  
    | Lookup           (me,ke)              -> [[me];ke]
    | Slice            (me,sch,pat_ve)      -> [[me];List.map snd pat_ve]
    | PCUpdate         (me,ke,te)           -> [[me];ke;[te]]
    | PCValueUpdate    (me,ine,oute,ve)     -> [[me];ine;oute;[ve]]
    (*| External         efn_id               -> [] *)
    end

(* Tree reconstruction, given a list of branches.
 * This can be used with fold_expr above with the bottom-up accumulator
 * as expressions, to enable stateful expression mappings.
 * Note we ignore the parts for base terms: here parts is assumed to be
 * some dummy value. *)
let rebuild_expr e (parts : expr_t list list) =
    let fst () = List.hd parts in
    let snd () = List.nth parts 1 in
    let thd () = List.nth parts 2 in
    let fth () = List.nth parts 3 in
    let sfst () = List.hd (fst()) in
    let ssnd () = List.hd (snd()) in
    let sthd () = List.hd (thd()) in
    let sfth () = List.hd (fth()) in
    begin match e with
    | Const            c                    -> e
    | Var              (id,t)               -> e
    | Tuple            e_l                  -> Tuple(List.flatten parts)
    | Project          (ce, idx)            -> Project(sfst(), idx)
    | Singleton        ce                   -> Singleton (sfst())
    | Combine          (ce1,ce2)            -> Combine(sfst(),ssnd())
    | Add              (ce1,ce2)            -> Add(sfst(),ssnd())
    | Mult             (ce1,ce2)            -> Mult(sfst(),ssnd())
    | Eq               (ce1,ce2)            -> Eq(sfst(),ssnd())
    | Neq              (ce1,ce2)            -> Neq(sfst(),ssnd())
    | Lt               (ce1,ce2)            -> Lt(sfst(),ssnd())
    | Leq              (ce1,ce2)            -> Leq(sfst(),ssnd())
    | IfThenElse0      (ce1,ce2)            -> IfThenElse0(sfst(),ssnd())
    | Comment          (c,ce1)              -> Comment(c, sfst())
    | IfThenElse       (pe,te,ee)           -> IfThenElse(sfst(),ssnd(),sthd())
    | Block            e_l                  -> Block(fst())
    | Iterate          (fn_e, ce)           -> Iterate(sfst(),ssnd())
    | Lambda           (arg_e,ce)           -> Lambda (arg_e,sfst())
    | AssocLambda      (arg1_e,arg2_e,be)   -> AssocLambda(arg1_e,arg2_e,sfst())
    | Apply            (fn_e,arg_e)         -> Apply(sfst(),ssnd())
    | Map              (fn_e,ce)            -> Map(sfst(),ssnd())
    | Flatten          ce                   -> Flatten(sfst())
    | Aggregate        (fn_e,i_e,ce)        -> Aggregate(sfst(),ssnd(),sthd())
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> GroupByAggregate(sfst(),ssnd(),sthd(),sfth())
    | SingletonPC      (id,t)               -> e
    | OutPC            (id,outs,t)          -> e
    | InPC             (id,ins,t)           -> e
    | PC               (id,ins,outs,t)      -> e
    | Member           (me,ke)              -> Member(sfst(),snd())  
    | Lookup           (me,ke)              -> Lookup(sfst(),snd())
    | Slice            (me,sch,pat_ve)      ->
        Slice(sfst(),sch,List.map2 (fun (id,_) e -> id,e) pat_ve (snd()))
    | PCUpdate         (me,ke,te)           -> PCUpdate(sfst(), snd(), sthd())
    | PCValueUpdate    (me,ine,oute,ve)     -> PCValueUpdate(sfst(),snd(),thd(),sfth())
    (*| External         efn_id               -> sfst() *)
    end

(* Apply a function to all its children *)
let descend_expr (f : expr_t -> expr_t) e =
    begin match e with
    | Const            c                    -> e
    | Var              (id,t)               -> e
    | Tuple            e_l                  -> Tuple (List.map f e_l)
    | Project          (ce, idx)            -> Project (f ce, idx)
    | Singleton        ce                   -> Singleton (f ce)
    | Combine          (ce1,ce2)            -> Combine (f ce1, f ce2)
    | Add              (ce1,ce2)            -> Add (f ce1, f ce2)
    | Mult             (ce1,ce2)            -> Mult (f ce1, f ce2)
    | Eq               (ce1,ce2)            -> Eq (f ce1, f ce2)
    | Neq              (ce1,ce2)            -> Neq (f ce1, f ce2)
    | Lt               (ce1,ce2)            -> Lt (f ce1, f ce2)
    | Leq              (ce1,ce2)            -> Leq (f ce1, f ce2)
    | IfThenElse0      (ce1,ce2)            -> IfThenElse0 (f ce1, f ce2)
    | Comment          (c, ce1)             -> Comment(c, f ce1)
    | IfThenElse       (pe,te,ee)           -> IfThenElse (f pe, f te, f ee)
    | Block            e_l                  -> Block (List.map f e_l)
    | Iterate          (fn_e, ce)           -> Iterate (f fn_e, f ce)
    | Lambda           (arg_e,ce)           -> Lambda (arg_e, f ce)
    | AssocLambda      (arg1_e,arg2_e,be)   -> AssocLambda (arg1_e, arg2_e, f be)
    | Apply            (fn_e,arg_e)         -> Apply (f fn_e, f arg_e)
    | Map              (fn_e,ce)            -> Map (f fn_e, f ce)
    | Flatten          ce                   -> Flatten (f ce)
    | Aggregate        (fn_e,i_e,ce)        -> Aggregate (f fn_e, f i_e, f ce)    
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> GroupByAggregate (f fn_e, f i_e, f ge, f ce)
    | SingletonPC      (id,t)               -> e
    | OutPC            (id,outs,t)          -> e
    | InPC             (id,ins,t)           -> e
    | PC               (id,ins,outs,t)      -> e
    | Member           (me,ke)              -> Member(f me, List.map f ke)  
    | Lookup           (me,ke)              -> Lookup(f me, List.map f ke)
    | Slice            (me,sch,pat_ve)      -> Slice(f me, sch, List.map (fun (id,e) -> id, f e) pat_ve)
    | PCUpdate         (me,ke,te)           -> PCUpdate(f me, List.map f ke, f te)
    | PCValueUpdate    (me,ine,oute,ve)     -> PCValueUpdate(f me, List.map f ine, List.map f oute, f ve)
    (*| External         efn_id               -> e *)
    end


(* Map: pre- and post-order traversal of an expression tree, applying the
 * map function at every node *)
let rec pre_map_expr (f : expr_t -> expr_t) (e : expr_t) : expr_t =
    descend_expr (pre_map_expr f) (f e)

let rec post_map_expr (f : expr_t -> expr_t) (e : expr_t) : expr_t =
    f (descend_expr (post_map_expr f) e)

(* A fold function that supports both bottom-up and top-down accumulation.
 * Arguments:
 * -- f, a folding function to be applied at every AST node. This function
 *    should accept top-down accumulations, a branch-based list of bottom-up
 *    accumulations and an expression. It should yield a bottom-up accumulation
 *    for this expression.
 * -- pre, a function that computes top-down state to pass to a recursive
 *    invocation of fold on a child. It accepts a parent expression.
 * -- acc, accumulated state from parents
 * -- init, initial bottom-up state at every leaf node
 *)
let rec fold_expr (f : 'b -> 'a list list -> expr_t -> 'a)
                  (pre : 'b -> expr_t -> 'b)
                  (acc : 'b) (init : 'a) (e: expr_t) : 'a =
    let nacc = pre acc e in
    let app_f = f nacc in
    let recur = fold_expr f pre nacc init in
    let sub ll = List.map (fun l -> List.map recur l) ll in
    begin match e with
    | Const            c                    -> app_f [[init]] e
    | Var              (id,t)               -> app_f [[init]] e
    | SingletonPC      (id,t)               -> app_f [[init]] e
    | OutPC            (id,outs,t)          -> app_f [[init]] e
    | InPC             (id,ins,t)           -> app_f [[init]] e
    | PC               (id,ins,outs,t)      -> app_f [[init]] e
    (*| External         efn_id               -> app_f [[init]] e *)
    | _ -> app_f (sub (get_branches e)) e
    end

(* Containment *)
let contains_expr e1 e2 =
  let contains_aux _ parts_contained e =
    (List.exists (fun x -> x) (List.flatten parts_contained)) || (e = e2)
  in fold_expr contains_aux (fun x _ -> x) None false e1

(* Stringification *)
let rec string_of_type t =
    match t with
      TUnit -> "TUnit" 
		| TBase(b_t) -> Types.string_of_type b_t
    | TTuple(t_l) -> "TTuple("^(String.concat " ; " (List.map string_of_type t_l))^")"
    | Collection(c_t) -> "Collection("^(string_of_type c_t)^")"
    | Fn(a,b) -> "Fn("^(String.concat "," (List.map string_of_type a))^
                    ","^(string_of_type b)^")"

let string_of_arg a = match a with
    | AVar(v,v_t) -> "AVar(\""^v^"\","^(string_of_type v_t)^")"
    | ATuple(args) -> 
        let f = String.concat ";"
          (List.map (fun (x,y) -> "\""^x^"\","^(string_of_type y)) args)
        in "ATuple(["^f^"])"

let string_of_expr e =
  let ob () = pp_open_hovbox str_formatter 2 in
  let cb () = pp_close_box str_formatter () in
  let pc () = pp_print_cut str_formatter () in
  let ps s = pp_print_string str_formatter s in
  let rec aux e =
    let recur list_branches = 
      let br = (get_branches e) in
      let nb = List.length br in
        ignore(List.fold_left
           (fun cnt l -> pc();
             if l = [] then ps "[]"
             else if List.mem cnt list_branches then
                (ps "["; List.iter (fun x -> aux x; ps ";"; pc()) l; ps "]") 
             else List.iter aux l;
             if cnt < (nb-1) then ps "," else (); cnt+1)
           0 br)
    in
    let pid id = ps ("\""^id^"\"") in
    let pop ?(lb = []) s = ob(); ps s; ps "("; recur lb; ps ")"; cb() in 
    let schema args = 
      "[" ^
      (String.concat ";"
        (List.map (fun (x,y) -> "\""^x^"\","^(string_of_type y)) args)) ^
      "]"
    in
    let pmap s id sch t = 
      ob(); ps s; ps "("; pid id; ps sch; 
                          ps (","^(string_of_type t)); ps ")"; cb() in
    match e with
    | Const c -> 
      let const_ts = match c with
        | Types.CFloat _ -> "CFloat"
        | Types.CString _ -> "CString"
				| Types.CInt _ -> "CInt"
				| Types.CBool _ -> "CBool" 
      in ob(); ps ("Const("^const_ts^"("^(Types.string_of_const c)^"))"); cb()
    | Var (id,t) -> ob(); ps "Var("; pid id; ps ","; 
                                     ps (string_of_type t); ps ")"; cb()

    | SingletonPC(id,t) -> pmap "SingletonPC" id "" t
    | OutPC(id,outs,t)  -> pmap "OutPC" id (","^(schema outs)) t
    | InPC(id,ins,t)    -> pmap "InPC" id (","^(schema ins)) t
    | PC(id,ins,outs,t) -> pmap "PC" id (","^(schema ins)^","^(schema outs)) t

    | Project (_, idx) ->
        ob(); ps "Project("; recur []; ps ",";
        ps ("["^(String.concat "," (List.map string_of_int idx))^"]");
        ps")"; cb()

    | Lambda           (arg_e,ce)           ->
        ob(); ps "Lambda(";
        ps (string_of_arg arg_e); ps ","; recur [];
        ps ")"; cb()

    | AssocLambda      (arg1_e,arg2_e,be)   ->
        ob(); ps "AssocLambda(";
        ps (string_of_arg arg1_e); ps ","; ps (string_of_arg arg2_e); 
        ps ","; recur []; ps ")"; cb()
    
    | Slice(pc, sch, vars) ->
        ob(); ps "Slice("; aux pc; ps ","; ps (schema sch); ps ",["; 
        (List.iter (fun (x,v) -> pid x; ps ",("; aux v; ps ");") vars); 
        ps "])"; cb()
    | Comment(c, cexpr) ->
        ob(); ps "(***"; ps c; ps "***)"; recur []; cb()
    
    | Tuple _             -> pop "Tuple"
    | Singleton _         -> pop "Singleton"
    | Combine _           -> pop "Combine"
    | Add  _              -> pop "Add"
    | Mult _              -> pop "Mult"
    | Eq   _              -> pop "Eq"
    | Neq  _              -> pop "Neq"
    | Lt   _              -> pop "Lt"
    | Leq  _              -> pop "Leq"
    | IfThenElse0 _       -> pop "IfThenElse0"
    | IfThenElse _        -> pop "IfThenElse"
    | Iterate _           -> pop "Iterate"
    | Apply _             -> pop "Apply"
    | Map _               -> pop "Map"
    | Flatten _           -> pop "Flatten"
    | Aggregate _         -> pop "Aggregate"
    | GroupByAggregate _  -> pop "GroupByAggregate"

    (* Pretty-print with list branches *)
    | Block _             -> pop ~lb:[0] "Block"
    | Member _            -> pop ~lb:[1] "Member"  
    | Lookup _            -> pop ~lb:[1] "Lookup"
    | PCUpdate _          -> pop ~lb:[1] "PCUpdate"
    | PCValueUpdate   _   -> pop ~lb:[1;2] "PCValueUpdate"
    (*| External         efn_id               -> pop "External" *)
    in pp_set_margin str_formatter 80; flush_str_formatter (aux e)


let rec code_of_expr e =
   let rcr ex = "("^(code_of_expr ex)^")" in
   let rec ttostr t = "("^(match t with
      | TUnit -> "K3.SR.TUnit"
      | TBase( b_t ) -> "Types."^(Types.ocaml_of_type b_t)
      | TTuple(tlist) -> 
         "K3.SR.TTuple("^(ListExtras.ocaml_of_list ttostr tlist)^")"
      | Collection(subt) -> "K3.SR.Collection("^(ttostr subt)^")"
      | Fn(argt,rett) -> 
         "K3.SR.Fn("^(ListExtras.ocaml_of_list ttostr argt)^","^(ttostr rett)^")"
   )^")" in
   let string_of_vpair (v,vt) = "\""^v^"\","^(ttostr vt) in
   let vltostr = ListExtras.ocaml_of_list string_of_vpair in
   let argstr arg = 
      match arg with
         | AVar(v,vt) -> 
            "K3.SR.AVar("^(string_of_vpair (v,vt))^")"
         | ATuple(vlist) -> 
            "K3.SR.ATuple("^(vltostr vlist)^")"
   in
   match e with
      | Const c -> 
        let const_ts = match c with
          | Types.CFloat _ -> "CFloat"
          | Types.CString _ -> "CString" 
					| Types.CInt _ -> "CInt"
          | Types.CBool _ -> "CBool" 
        in "K3.SR.Const(M3."^const_ts^"("^(Types.string_of_const c)^"))"
      | Var (id,t) -> "K3.SR.Var(\""^id^"\","^(ttostr t)^")"
      | Tuple e_l -> "K3.SR.Tuple("^(ListExtras.ocaml_of_list rcr e_l)^")"
      
      | Project (ce, idx) -> "K3.SR.Project("^(rcr ce)^","^
                           (ListExtras.ocaml_of_list string_of_int idx)^")"
      
      | Singleton ce      -> "K3.SR.Singleton("^(rcr ce)^")"
      | Combine (ce1,ce2) -> "K3.SR.Combine("^(rcr ce1)^","^(rcr ce2)^")"
      | Add  (ce1,ce2)    -> "K3.SR.Add("^(rcr ce1)^","^(rcr ce2)^")"
      | Mult (ce1,ce2)    -> "K3.SR.Mult("^(rcr ce1)^","^(rcr ce2)^")"
      | Eq   (ce1,ce2)    -> "K3.SR.Eq("^(rcr ce1)^","^(rcr ce2)^")"
      | Neq  (ce1,ce2)    -> "K3.SR.Neq("^(rcr ce1)^","^(rcr ce2)^")"
      | Lt   (ce1,ce2)    -> "K3.SR.Lt("^(rcr ce1)^","^(rcr ce2)^")"
      | Leq  (ce1,ce2)    -> "K3.SR.Leq("^(rcr ce1)^","^(rcr ce2)^")"
      
      | IfThenElse0 (ce1,ce2)  -> 
            "K3.SR.IfThenElse0("^(rcr ce1)^","^(rcr ce2)^")"
      | Comment(c, cexpr) ->
            "(*** "^c^" ***) "^(rcr cexpr)
      | IfThenElse  (pe,te,ee) -> 
            "K3.SR.IfThenElse("^(rcr pe)^","^(rcr te)^","^(rcr ee)^")"
      
      | Block   e_l        -> "K3.SR.Block("^(ListExtras.ocaml_of_list rcr e_l)^")"
      | Iterate (fn_e, ce) -> "K3.SR.Iterate("^(rcr fn_e)^","^(rcr ce)^")"
      | Lambda  (arg_e,ce) -> "K3.SR.Lambda("^(argstr arg_e)^","^(rcr ce)^")"
      
      | AssocLambda(arg1,arg2,be) ->
            "K3.SR.AssocLambda("^(argstr arg1)^","^(argstr arg2)^","^
                               (rcr be)^")"
      | Apply(fn_e,arg_e) -> 
            "K3.SR.Apply("^(rcr fn_e)^","^(rcr arg_e)^")"
      | Map(fn_e,ce) -> 
            "K3.SR.Map("^(rcr fn_e)^","^(rcr ce)^")"
      | Flatten(ce) -> 
            "K3.SR.Flatten("^(rcr ce)^")"
      | Aggregate(fn_e,i_e,ce) -> 
            "K3.SR.Aggregate("^(rcr fn_e)^","^(rcr i_e)^","^(rcr ce)^")"
      | GroupByAggregate(fn_e,i_e,ge,ce) -> 
            "K3.SR.GroupByAggregate("^(rcr fn_e)^","^(rcr i_e)^","^(rcr ge)^
                                    ","^(rcr ce)^")"
      | SingletonPC(id,t) -> 
            "K3.SR.SingletonPC(\""^id^"\","^(ttostr t)^")"
      | OutPC(id,outs,t) -> 
            "K3.SR.OutPC(\""^id^"\","^(vltostr outs)^","^(ttostr t)^")"
      | InPC(id,ins,t) -> 
            "K3.SR.InPC(\""^id^"\","^(vltostr ins)^","^(ttostr t)^")"
      | PC(id,ins,outs,t) -> 
            "K3.SR.PC(\""^id^"\","^(vltostr ins)^","^(vltostr ins)^","^
                      (ttostr t)^")"
      | Member(me,ke) -> 
            "K3.SR.Member("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^")"  
      | Lookup(me,ke) -> 
            "K3.SR.Lookup("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^")"
      | Slice(me,sch,pat_ve) -> 
            let pat_str = ListExtras.ocaml_of_list (fun (id,expr) ->
               "\""^id^"\","^(rcr expr)
            ) pat_ve in
            "K3.SR.Slice("^(rcr me)^","^(vltostr sch)^",("^pat_str^"))"
      | PCUpdate(me,ke,te) -> 
            "K3.SR.PCUpdate("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^","^
                            (rcr te)^")"
      | PCValueUpdate(me,ine,oute,ve) -> 
            "K3.SR.PCValueUpdate("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ine)^
                                 ","^(ListExtras.ocaml_of_list rcr oute)^","^
                                 (rcr ve)^")"

(* Native collection constructors *)
let collection_of_list (l : expr_t list) =
    if l = [] then failwith "invalid list for construction" else
    List.fold_left (fun acc v -> Combine(acc,Singleton(v)))
        (Singleton(List.hd l)) (List.tl l)

let collection_of_float_list (l : float list) =
    if l = [] then failwith "invalid list for construction" else
    List.fold_left (fun acc v -> Combine(acc,Singleton(Const(Types.CFloat(v)))))
        (Singleton(Const(Types.CFloat(List.hd l)))) (List.tl l)


(* Incremental section *)
type statement = expr_t * expr_t
type ev_type_t = Insert | Delete
type trigger = ev_type_t * string * k3_var_t list * statement list
type program = k3_map_type_t list * Patterns.pattern_map * trigger list
    
end
