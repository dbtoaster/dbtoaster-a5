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

module M3P = M3.Prepared
open M3
open M3Common

(* Signatures *)
module type SRSig =
sig
    type id_t = M3.var_id_t
    type coll_id_t = M3.map_id_t
    
    type prebind   = M3.Prepared.pprebind_t
    type inbind    = M3.Prepared.pinbind_t
    type m3schema  = M3.var_t list
    type extension = M3.var_t list
    type pattern   = int list

    (*
    type fn_id_t = string
    type ext_fn_id = Symbol of fn_id_t
    *)

    type type_t =
	      TUnit | TFloat | TInt
        | TTuple     of type_t list          (* unnamed records *)
	    | Collection of type_t               (* collections *)
	    | Fn         of type_t list * type_t (* args * body *)

    type schema = (id_t * type_t) list

    type arg_t = AVar of id_t * type_t | ATuple of (id_t * type_t) list

    type expr_t =
   
	   (* Terminals *)
	     Const         of M3.const_t
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

    val rebuild_expr : expr_t -> expr_t list list -> expr_t

    val string_of_type : type_t -> string
    val string_of_expr : expr_t -> string
    
    (* Helpers *)
    val collection_of_list : expr_t list -> expr_t
    val collection_of_float_list : float list -> expr_t

    (* Incremental section *)
    type statement = expr_t * expr_t
    type trigger = M3.pm_t * M3.rel_id_t * M3.var_t list * statement list
    type program = M3.map_type_t list * M3Common.Patterns.pattern_map * trigger list
    
end


module SR : SRSig =
struct

(* Metadata for code generation *)

(* From M3.Prepared *)
type prebind   = M3.Prepared.pprebind_t
type inbind    = M3.Prepared.pinbind_t
type m3schema  = M3.var_t list
type extension = M3.var_t list

type pattern   = int list

type id_t = M3.var_id_t
type coll_id_t = M3.map_id_t

(* K3 Typing.
 * -- collection type is used for both persistent and temporary collections.
 * -- maps are collections of tuples, where the tuple includes key types
 *    and the value type. 
 *)
type type_t =
      TUnit | TFloat | TInt
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
     Const         of M3.const_t
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

let rec map_expr (f : expr_t -> expr_t) (e : expr_t) : expr_t =
    let rcr = map_expr f in
    begin match e with
    | Const            c                    -> f e
    | Var              (id,t)               -> f e
    | Tuple            e_l                  -> f (Tuple (List.map rcr e_l))
    | Project          (ce, idx)            -> f (Project (rcr ce, idx))
    | Singleton        ce                   -> f (Singleton (rcr ce))
    | Combine          (ce1,ce2)            -> f (Combine (rcr ce1, rcr ce2))
    | Add              (ce1,ce2)            -> f (Add (rcr ce1, ce2))
    | Mult             (ce1,ce2)            -> f (Mult (rcr ce1, ce2))
    | Eq               (ce1,ce2)            -> f (Eq (rcr ce1, ce2))
    | Neq              (ce1,ce2)            -> f (Neq (rcr ce1, ce2))
    | Lt               (ce1,ce2)            -> f (Lt (rcr ce1, ce2))
    | Leq              (ce1,ce2)            -> f (Leq (rcr ce1, ce2))
    | IfThenElse0      (ce1,ce2)            -> f (IfThenElse0 (rcr ce1, ce2))
    | IfThenElse       (pe,te,ee)           ->
        f (IfThenElse (rcr pe, rcr te, rcr ee))

    | Block            e_l                  -> f (Block (List.map rcr e_l))
    | Iterate          (fn_e, ce)           -> f (Iterate (rcr fn_e, rcr ce))
    | Lambda           (arg_e,ce)           -> f (Lambda (arg_e, rcr ce))
    | AssocLambda      (arg1_e,arg2_e,be)   ->
        f (AssocLambda (arg1_e, arg2_e, rcr be))

    | Apply            (fn_e,arg_e)         -> f (Apply (rcr fn_e, rcr arg_e))
    | Map              (fn_e,ce)            -> f (Map (rcr fn_e, rcr ce))
    | Flatten          ce                   -> f (Flatten (rcr ce))
    | Aggregate        (fn_e,i_e,ce)        ->
        f (Aggregate (rcr fn_e, rcr i_e, rcr ce))
    
    | GroupByAggregate (fn_e,i_e,ge,ce)     ->
        f (GroupByAggregate (rcr fn_e, rcr i_e, rcr ge, rcr ce))

    | SingletonPC      (id,t)               -> f e
    | OutPC            (id,outs,t)          -> f e
    | InPC             (id,ins,t)           -> f e
    | PC               (id,ins,outs,t)      -> f e
    
    | Member           (me,ke)              ->
        f (Member(rcr me, List.map rcr ke))  

    | Lookup           (me,ke)              ->
        f (Lookup(rcr me, List.map rcr ke))
    
    | Slice            (me,sch,pat_ve)      ->
        f (Slice(rcr me, sch, List.map (fun (id,e) -> id, rcr e) pat_ve))
    
    | PCUpdate         (me,ke,te)           ->
        f (PCUpdate(rcr me, List.map rcr ke, rcr te))
    
    | PCValueUpdate    (me,ine,oute,ve)     ->
        f (PCValueUpdate(rcr me, List.map rcr ine, List.map rcr oute, rcr ve))
    (*| External         efn_id               -> f e *)
    end

(* Arguments:
 * -- f, a folding function to be applied at every AST node
 * -- g, a combiner for states accumulated at an internal node
 * -- init, initial state at every leaf node
 * Notes: given a fold function f, this fold does not thread the accumulator
 * through every AST node. Rather it applies f with initial value at the leaves
 * and uses a second function g to combine accumulated state at internal nodes
 *)
let rec fold_expr (f : 'b -> 'a list list -> expr_t -> 'a)
                  (pre : 'b -> expr_t -> 'b)
                  (acc : 'b)
                  (init : 'a)
                  (e: expr_t) : 'a =
    let nacc = pre acc e in
    let app_f = f nacc in
    let recur = fold_expr f pre nacc init in
    let sub ll = List.map (fun l -> List.map recur l) ll in
    begin match e with
    | Const            c                    -> app_f [[init]] e
    | Var              (id,t)               -> app_f [[init]] e
    | Tuple            e_l                  -> app_f (sub [e_l]) e
    | Project          (ce, idx)            -> app_f [[(recur ce)]] e
    | Singleton        ce                   -> app_f [[(recur ce)]] e
    | Combine          (ce1,ce2)            -> app_f (sub [[ce1];[ce2]]) e
    | Add              (ce1,ce2)            -> app_f (sub [[ce1];[ce2]]) e
    | Mult             (ce1,ce2)            -> app_f (sub [[ce1];[ce2]]) e
    | Eq               (ce1,ce2)            -> app_f (sub [[ce1];[ce2]]) e
    | Neq              (ce1,ce2)            -> app_f (sub [[ce1];[ce2]]) e
    | Lt               (ce1,ce2)            -> app_f (sub [[ce1];[ce2]]) e
    | Leq              (ce1,ce2)            -> app_f (sub [[ce1];[ce2]]) e
    | IfThenElse0      (ce1,ce2)            -> app_f (sub [[ce1];[ce2]]) e
    | IfThenElse       (pe,te,ee)           -> app_f (sub [[pe];[te];[ee]]) e
    | Block            e_l                  -> app_f (sub [e_l]) e
    | Iterate          (fn_e, ce)           -> app_f (sub [[fn_e]; [ce]]) e
    | Lambda           (arg_e,ce)           -> app_f [[(recur ce)]] e
    | AssocLambda      (arg1_e,arg2_e,be)   -> app_f [[(recur be)]] e
    | Apply            (fn_e,arg_e)         -> app_f (sub [[fn_e];[arg_e]]) e
    | Map              (fn_e,ce)            -> app_f (sub [[fn_e];[ce]]) e
    | Flatten          ce                   -> app_f [[(recur ce)]] e
    | Aggregate        (fn_e,i_e,ce)        -> app_f (sub [[fn_e]; [i_e]; [ce]]) e
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> app_f (sub [[fn_e]; [i_e]; [ge]; [ce]]) e
    | SingletonPC      (id,t)               -> app_f [[init]] e
    | OutPC            (id,outs,t)          -> app_f [[init]] e
    | InPC             (id,ins,t)           -> app_f [[init]] e
    | PC               (id,ins,outs,t)      -> app_f [[init]] e
    | Member           (me,ke)              -> app_f (sub ([me]::[ke])) e  
    | Lookup           (me,ke)              -> app_f (sub ([me]::[ke])) e
    | Slice            (me,sch,pat_ve)      -> app_f (sub ([me]::[(List.map snd pat_ve)])) e
    | PCUpdate         (me,ke,te)           -> app_f (sub ([[me];ke;[te]])) e
    | PCValueUpdate    (me,ine,oute,ve)     -> app_f (sub ([[me];ine;oute;[ve]])) e
    (*| External         efn_id               -> app_f init e *)
    end

(* Helper for fold to reconstruct the tree when the bottom accumulator are
 * K3 expressions themselves. This enables implementing stateful expression
 * mappings as folds
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
    match e with
    | Const            c                    -> e
    | Var              (id,t)               -> e
    | Tuple            e_l                  -> Tuple(fst())
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


(* Stringification *)
let rec string_of_type t =
    match t with
      TUnit -> "Unit" | TFloat -> "Float" | TInt -> "Int"
    | TTuple(t_l) -> "Tuple("^(String.concat " ; " (List.map string_of_type t_l))^")"
    | Collection(c_t) -> "Collection("^(string_of_type c_t)^")"
    | Fn(a,b) -> "( "^(String.concat " * " (List.map string_of_type a))^
                    " -> "^(string_of_type b)^" )"

let string_of_arg a = match a with
    | AVar(v,v_t) -> v^","^(string_of_type v_t)
    | ATuple(args) -> String.concat ","
        (List.map (fun (x,y) -> x^","^(string_of_type y)) args)

let string_of_expr e =
    let aux _ subll e =
    let sub = String.concat "," (List.flatten subll) in
    match e with
    | Const c -> "Const("^(string_of_const c)^")"
    | Var (id,t) -> "Var("^id^")"
    | Tuple e_l -> "Tuple("^sub^")"
    
    | Project (ce, idx) -> "Project("^sub^
        ",["^(String.concat "," (List.map string_of_int idx))^"])"
    
    | Singleton ce      -> "Singleton("^sub^")"
    | Combine (ce1,ce2) -> "Combine("^sub^")"
    | Add  (ce1,ce2)    -> "Add("^sub^")"
    | Mult (ce1,ce2)    -> "Mult("^sub^")"
    | Eq   (ce1,ce2)    -> "Eq("^sub^")"
    | Neq  (ce1,ce2)    -> "Neq("^sub^")"
    | Lt   (ce1,ce2)    -> "Lt("^sub^")"
    | Leq  (ce1,ce2)    -> "Leq("^sub^")"

    | IfThenElse0      (ce1,ce2)            -> "IfThenElse0("^sub^")"
    | IfThenElse       (pe,te,ee)           -> "IfThenElse("^sub^")"
    | Block            e_l                  -> "Block("^sub^")"
    | Iterate          (fn_e, ce)           -> "Iterate("^sub^")"
    | Lambda           (arg_e,ce)           ->
        "Lambda("^(string_of_arg arg_e)^","^sub^")"

    | AssocLambda      (arg1_e,arg2_e,be)   ->
        let x = String.concat "," [string_of_arg arg1_e; string_of_arg arg2_e]
        in "AssocLambda("^x^","^sub^")"

    | Apply            (fn_e,arg_e)         -> "Apply("^sub^")"
    | Map              (fn_e,ce)            -> "Map("^sub^")"
    | Flatten          ce                   -> "Flatten("^sub^")"
    | Aggregate        (fn_e,i_e,ce)        -> "Aggregate("^sub^")"
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> "GroupByAggregate("^sub^")"
    | SingletonPC      (id,t)               -> "SingletonPC("^id^")"
    | OutPC            (id,outs,t)          -> "OutPC("^id^")"
    | InPC             (id,ins,t)           -> "InPC("^id^")"
    | PC               (id,ins,outs,t)      -> "PC("^id^")"
    | Member           (me,ke)              -> "Member("^sub^")"  
    | Lookup           (me,ke)              -> "Lookup("^sub^")"
    | Slice            (me,sch,pat_ve)      -> "Slice("^sub^")"
    | PCUpdate         (me,ke,te)           -> "PCUpdate("^sub^")"
    | PCValueUpdate    (me,ine,oute,ve)     -> "PCValueUpdate("^sub^")"
    (*| External         efn_id               -> "External(...)" *)
    in fold_expr aux (fun x e -> None) None "" e

(* Native collection constructors *)
let collection_of_list (l : expr_t list) =
    if l = [] then failwith "invalid list for construction" else
    List.fold_left (fun acc v -> Combine(acc,Singleton(v)))
        (Singleton(List.hd l)) (List.tl l)

let collection_of_float_list (l : float list) =
    if l = [] then failwith "invalid list for construction" else
    List.fold_left (fun acc v -> Combine(acc,Singleton(Const(CFloat(v)))))
        (Singleton(Const(CFloat(List.hd l)))) (List.tl l)


(* Incremental section *)
type statement = expr_t * expr_t
type trigger = M3.pm_t * M3.rel_id_t * M3.var_t list * statement list
type program = M3.map_type_t list * M3Common.Patterns.pattern_map * trigger list

end
