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

    (* fold f g init e
     * applies f to each subexpr of e, using init as the accumulator at leaves,
     * and using g to combine accumulations at internal nodes
     *)
    val fold_expr :
        ('a -> expr_t -> 'a) -> ('a list -> 'a) -> 'a -> expr_t -> 'a

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


(* Arguments:
 * -- f, a folding function to be applied at every AST node
 * -- g, a combiner for states accumulated at an internal node
 * -- init, initial state at every leaf node
 * Notes: given a fold function f, this fold does not thread the accumulator
 * through every AST node. Rather it applies f with initial value at the leaves
 * and uses a second function g to combine accumulated state at internal nodes
 *)
let rec fold_expr (f : 'a -> expr_t -> 'a)
                  (g : 'a list -> 'a)
                  (init: 'a)
                  (e: expr_t) : 'a =
    let recur = fold_expr f g init in
    let sub l = g (List.map recur l) in
    begin match e with
    | Const            c                    -> f init e
    | Var              (id,t)               -> f init e
    | Tuple            e_l                  -> f (sub e_l) e
    | Project          (ce, idx)            -> f (recur ce) e
    | Singleton        ce                   -> f (recur ce) e
    | Combine          (ce1,ce2)            -> f (sub [ce1;ce2]) e
    | Add              (ce1,ce2)            -> f (sub [ce1;ce2]) e
    | Mult             (ce1,ce2)            -> f (sub [ce1;ce2]) e
    | Eq               (ce1,ce2)            -> f (sub [ce1;ce2]) e
    | Neq              (ce1,ce2)            -> f (sub [ce1;ce2]) e
    | Lt               (ce1,ce2)            -> f (sub [ce1;ce2]) e
    | Leq              (ce1,ce2)            -> f (sub [ce1;ce2]) e
    | IfThenElse0      (ce1,ce2)            -> f (sub [ce1;ce2]) e
    | IfThenElse       (pe,te,ee)           -> f (sub [pe;te;ee]) e
    | Block            e_l                  -> f (sub e_l) e
    | Iterate          (fn_e, ce)           -> f (sub [fn_e; ce]) e
    | Lambda           (arg_e,ce)           -> f (recur ce) e
    | AssocLambda      (arg1_e,arg2_e,be)   -> f (recur be) e
    | Apply            (fn_e,arg_e)         -> f (sub [fn_e;arg_e]) e
    | Map              (fn_e,ce)            -> f (sub [fn_e;ce]) e
    | Flatten          ce                   -> f (recur ce) e
    | Aggregate        (fn_e,i_e,ce)        -> f (sub [fn_e; i_e; ce]) e
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> f (sub [fn_e; i_e; ge; ce]) e
    | SingletonPC      (id,t)               -> f init e
    | OutPC            (id,outs,t)          -> f init e
    | InPC             (id,ins,t)           -> f init e
    | PC               (id,ins,outs,t)      -> f init e
    | Member           (me,ke)              -> f (sub (me::ke)) e  
    | Lookup           (me,ke)              -> f (sub (me::ke)) e
    | Slice            (me,sch,pat_ve)      -> f (sub (me::(List.map snd pat_ve))) e
    | PCUpdate         (me,ke,te)           -> f (sub ([me]@ke@[te])) e
    | PCValueUpdate    (me,ine,oute,ve)     -> f (sub ([me]@ine@oute@[ve])) e
    (*| External         efn_id               -> f init e *)
    end

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
    let aux sub e = match e with
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
    in fold_expr aux (String.concat ",") "" e

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
