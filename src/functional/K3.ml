(**
   A simple collection language (K3) admitting structural recursion 
   optimizations

  Notes:
    - K3 is a simple functional language with tuples, temporary and persistent
      collections, collection operators (iterate, map, flatten, aggregate,
      group-by aggregate), and persistence operations (get, update).
    - Here "persistent" means global for now, as needed by DBToaster triggers,
      rather than being long-lived over multiple program invocations. This
      suffices because DBToaster's continuous queries are long-running 
      themselves.
    - Maps are persistent collections, where the collection is a tuple of
      keys and value. 
    - For tuple collection accessors: {ul
       {- we assume a call-by-value semantics, thus for persistent collections,
          the collections are repeatedly retrieved from the persistent store,
          and our code explicitly avoids this by passing them as arguments to
          functions.}
       {- these can operate on either temporary or persistent collections.
          It is the responsibility of the code generator to produce the correct
          implementation on the actual datatype representing the collection.}
      }
    - For persistent collections based on SliceableMaps: {ul
       {- code generator should instantiate the collection datastructure used
          during evaluation, whether they are lists or ValuationMaps.}
       {- code generator should strip any secondary indexes as needed.}
       {- currently in the interpreter, during slicing we convert a persistent
          collection into a temporary one, from a SliceableMap to TupleList}
       {- updates should add any secondary indexes as needed}
      }
*) 

open Format


(* Metadata for code generation *)

(** Variable identifiers.  This is distinct from [Types.var_t] because K3 
    variables can have more complex types ([K3.type_t], rather than 
    [Types.type_t]) *)
type id_t = string

(** Collection identifiers *)
type coll_id_t = string

(**K3 Typing primitives.  This needs to be more extensive than the primitive
   types defined in the Types module.  We include those via TBase.

   Note that the collection type is used for both persistent and temporary 
   collections.  Maps are collections of tuples, where the tuple includes key 
   types and the value type. 
   
   Note also that the function type is compositional, and thus a little bit 
   ambiguous.  Specifically:
   [Fn([A, B], Fn([C], t))]
   is equivalent to
   [Fn([A, B, C], t)]
   which is also equivalent to
   [Fn([A], Fn([B, C], t))]
   
   Also note that Collections of Tuples are treated as HashMaps (specifically
   by the operations [Member], [Lookup], and [Slice]).  For collections of 
   n-field tuples, the first n-1 fields are the "key", and the remaining field
   is the "value".  Note that for arbitrary collections, uniqueness of keys is 
   not implicitly guaranteed.  Only Persistent Collections as defined below 
   guarantee uniqueness for the keys provided as arguments to [PCUpdate]/
   [PCValueUpdate].
 *)
type type_t =
    | TUnit                               (** Unit type; No data content *)
    | TBase      of Types.type_t          (** Primitive type, see Types *)
    | TTuple     of type_t list           (** Tuples of nested objects *)
    | Collection of type_t                (** Collections (typically bags) *)
    | Fn         of type_t list * type_t  (** Function type *)

let base_type_of t = begin match t with
	| TBase(bt) -> bt
	| _ -> failwith "Invalid argument: not a K3 base type!"
end

(** Schemas are carried along with persistent map references, and temporary 
    slices *)
type schema = (id_t * type_t) list

(** A function argument *)
type arg_t = 
   | AVar of id_t * type_t          (** The function accepts a single variable 
                                        of any type.  Within the scope of the 
                                        function the parameter is referenced by 
                                        the specified identifier *)
   | ATuple of (id_t * type_t) list (** The function accepts a single variable
                                        which must be of tuple type, with a 
                                        width and type corresponding to the 
                                        provided types.  Within the scope of the 
                                        function, the fields of the tuple can be 
                                        referenced individually by the specified 
                                        identifiers *)

(* External functions (Not implemented yet) *)
(*
type ext_fn_type_t = type_t list * type_t   (* arg, ret type *)

type fn_id_t = string
type ext_fn_id = Symbol of fn_id_t

type symbol_table = (fn_id_t, ext_fn_type_t) Hashtbl.t
let ext_fn_symbols : symbol_table = Hashtbl.create 100
*)

(** Expression AST *)
type expr_t =
   
   (* Terminals *)
     Const         of Types.const_t          (** 
         Constants of primitive types.
      *)
   | Var           of id_t        * type_t   (**
         Variable references.  
      *)
   

   (* Tuples, i.e. unnamed records *)
   | Tuple         of expr_t list            (**
         Tuple constructor
      *)
   | Project       of expr_t      * int list (**
         Tuple projection {b NOT IMPLEMENTED IN RUNTIMES}
      *)

   (* Collection construction *)
   | Singleton     of expr_t                 (**
         Constructor for singleton collections (collections with only one 
         element)
      *)
   | Combine       of expr_t      * expr_t   (**
         Combine two or more collections together {b NOT FULLY SUPPORTED BY 
         RUNTIMES}
      *)

   (* Arithmetic and comparison operators, conditionals *) 
   | Add           of expr_t      * expr_t   (** Add two primitives *)
   | Mult          of expr_t      * expr_t   (** Multiply two primitives *)
   | Eq            of expr_t      * expr_t   (** Equality test *)
   | Neq           of expr_t      * expr_t   (** Inequality test *)
   | Lt            of expr_t      * expr_t   (** Less than test *)
   | Leq           of expr_t      * expr_t   (** Greater than test *)
   | IfThenElse0   of expr_t      * expr_t   (** 
         If the first field is nonzero/true, evaluate and return the second
         field.  Otherwise return 0.
       *)

   (* Control flow: conditionals, sequences, side-effecting iterations *)
   | Comment       of string      * expr_t   (**
         Ignored syntax node, used primarilly for debugging
      *)
   | IfThenElse    of expr_t      * expr_t   * expr_t (** 
         If the first field is nonzero/true, evaluate and return the second
         field.  Otherwise evaluate and return the third field.
      *)
   | Block         of expr_t list            (**
         Evaluate each expression in the list in turn and return the return
         value of the last expression.  All other expressions must evaluate to
         TUnit
      *)
   | Iterate       of expr_t      * expr_t   (**
         Iterate over elements of the collection provided in the second field
         using the lambda function provided in the first field
      *)
     
   (* Functions *)
   | Lambda        of arg_t       * expr_t   (**
         Defines a single-argument function.  When the function is [Apply]ed, 
         the second field expression is evaluated with the first field's 
         variable(s) in scope, bound to the applied value, and the return value
         of the expression is returned from the [Apply].
      *)
   | AssocLambda   of arg_t       * arg_t    * expr_t (**
         Defines a two-argument function.  This is equivalent to
         [Lambda(arg1, Lambda(arg2, expr))].  Mostly here for utility, because
         Aggregate and GroupByAggregate expect the aggregate function to take
         two parameters.
      *)
   | ExternalLambda of id_t       * arg_t    * type_t (**
         Defines a single-argument function.  When the function is [Apply]ed, 
         the first field external function is evaluated with the seconds field's 
         variable(s) in scope, bound to the applied value, and the return value
         of the function is returned from the [Apply]. The type of the return
				 value is defined by the third field.
      *)
   | Apply         of expr_t      * expr_t (**
         Evaluate a [Lambda] function on a value, or curry the outermost 
         parameter of an [AssocLambda] function.  The first field must be a 
         function who's leftmost argument type is the type of the second field.  
         The function will be evaluated with the second field as its argument, 
         and the return value of the function will be returned by the [Apply].
      *)

   (* Structural recursion operators *)
   | Map              of expr_t      * expr_t (**
         Transform the collection provided in the second field, by applying the 
         function provided in the first field to every element of the 
         collection.
      *)
   | Flatten          of expr_t              (**
         Flatten a collection of collections into a single-level collection
      *)
   | Aggregate        of expr_t      * expr_t   * expr_t (**
         Fold the elements of the collection provided in the third field, by 
         applying the two-argument function provided in the first field, using 
         the second field as an initial value for the fold.  The first parameter
         of the provided function should be the tuple being folded, and the
         second is the current fold value.
      *)
   | GroupByAggregate of expr_t      * expr_t   * expr_t * expr_t (**
         Fold the elements of the collection provided in the fourth field, by
         applying the two-argument function provided in the first field, using
         the second field as an initial value for the fold.  Before folding, 
         elements of the collection are classified into groups labeled by the
         return value of the function provided in the third field.  If this
         return value is of tuple type, the return value of the aggregate is
         a collection of this tuple type extended by the fold value type.  If 
         the classifier's return value is not a tuple type, then the return 
         value of the aggregate is a collection of tuples of this type and the
         fold value's type
      *)

   (* Tuple collection accessors *)
   | Member      of expr_t      * expr_t list  (**
         Determine if values obtained by evaluating the expressions in the 
         second field are a key that has been defined in the tuple collection 
         provided in the first field.
      *)
   | Lookup      of expr_t      * expr_t list  (**
         Find the value corresponding to the key formed by evaluating the 
         expressions in the second field in the tuple collection provided in the 
         first field.  This operation will cause an error if the value is not
         already present in the collection.
      *)
   
   | Slice       of expr_t      * schema      * (id_t * expr_t) list (**
         Perform a partial key access on the tuple collection provided in the 
         first field.  Key fields provided in the third field are bound 
         according to the schema provided in the second field, and all unbound 
         keys are treated as wildcards.  All matching tuples in the collection 
         are returned.
      *)
   
   (* Persistent collection types w.r.t in/out vars *)
   | SingletonPC   of coll_id_t   * type_t (**
         A reference to a persistent global variable.  The value of this 
         variable may be changed by calling PCValueUpdate with no keys.  The 
         reference acts as the value itself otherwise.
      *)
   | OutPC         of coll_id_t   * schema   * type_t (**
         A reference to a persistent global collection optimized for slicing.  
         The value of this collection may be changed in two ways: The entire 
         collection may be replaced in bulk by calling PCUpdate with no keys or 
         individual elements of this collection may be updated by calling 
         PCValueUpdate with the "key" (as defined above under Tuple collection 
         operators) in the {b second} (output variable) field.
      *)
   | InPC          of coll_id_t   * schema   * type_t (** 
         A reference to a persistent global collection optimized for membership 
         testing. The value of this collection may be changed in two ways: The 
         entire collection may be replaced in bulk by calling PCUpdate with no 
         keys or individual elements of this collection may be updated by 
         calling PCValueUpdate with the "key" (as defined above under Tuple 
         collection operators) in the {b first} (input variable) field.
      *)
   | PC            of coll_id_t   * schema   * schema    * type_t (**
         A reference to a persistent global collection consisting of an OutPC 
         nested inside an InPC.  An entire OutPC may be replaced by calling 
         PCUpdate with the "key" (as defined above under Tuple collection 
         operators) if the InPC element to be replaced.  Individual values may 
         be replaced by calling PCValueUpdate with the "key"s of the InPC and 
         the OutPC respectively.
      *)

   (* map, key (optional, used for double-tiered), tier *)
   | PCUpdate      of expr_t      * expr_t list * expr_t (**
         Bulk replace the contents of an OutPC, InPC or the OutPC component of a 
         PC.  If the collection provided in the first field is an OutPC or InPC, 
         then the key provided in the second field must be empty and the entire 
         collection will be replaced by the collection provided in the third 
         field.  If the collection provided in the first field is a PC, then the 
         key in the second field is evaluated used to determine which InPC 
         element to set/replace.
      *)

   (* map, in key (optional), out key, value *)   
   | PCValueUpdate of expr_t      * expr_t list * expr_t list * expr_t (**
         Replace a single element of a SingletonPC, OutPC, InPC or PC provided 
         in the first field with the value resulting from evaluating the fourth 
         field.  The second field is used to index into an InPC or the InPC 
         component of a PC.  The third field is used to index into an OutPC or 
         the OutPC component of a PC.  If either field is not relevant, it must 
         be left blank.
      *)
   (* map, in key (optional), out key *) 
   | PCElementRemove of expr_t      * expr_t list * expr_t list (** 
         Removes a single element of an OutPC, InPC or PC.  inKey
         is used to index into an InPC or the InPC component of a PC.  outKey 
         is used to index into an OutPC or the OutPC component of a PC.  If
         either field is not relevant, it must be left blank.
      *)
   (* no input *) 
   | Unit (** 
         Unit operation. Does nothing, only returns value with unit type
      *)
    

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
		| ExternalLambda   (arg_e,fn_id,fn_t)   -> []
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
    | PCElementRemove  (me,ine,oute)        -> [[me];ine;oute]
    | Unit                                  -> []
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
    | ExternalLambda   (arg_e,fn_id,fn_t)   -> e
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
    | PCElementRemove  (me,ine,oute)        -> PCElementRemove(sfst(),snd(),thd())
    | Unit                                  -> e
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
    | ExternalLambda   (arg_e,fn_id,fn_t)   -> e
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
    | PCElementRemove  (me,ine,oute)        -> PCElementRemove(f me, List.map f ine, List.map f oute)
    | Unit                                  -> e
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
        | Types.CDate _ -> "CDate"
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
    
    | ExternalLambda      (fn_id,arg_e,fn_t)   ->
        ob(); ps "ExternalLambda(";
        pid fn_id; ps ","; ps (string_of_arg arg_e); 
        ps ","; ps (string_of_type fn_t); ps ")"; cb()
    
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
    | Unit                -> ob(); ps "Unit"; cb();

    (* Pretty-print with list branches *)
    | Block _             -> pop ~lb:[0] "Block"
    | Member _            -> pop ~lb:[1] "Member"  
    | Lookup _            -> pop ~lb:[1] "Lookup"
    | PCUpdate _          -> pop ~lb:[1] "PCUpdate"
    | PCValueUpdate   _   -> pop ~lb:[1;2] "PCValueUpdate"
    | PCElementRemove _   -> pop ~lb:[1;2] "PCElementRemove"
    (*| External         efn_id               -> pop "External" *)
    in pp_set_margin str_formatter 80; flush_str_formatter (aux e)

let string_of_exprs e_l = ListExtras.string_of_list string_of_expr e_l

let rec code_of_expr e =
   let rcr ex = "("^(code_of_expr ex)^")" in
   let rec ttostr t = "("^(match t with
      | TUnit -> "K3.TUnit"
      | TBase( b_t ) -> "K3.TBase(Types."^(Types.ocaml_of_type b_t)^")"
      | TTuple(tlist) -> 
         "K3.TTuple("^(ListExtras.ocaml_of_list ttostr tlist)^")"
      | Collection(subt) -> "K3.Collection("^(ttostr subt)^")"
      | Fn(argt,rett) -> 
         "K3.Fn("^(ListExtras.ocaml_of_list ttostr argt)^","^(ttostr rett)^")"
   )^")" in
   let string_of_vpair (v,vt) = "\""^v^"\","^(ttostr vt) in
   let vltostr = ListExtras.ocaml_of_list string_of_vpair in
   let argstr arg = 
      match arg with
         | AVar(v,vt) -> 
            "K3.AVar("^(string_of_vpair (v,vt))^")"
         | ATuple(vlist) -> 
            "K3.ATuple("^(vltostr vlist)^")"
   in
   match e with
      | Const c -> 
        let const_ts = match c with
          | Types.CFloat _ -> "CFloat"
          | Types.CString _ -> "CString" 
					| Types.CInt _ -> "CInt"
          | Types.CBool _ -> "CBool" 
          | Types.CDate _ -> "CDate"
        in "K3.Const(Types."^const_ts^"("^(Types.string_of_const c)^"))"
      | Var (id,t) -> "K3.Var(\""^id^"\","^(ttostr t)^")"
      | Tuple e_l -> "K3.Tuple("^(ListExtras.ocaml_of_list rcr e_l)^")"
      
      | Project (ce, idx) -> "K3.Project("^(rcr ce)^","^
                           (ListExtras.ocaml_of_list string_of_int idx)^")"
      
      | Singleton ce      -> "K3.Singleton("^(rcr ce)^")"
      | Combine (ce1,ce2) -> "K3.Combine("^(rcr ce1)^","^(rcr ce2)^")"
      | Add  (ce1,ce2)    -> "K3.Add("^(rcr ce1)^","^(rcr ce2)^")"
      | Mult (ce1,ce2)    -> "K3.Mult("^(rcr ce1)^","^(rcr ce2)^")"
      | Eq   (ce1,ce2)    -> "K3.Eq("^(rcr ce1)^","^(rcr ce2)^")"
      | Neq  (ce1,ce2)    -> "K3.Neq("^(rcr ce1)^","^(rcr ce2)^")"
      | Lt   (ce1,ce2)    -> "K3.Lt("^(rcr ce1)^","^(rcr ce2)^")"
      | Leq  (ce1,ce2)    -> "K3.Leq("^(rcr ce1)^","^(rcr ce2)^")"
      
      | IfThenElse0 (ce1,ce2)  -> 
            "K3.IfThenElse0("^(rcr ce1)^","^(rcr ce2)^")"
      | Comment(c, cexpr) ->
            "(*** "^c^" ***) "^(rcr cexpr)
      | IfThenElse  (pe,te,ee) -> 
            "K3.IfThenElse("^(rcr pe)^","^(rcr te)^","^(rcr ee)^")"
      
      | Block   e_l        -> "K3.Block("^(ListExtras.ocaml_of_list rcr e_l)^")"
      | Iterate (fn_e, ce) -> "K3.Iterate("^(rcr fn_e)^","^(rcr ce)^")"
      | Lambda  (arg_e,ce) -> "K3.Lambda("^(argstr arg_e)^","^(rcr ce)^")"
      
      | AssocLambda(arg1,arg2,be) ->
            "K3.AssocLambda("^(argstr arg1)^","^(argstr arg2)^","^
                               (rcr be)^")"
      | ExternalLambda(fn_id,arg,fn_t) ->
            "K3.ExternalLambda(\""^fn_id^"\","^(argstr arg)^","^(ttostr fn_t)^")"
      | Apply(fn_e,arg_e) -> 
            "K3.Apply("^(rcr fn_e)^","^(rcr arg_e)^")"
      | Map(fn_e,ce) -> 
            "K3.Map("^(rcr fn_e)^","^(rcr ce)^")"
      | Flatten(ce) -> 
            "K3.Flatten("^(rcr ce)^")"
      | Aggregate(fn_e,i_e,ce) -> 
            "K3.Aggregate("^(rcr fn_e)^","^(rcr i_e)^","^(rcr ce)^")"
      | GroupByAggregate(fn_e,i_e,ge,ce) -> 
            "K3.GroupByAggregate("^(rcr fn_e)^","^(rcr i_e)^","^(rcr ge)^
                                    ","^(rcr ce)^")"
      | SingletonPC(id,t) -> 
            "K3.SingletonPC(\""^id^"\","^(ttostr t)^")"
      | OutPC(id,outs,t) -> 
            "K3.OutPC(\""^id^"\","^(vltostr outs)^","^(ttostr t)^")"
      | InPC(id,ins,t) -> 
            "K3.InPC(\""^id^"\","^(vltostr ins)^","^(ttostr t)^")"
      | PC(id,ins,outs,t) -> 
            "K3.PC(\""^id^"\","^(vltostr ins)^","^(vltostr ins)^","^
                      (ttostr t)^")"
      | Member(me,ke) -> 
            "K3.Member("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^")"  
      | Lookup(me,ke) -> 
            "K3.Lookup("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^")"
      | Slice(me,sch,pat_ve) -> 
            let pat_str = ListExtras.ocaml_of_list (fun (id,expr) ->
               "\""^id^"\","^(rcr expr)
            ) pat_ve in
            "K3.Slice("^(rcr me)^","^(vltostr sch)^",("^pat_str^"))"
      | PCUpdate(me,ke,te) -> 
            "K3.PCUpdate("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^","^
                            (rcr te)^")"
      | PCValueUpdate(me,ine,oute,ve) -> 
            "K3.PCValueUpdate("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ine)^
                                 ","^(ListExtras.ocaml_of_list rcr oute)^","^
                                 (rcr ve)^")"
      | PCElementRemove(me,ine,oute) -> 
            "K3.PCElementRemove("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ine)^
                                ","^(ListExtras.ocaml_of_list rcr oute)^")"
      | Unit -> "K3.Unit"

let get_pc_schema pce maps = 
   let rec get_pc_name expression = match expression with
      | OutPC(id,outs,t) -> 
            id
      | InPC(id,ins,t) -> 
            id
      | PC(id,ins,outs,t) -> 
            id
      | Map(_, e) -> get_pc_name e
      | Slice(e, _, _) -> get_pc_name e
      | Block(el) -> 
            let rec last_item l = 
                if List.length l = 1 then
                    List.hd l
                else 
                    last_item (List.tl l)
            in
                get_pc_name (last_item el)
      | _ -> failwith "First element of Slice must be PC!"
   in
   let pc_name =  get_pc_name pce    
   in
      let (mapn, mapiv, mapov, mapt) = 
         List.hd (
            List.filter (fun (mapn, mapiv, mapov, mapt) -> mapn = pc_name) maps
         ) in
         List.map fst (mapiv@mapov)


let nice_string_of_expr ?(type_is_needed = false) e maps =
  let ob () = pp_open_hovbox str_formatter 2(*; pp_print_string str_formatter "{"*) in
  let cb () = (*pp_print_string str_formatter "}";*)pp_close_box str_formatter () in
  let pc () = pp_print_cut str_formatter () in
  let ps s = pp_print_string str_formatter s in
  let psp () = pp_print_space str_formatter () in
  let fnl () = pp_force_newline str_formatter () in
  let rec ttostr t = (match t with
      | TUnit -> "unit"
      | TBase( b_t ) -> begin match b_t with
            | Types.TInt -> "int"
            | Types.TFloat -> "float"
            | Types.TString -> "string"
            | _ -> "unknown!"
            end
      | TTuple(tlist) -> 
         "<"^(ListExtras.string_of_list ttostr tlist)^">"
      | Collection(subt) -> "Collection("^(ttostr subt)^")"
      | Fn(argt,rett) -> 
         "("^(ListExtras.string_of_list ttostr argt)^") -> "^(ttostr rett)^")"
   ) in
   let string_of_vpair (v,vt) = v^":"^(ttostr vt) in
   let vltostr = ListExtras.string_of_list string_of_vpair in
   let argstr arg = 
      match arg with
         | AVar(v,vt) -> 
            (string_of_vpair (v,vt))
         | ATuple(vlist) -> 
            "<"^(vltostr vlist)^">" in
  let is_single_statement e = 
    match e with
      | Const _ -> true
      | Var _ -> true
      | SingletonPC _ -> true
      | OutPC _ -> true
      | InPC _ -> true
      | PC _ -> true
      | _ -> false
  in
  let rec aux e =
    let print_list l delim =
      let list_size = List.length l in
      ignore( List.fold_left ( 
            fun cnt elem ->
               pc ();
               aux elem; 
               if cnt <> list_size-1 then
                  ps delim
               else 
                  ();
               cnt + 1
         ) 0 l
      )
    in
    let recur ?(delim = ",") list_branches = 
      let br = (get_branches e) in
      let nb = List.length br in
        ignore(List.fold_left
           (fun cnt l -> pc();
             if l = [] then ps "[]"
             else if List.mem cnt list_branches then
                (ps "["; print_list l ";"; ps "]") 
             else List.iter aux l;
             if cnt < (nb-1) then ps delim else (); cnt+1)
           0 br)
    in
    let pid id = ps (id) in
    let ppc pcid = ps ("$"^pcid) in
    let paroperand e1 = if is_single_statement e1 then aux e1 else begin ps "("; aux e1; ps ")" end in
    let par e1 e2 op = ob(); ps "("; paroperand e1; pc(); psp (); ps (op); psp(); pc(); paroperand e2; ps ")"; cb() in
    let pop ?(lb = []) s = ob(); ps s; ps "("; recur lb; ps ")"; cb() in 
    (*let ppar sname earg e1 = ob(); ps (sname^"("); aux earg; ps ")"; pc(); aux e1; cb() in*)
    let schema args = 
      "[" ^
      (String.concat ";"
        (List.map (fun (x,y) -> "\""^x^"\","^(string_of_type y)) args)) ^
      "]"
    in
    match e with
    | Const c -> 
      let const_ts = match c with
        | Types.CFloat _ -> "float"
        | Types.CString _ -> "string"
				| Types.CInt _ -> "int"
				| Types.CBool _ -> "bool" 
        | Types.CDate _ -> "CDate"
      in ob(); ps (Types.string_of_const c);
         if type_is_needed then ps (":"^(const_ts));
         cb()
    | Var (id,t) -> 
         ob(); pid id; 
         ps (":"^(ttostr t));
         cb()

    | SingletonPC(id,t) -> ob(); ppc id; cb()
    | OutPC(id,outs,t)  -> ob(); ppc id; cb()
    | InPC(id,ins,t)    -> ob(); ppc id; cb()
    | PC(id,ins,outs,t) -> ob(); ppc id; cb()

    | Project (_, idx) ->
        ob(); ps "Project("; recur []; ps ",";
        ps ("["^(String.concat "," (List.map string_of_int idx))^"]");
        ps")"; cb()

    | Lambda           (arg_e,ce)           ->
      ob(); ps "Lambda("; ps (argstr arg_e); ps ") "; pc(); ps "{"; pc(); 
      aux ce; pc(); ps "}"; cb()

    | AssocLambda      (arg1_e,arg2_e,be)   ->
        ob(); ps "Lambda(";
        ps (argstr arg1_e); ps ","; pc(); ps (argstr arg2_e); 
        ps ") ";  pc(); ps "{"; pc(); 
        aux be; pc(); ps "}"; cb()
    
    | ExternalLambda      (fn_id,arg_e,fn_t)   ->
        ob(); ps "ExternalLambda(";
        pid fn_id; ps ","; ps (string_of_arg arg_e); 
        ps ","; ps (string_of_type fn_t); ps ")"; cb()
    
    | Slice(pce, sch, vars) ->
        ob(); ps "Slice("; aux pce; 
        if type_is_needed then 
            begin
               ps ","; pc (); ps (schema sch) 
            end
        else 
            ();
        ps ",["; 
        let convert_each_var_to_new_var = 
           try 
              let orig_sch = get_pc_schema pce maps in
              (fun each_var -> List.nth orig_sch (ListExtras.index_of each_var (List.map fst sch)))
           with _ ->
             (fun each_var -> each_var)
        in
           let new_vars = List.map (fun (x, v) -> (convert_each_var_to_new_var x, v)) vars
               in
                  (List.iter (fun (x,v) -> pid x; ps " => ("; aux v; ps ");") new_vars); 
                  ps "])"; cb()
    | Comment(c, cexpr) ->
        ob(); ps "/**"; ps c; ps "**/"; fnl(); aux cexpr; cb()
    
    | Tuple _             -> ob(); ps "<"; recur ~delim:";" []; ps ">"; cb()
    | Singleton _         -> pop "Singleton"
    | Combine _           -> pop "Combine"
    | Add(e1, e2)         -> par e1 e2 "+"
    | Mult(e1, e2)        -> par e1 e2 "*"
    | Eq(e1, e2)          -> par e1 e2 "=="
    | Neq(e1, e2)         -> par e1 e2 "!="
    | Lt(e1, e2)          -> par e1 e2 "<"
    | Leq(e1, e2)         -> par e1 e2 "<="
    | IfThenElse0(c, ie)  -> ob(); ps "if0( "; aux c; ps " )"; fnl(); aux ie; cb();
    | IfThenElse(c,ie,ee) -> ob(); ps "if( "; aux c; ps " )"; fnl(); aux ie; cb(); fnl(); ob();  ps "else"; fnl(); aux ee; cb();
    | Iterate _           -> pop "Iterate"
    | Apply _             -> pop "Apply"
    | Map _               -> pop "Map"
    | Flatten _           -> pop "Flatten"
    | Aggregate _         -> pop "Aggregate"
    | GroupByAggregate _  -> pop "GroupByAggregate"
    | Block(expr_list)    -> ob(); ps "{"; ob();
                             List.iter (fun (expr) -> fnl(); aux expr; ps ";") expr_list;
                             cb(); cb(); fnl(); ps "}"
    | Unit                -> ob(); ps "()"; cb()

    (* Pretty-print with list branches *)
    | Member _            -> pop ~lb:[1] "Member"  
    | Lookup _            -> pop ~lb:[1] "Lookup"
    | PCUpdate _          -> pop ~lb:[1] "PCUpdate"
    | PCValueUpdate   _   -> pop ~lb:[1;2] "PCValueUpdate"
    | PCElementRemove _   -> pop ~lb:[1;2] "PCElementRemove"
    (*| External         efn_id               -> pop "External" *)
    in pp_set_margin str_formatter 80; flush_str_formatter (aux e)

let rec nice_code_of_expr e =
   let rcr ex = (nice_code_of_expr ex) in
   let rec ttostr t = (match t with
      | TUnit -> "unit"
      | TBase( b_t ) -> (Types.ocaml_of_type b_t)
      | TTuple(tlist) -> 
         "<"^(ListExtras.string_of_list ttostr tlist)^">"
      | Collection(subt) -> "Collection("^(ttostr subt)^")"
      | Fn(argt,rett) -> 
         "("^(ListExtras.string_of_list ttostr argt)^") -> "^(ttostr rett)^")"
   ) in
   let string_of_vpair (v,vt) = v^":"^(ttostr vt) in
   let vltostr = ListExtras.string_of_list string_of_vpair in
   let argstr arg = 
      match arg with
         | AVar(v,vt) -> 
            "("^(string_of_vpair (v,vt))^")"
         | ATuple(vlist) -> 
            "<"^(vltostr vlist)^">"
   in
   match e with
      | Const c -> Types.string_of_const c
      | Var (id,t) -> id
      | Tuple e_l -> "<"^(ListExtras.string_of_list rcr e_l)^">"
      
      | Project (ce, idx) -> "K3.SR.Project("^(rcr ce)^","^
                           (ListExtras.ocaml_of_list string_of_int idx)^")"
      
      | Singleton ce      -> "Singleton("^(rcr ce)^")"
      | Combine (ce1,ce2) -> "Combine("^(rcr ce1)^","^(rcr ce2)^")"
      | Add  (ce1,ce2)    -> "("^(rcr ce1)^"+"^(rcr ce2)^")"
      | Mult (ce1,ce2)    -> "("^(rcr ce1)^"*"^(rcr ce2)^")"
      | Eq   (ce1,ce2)    -> "("^(rcr ce1)^"=="^(rcr ce2)^")"
      | Neq  (ce1,ce2)    -> "("^(rcr ce1)^"!="^(rcr ce2)^")"
      | Lt   (ce1,ce2)    -> "("^(rcr ce1)^"<"^(rcr ce2)^")"
      | Leq  (ce1,ce2)    -> "("^(rcr ce1)^"<="^(rcr ce2)^")"
      
      | IfThenElse0 (ce1,ce2)  -> 
            "if0("^(rcr ce1)^")"^(rcr ce2)
      | Comment(c, cexpr) ->
            "(*** "^c^" ***) "^(rcr cexpr)
      | IfThenElse  (pe,te,ee) -> 
            "if("^(rcr pe)^")"^(rcr te)^"else"^(rcr ee)
      
      | Block   e_l        -> "{"^(ListExtras.string_of_list rcr e_l)^"}"
      | Iterate (fn_e, ce) -> "Iterate("^(rcr fn_e)^")"^(rcr ce)
      | Lambda  (arg_e,ce) -> "Lambda("^(argstr arg_e)^")"^(rcr ce)
      
      | AssocLambda(arg1,arg2,be) ->
            "AssocLambda("^(argstr arg1)^","^(argstr arg2)^")"^
                               (rcr be)
      | ExternalLambda(fn_id,arg,fn_t) ->
            "ExternalLambda(\""^fn_id^"\","^(argstr arg)^","^(ttostr fn_t)^")"
      | Apply(fn_e,arg_e) -> 
            "Apply("^(rcr fn_e)^","^(rcr arg_e)^")"
      | Map(fn_e,ce) -> 
            "Map("^(rcr fn_e)^","^(rcr ce)^")"
      | Flatten(ce) -> 
            "Flatten("^(rcr ce)^")"
      | Aggregate(fn_e,i_e,ce) -> 
            "Aggregate("^(rcr fn_e)^","^(rcr i_e)^","^(rcr ce)^")"
      | GroupByAggregate(fn_e,i_e,ge,ce) -> 
            "GroupByAggregate("^(rcr fn_e)^","^(rcr i_e)^","^(rcr ge)^
                                    ","^(rcr ce)^")"
      | SingletonPC(id,t) -> 
            "$"^id
      | OutPC(id,outs,t) -> 
            "$"^id
      | InPC(id,ins,t) -> 
            "$"^id
      | PC(id,ins,outs,t) -> 
            "$"^id
      | Member(me,ke) -> 
            "Member("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^")"  
      | Lookup(me,ke) -> 
            "Lookup("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^")"
      | Slice(me,sch,pat_ve) -> 
            let pat_str = ListExtras.ocaml_of_list (fun (id,expr) ->
               id^" => "^(rcr expr)
            ) pat_ve in
            "Slice("^(rcr me)^","^(vltostr sch)^",("^pat_str^"))"
      | PCUpdate(me,ke,te) -> 
            "PCUpdate("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^","^
                            (rcr te)^")"
      | PCValueUpdate(me,ine,oute,ve) -> 
            "PCValueUpdate("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ine)^
                                 ","^(ListExtras.ocaml_of_list rcr oute)^","^
                                 (rcr ve)^")"
      | PCElementRemove(me,ine,oute) -> 
            "PCElementRemove("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ine)^
                                ","^(ListExtras.ocaml_of_list rcr oute)^")"
      | Unit -> "()"

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
type map_t = string * (Types.var_t list) * (Types.var_t list) * Types.type_t
type schema_t = (map_t list * Patterns.pattern_map)
type statement_t = expr_t
type trigger_t = Schema.event_t * statement_t list
(** [Query name] x [Query expr] *)
type toplevel_query_t = string * expr_t
type prog_t = Schema.t * schema_t * trigger_t list * toplevel_query_t list

(* Returns all the persistent collections appearing in 'e' *)
let rec get_expr_map_schema (e: expr_t) : map_t list =
  let to_map_t (id,ins,outs,t) =
    (id, List.map (fun v -> (fst v),(base_type_of (snd v))) ins,
         List.map (fun v -> (fst v),(base_type_of (snd v))) outs,
        base_type_of t) 
  in
    begin match e with
    | Const            c                    -> []
    | Var              (id,t)               -> []
    | SingletonPC      (id,t)               -> [to_map_t (id,[],[],t)]
    | OutPC            (id,outs,t)          -> [to_map_t (id,[],outs,t)]
    | InPC             (id,ins,t)           -> [to_map_t (id,ins,[],t)]
    | PC               (id,ins,outs,t)      -> [to_map_t (id,ins,outs,t)]
    | _ -> List.flatten 
            (List.map get_expr_map_schema 
                (List.flatten (get_branches e)))
    end

let code_of_prog ((_,(maps,_),triggers,_):prog_t): string = (
   "--------------------- MAPS ----------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (mapn, mapiv, mapov, mapt) ->
      "DECLARE "^mapn^"("^(Types.string_of_type mapt)^")"^
      (ListExtras.ocaml_of_list Types.string_of_type (List.map snd mapiv))^
      (ListExtras.ocaml_of_list Types.string_of_type (List.map snd mapov))
   ) maps)^"\n\n"^
   "--------------------- TRIGGERS ----------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (event, stmts) ->
      "ON "^(Schema.string_of_event event)^" : {"^
      (ListExtras.string_of_list ~sep:"\n\t" string_of_expr stmts)^
      "}"
   ) triggers)^"\n"
)

let nice_code_of_prog ((db_schema,(maps,patts),triggers,tl_queries):prog_t): string = (
   let string_of_var_type (n, t) = n ^ " : " ^ (Types.string_of_type t) in
   "--------------------- SCHEMA ----------------------\n"^
   Schema.code_of_schema db_schema ^ "\n" ^
   "--------------------- MAPS ----------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (mapn, mapiv, mapov, mapt) ->
      mapn^"("^(Types.string_of_type mapt)^")"^
      "["^(ListExtras.string_of_list ~sep:"," string_of_var_type mapiv)^"]"^
      "["^(ListExtras.string_of_list ~sep:"," string_of_var_type mapov)^"];"
   ) maps)^"\n\n"^
   "-------------------- QUERIES --------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (qname,qdefn) ->
      "QUERY "^qname^" := "^(nice_string_of_expr qdefn maps)^";"
   ) tl_queries)^"\n\n"^
   "-------------------- PATTERNS --------------------\n"^
   (
      if Debug.active "USE-PATTERNS" then
         ""
      else
         "/*"
   )^(Patterns.patterns_to_nice_string patts)^"\n"^
   (
      if Debug.active "USE-PATTERNS" then
         ""
      else
         "*/\n"
   )^
   "--------------------- TRIGGERS ----------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (event, stmts) ->
      (Schema.string_of_event event)^"\n{\n"^
      (ListExtras.string_of_list ~sep:"\n" (fun e -> (nice_string_of_expr e maps)^";\n") stmts)^
      "\n}"
   ) triggers)^"\n"
)
