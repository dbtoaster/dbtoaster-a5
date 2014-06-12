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

(** Variable identifiers.  This is distinct from [Type.var_t] because K3 
    variables can have more complex types ([K3.type_t], rather than 
    [Type.type_t]) *)
type id_t = string

(** Collection identifiers *)
type coll_id_t = string


type collection_type_t = 
    | Unknown         (** The type of the collection is currently not known **)
    | Persistent      (** The collection is persistent **)
    | Intermediate    (** The collection is intermediate **)

(**K3 Typing primitives.  This needs to be more extensive than the primitive
   types defined in the Type module.  We include those via TBase.

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
    | TUnit                               (** Unit type; No data content   *)
    | TBase      of Type.type_t          (** Primitive type, see Type    *)
    | TTuple     of type_t list           (** Tuples of nested objects     *)
    | Collection of collection_type_t * 
                    type_t                (** Collections (typically bags) *)
    | Fn         of type_t list * type_t  (** Function type                *)

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

(** Expression AST *)
type expr_t =
   
   (* Terminals *)
     Const         of Constants.const_t          (** 
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
   | Combine       of expr_t  list   (**
         Combine two or more collections together
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
         Defines an external function. When the function is [Apply]ed, 
         the external function named by the first field is evaluated
         with argument's taken from the [Apply] context.
         The type of the return value is defined by the third field.
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
   (* filter function, map *)
   | Filter      of expr_t      * expr_t
   
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
   | LookupOrElse of expr_t * expr_t list * expr_t (**
         Collection, key, expression
      *)
   

(* Expression traversal helpers *)

let get_branches (e : expr_t) : expr_t list list =
    begin match e with
    | Const            c                    -> []
    | Var              (id,t)               -> []
    | Tuple            e_l                  -> List.map (fun e -> [e]) e_l
    | Project          (ce, idx)            -> [[ce]]
    | Singleton        ce                   -> [[ce]]
    | Combine          e_l                  -> List.map (fun e -> [e]) e_l
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
    | ExternalLambda   (fn_id,arg_e,fn_t)   -> []
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
    | Filter           (fn_e,ce)            -> [[fn_e];[ce]]
    | PCUpdate         (me,ke,te)           -> [[me];ke;[te]]
    | PCValueUpdate    (me,ine,oute,ve)     -> [[me];ine;oute;[ve]]
    | PCElementRemove  (me,ine,oute)        -> [[me];ine;oute]
    | Unit                                  -> []
    | LookupOrElse      (me,ke,te)          -> [[me];ke;[te]]
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
    | Combine          e_l                  -> Combine(List.flatten parts)
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
    | ExternalLambda   (fn_id,arg_e,fn_t)   -> e
    | Apply            (fn_e,arg_e)         -> Apply(sfst(),ssnd())
    | Map              (fn_e,ce)            -> Map(sfst(),ssnd())
    | Flatten          ce                   -> Flatten(sfst())
    | Aggregate        (fn_e,i_e,ce)        -> Aggregate(sfst(),ssnd(),sthd())
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> 
       GroupByAggregate(sfst(),ssnd(),sthd(),sfth())
    | SingletonPC      (id,t)               -> e
    | OutPC            (id,outs,t)          -> e
    | InPC             (id,ins,t)           -> e
    | PC               (id,ins,outs,t)      -> e
    | Member           (me,ke)              -> Member(sfst(),snd())  
    | Lookup           (me,ke)              -> Lookup(sfst(),snd())
    | Slice            (me,sch,pat_ve)      ->
        Slice(sfst(),sch,List.map2 (fun (id,_) e -> id,e) pat_ve (snd()))
    | Filter           (fn_e,ce)            -> Filter(sfst(),ssnd())
    | PCUpdate         (me,ke,te)           -> PCUpdate(sfst(), snd(), sthd())
    | PCValueUpdate    (me,ine,oute,ve)     -> 
       PCValueUpdate(sfst(),snd(),thd(),sfth())
    | PCElementRemove  (me,ine,oute)        -> 
       PCElementRemove(sfst(),snd(),thd())
    | Unit                                  -> e
    | LookupOrElse     (me,ke,te)           -> LookupOrElse(sfst(), snd(), sthd())
    end

(* Apply a function to all its children *)
let descend_expr (f : expr_t -> expr_t) e =
    begin match e with
    | Const            c                    -> e
    | Var              (id,t)               -> e
    | Tuple            e_l                  -> Tuple (List.map f e_l)
    | Project          (ce, idx)            -> Project (f ce, idx)
    | Singleton        ce                   -> Singleton (f ce)
    | Combine          e_l                  -> Combine (List.map f e_l)
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
    | AssocLambda      (arg1_e,arg2_e,be)   -> AssocLambda (arg1_e, arg2_e, 
                                                            f be)
    | ExternalLambda   (fn_id,arg_e,fn_t)   -> e
    | Apply            (fn_e,arg_e)         -> Apply (f fn_e, f arg_e)
    | Map              (fn_e,ce)            -> Map (f fn_e, f ce)
    | Flatten          ce                   -> Flatten (f ce)
    | Aggregate        (fn_e,i_e,ce)        -> Aggregate (f fn_e, f i_e, f ce)
    | GroupByAggregate (fn_e,i_e,ge,ce)     -> GroupByAggregate (f fn_e, f i_e,
                                                                 f ge, f ce)
    | SingletonPC      (id,t)               -> e
    | OutPC            (id,outs,t)          -> e
    | InPC             (id,ins,t)           -> e
    | PC               (id,ins,outs,t)      -> e
    | Member           (me,ke)              -> Member(f me, List.map f ke)  
    | Lookup           (me,ke)              -> Lookup(f me, List.map f ke)
    | Slice            (me,sch,pat_ve)      -> Slice(f me, sch, 
                                                     List.map (fun (id,e) ->
                                                         id, f e) pat_ve)
    | Filter           (fn_e,ce)            -> Filter(f fn_e, f ce)
    | PCUpdate         (me,ke,te)           -> PCUpdate(f me, List.map f ke, 
                                                        f te)
    | PCValueUpdate    (me,ine,oute,ve)     -> PCValueUpdate(f me, 
                                                             List.map f ine, 
                                                             List.map f oute, 
                                                             f ve)
    | PCElementRemove  (me,ine,oute)        -> PCElementRemove(f me, 
                                                               List.map f ine,
                                                               List.map f oute)
    | Unit                                  -> e
    | LookupOrElse     (me,ke,te)           -> LookupOrElse(f me, List.map f ke,
                                                        f te)
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
    | ExternalLambda   (id,arg_e,t)         -> app_f [[init]] e
    | SingletonPC      (id,t)               -> app_f [[init]] e
    | OutPC            (id,outs,t)          -> app_f [[init]] e
    | InPC             (id,ins,t)           -> app_f [[init]] e
    | PC               (id,ins,outs,t)      -> app_f [[init]] e
    | Unit                                  -> app_f [[init]] e
    | _ -> app_f (sub (get_branches e)) e
    end

(* Containment *)
let contains_expr e1 e2 =
  let contains_aux _ parts_contained e =
    (List.exists (fun x -> x) (List.flatten parts_contained)) || (e = e2)
  in fold_expr contains_aux (fun x _ -> x) None false e1

(* Argument helpers *)
let vars_of_arg a = match a with
  | AVar(v,_) -> [v]
  | ATuple(vt_l) -> List.map fst vt_l

let types_of_arg a = match a with
  | AVar(_,t) -> [t]
  | ATuple(vt_l) -> List.map snd vt_l

(* Stringification *)
let rec string_of_type t =
    match t with
      TUnit -> "TUnit" 
    | TBase(b_t) -> Type.string_of_type b_t
    | TTuple(t_l) -> "TTuple(" ^ 
                     (String.concat " ; " (List.map string_of_type t_l)) ^ ")"
    | Collection(k,c_t) ->
      "Collection("^(string_of_type c_t)^")"
    | Fn(a,b) -> "Fn("^(String.concat "," (List.map string_of_type a))^
                    ","^(string_of_type b)^")"

let string_of_arg a = match a with
    | AVar(v,v_t) -> "AVar(\""^v^"\","^(string_of_type v_t)^")"
    | ATuple(args) -> 
        let f = String.concat ";"
          (List.map (fun (x,y) -> "\""^x^"\","^(string_of_type y)) args)
        in "ATuple(["^f^"])"
    
let rec escalate_type t1 t2 =
   match (t1,t2) with 
    | (TUnit,TUnit) -> TUnit
    | (TBase(b1),TBase(b2)) -> TBase(Type.escalate_type b1 b2)
    | (TTuple(f1),TTuple(f2)) -> TTuple(List.map2 escalate_type f1 f2)
    | (Collection(a1,c1),
       Collection(a2,c2)) -> Collection(a1,escalate_type c1 c2)
    | (Fn(a1,r1), Fn(a2,r2)) -> Fn(List.map2 escalate_type a1 a2,
                                   escalate_type r1 r2)
    | (_, _) -> failwith ("Could not escalate "^(string_of_type t1)^", "^
                          (string_of_type t2))

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
      ob(); 
      ps ("K3.Const(" ^ (Constants.ocaml_of_const ~prefix:true c) ^ ")");
      cb()
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
    
    | ExternalLambda      (fn_id,fn_arg,fn_t)   ->
        ob(); ps "ExternalLambda(";
        pid fn_id; ps ","; ps (string_of_arg fn_arg); 
        ps ","; ps (string_of_type fn_t); ps ")"; cb()
    
    | Slice(pc, sch, vars) ->
        ob(); ps "Slice("; aux pc; ps ","; ps (schema sch); ps ",["; 
        (List.iter (fun (x,v) -> pid x; ps ",("; aux v; ps ");") vars); 
        ps "])"; cb()
    | Filter _            -> pop "Filter"
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
    | LookupOrElse _      -> pop ~lb:[1;2] "LookupOrElse"
    in pp_set_margin str_formatter 80; flush_str_formatter (aux e)

let string_of_exprs e_l = ListExtras.string_of_list string_of_expr e_l

let rec code_of_expr e =
   let rcr ex = "("^(code_of_expr ex)^")" in
   let rec ttostr t = "("^(match t with
      | TUnit -> "K3.TUnit"
      | TBase( b_t ) -> "K3.TBase(Type."^(Type.ocaml_of_type b_t)^")"
      | TTuple(tlist) -> 
         "K3.TTuple("^(ListExtras.ocaml_of_list ttostr tlist)^")"
      | Collection(_,subt) -> "K3.Collection("^(ttostr subt)^")"
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
         "K3.Const(" ^ (Constants.ocaml_of_const ~prefix:true c) ^ ")"
      | Var (id,t) -> "K3.Var(\""^id^"\","^(ttostr t)^")"
      | Tuple e_l -> "K3.Tuple("^
               (ListExtras.string_of_list ~sep:";" rcr e_l)^")"
      
      | Project (ce, idx) -> "K3.Project("^(rcr ce)^","^
               (ListExtras.ocaml_of_list string_of_int idx)^")"
      
      | Singleton ce      -> "K3.Singleton("^(rcr ce)^")"
      | Combine e_l       -> "K3.Combine("^
               (ListExtras.string_of_list ~sep:";" rcr e_l)^")"
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

      | ExternalLambda(fn_id,fn_arg,fn_t) ->
            "K3.ExternalLambda(\""^fn_id^"\","^
              (string_of_arg fn_arg)^","^(ttostr fn_t)^")"

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
      | Filter(fn_e,ce) -> 
            "K3.Filter("^(rcr fn_e)^","^(rcr ce)^")"
      | PCUpdate(me,ke,te) -> 
            "K3.PCUpdate("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^","^
                            (rcr te)^")"
      | PCValueUpdate(me,ine,oute,ve) -> 
            "K3.PCValueUpdate("^(rcr me)^","^
                              (ListExtras.ocaml_of_list rcr ine)^
                              ","^(ListExtras.ocaml_of_list rcr oute)^","^
                              (rcr ve)^")"
      | PCElementRemove(me,ine,oute) -> 
            "K3.PCElementRemove("^(rcr me)^","^
                                (ListExtras.ocaml_of_list rcr ine)^
                                ","^(ListExtras.ocaml_of_list rcr oute)^")"
      | Unit -> "K3.Unit"
      | LookupOrElse(me,ke,ve) -> 
            "K3.Lookup("^(rcr me)^","^(ListExtras.ocaml_of_list rcr ke)^","^
              (rcr ve)^")"

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
  let ob () = 
     pp_open_hovbox str_formatter 2 (*; pp_print_string str_formatter "{"*)
  in
  let cb () = 
     (*pp_print_string str_formatter "}";*)pp_close_box str_formatter () 
  in
  let pc () = pp_print_cut str_formatter () in
  let ps s = pp_print_string str_formatter s in
  let psp () = pp_print_space str_formatter () in
  let fnl () = pp_force_newline str_formatter () in
  let rec ttostr t = (match t with
      | TUnit -> "unit"
      | TBase( b_t ) -> Type.string_of_type b_t
      | TTuple(tlist) -> 
         "<"^(ListExtras.string_of_list ttostr tlist)^">"
      | Collection(_,subt) -> "Collection("^(ttostr subt)^")"
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
    let paroperand e1 = if is_single_statement e1 
                        then aux e1 
                        else begin ps "("; aux e1; ps ")" end 
    in
    let par e1 e2 op = ob(); ps "("; paroperand e1; 
                       pc(); psp (); ps (op); psp(); 
                       pc(); paroperand e2; ps ")"; cb() 
    in
    let pop ?(parens = ("(",")")) ?(delim=",") ?(lb = []) s = 
         ob(); ps s; ps (fst parens); recur ~delim:delim lb; 
         ps (snd parens); cb() in 
    let schema args = 
      "[" ^
      (String.concat ";"
        (List.map (fun (x,y) -> "\""^x^"\","^(string_of_type y)) args)) ^
      "]"
    in
    match e with
    | Const c -> 
      let const_ts = Type.string_of_type (Constants.type_of_const c) in
      ob(); 
      ps (Constants.string_of_const c);
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
    
    | ExternalLambda      (fn_id,fn_arg,fn_t)   ->
        ob(); ps "ExternalLambda(";
        pid fn_id; ps ","; ps (string_of_arg fn_arg); 
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
              (fun each_var -> List.nth orig_sch (
                 ListExtras.index_of each_var (List.map fst sch)))
           with _ ->
             (fun each_var -> each_var)
        in
           let new_vars = List.map (fun (x, v) -> 
              (convert_each_var_to_new_var x, v)) vars
           in
              (List.iter (fun (x,v) -> 
                  pid x; ps " => ("; aux v; ps ");"
               ) new_vars); 
              ps "])"; cb()
    | Filter _               -> pop "Filter"
    | Comment(c, cexpr) ->
        ob(); ps "/**"; ps c; ps "**/"; fnl(); aux cexpr; cb()
    
    | Tuple _             -> ob(); ps "<"; recur ~delim:";" []; ps ">"; cb()
    | Singleton _         -> pop "Singleton"
    | Combine _           -> pop ~parens:("{","}") ~delim:";" "Combine"
    | Add(e1, e2)         -> par e1 e2 "+"
    | Mult(e1, e2)        -> par e1 e2 "*"
    | Eq(e1, e2)          -> par e1 e2 "=="
    | Neq(e1, e2)         -> par e1 e2 "!="
    | Lt(e1, e2)          -> par e1 e2 "<"
    | Leq(e1, e2)         -> par e1 e2 "<="
    | IfThenElse0(c, ie)  -> ob(); ps "if0( "; aux c; ps " )"; 
                             fnl(); aux ie; cb();
    | IfThenElse(c,ie,ee) -> ob(); ps "if( "; aux c; ps " )"; 
                             fnl(); aux ie; cb(); fnl(); 
                             ob(); ps "else"; fnl(); aux ee; cb();
    | Iterate _           -> pop "Iterate"
    | Apply _             -> pop "Apply"
    | Map _               -> pop "Map"
    | Flatten _           -> pop "Flatten"
    | Aggregate _         -> pop "Aggregate"
    | GroupByAggregate _  -> pop "GroupByAggregate"
    | Block(expr_list)    -> ob(); ps "{"; ob();
                             List.iter (fun (expr) -> 
                                fnl(); aux expr; ps ";"
                             ) expr_list;
                             cb(); cb(); fnl(); ps "}"
    | Unit                -> ob(); ps "()"; cb()

    (* Pretty-print with list branches *)
    | Member _            -> pop ~lb:[1] "Member"  
    | Lookup _            -> pop ~lb:[1] "Lookup"
    | PCUpdate _          -> pop ~lb:[1] "PCUpdate"
    | PCValueUpdate   _   -> pop ~lb:[1;2] "PCValueUpdate"
    | PCElementRemove _   -> pop ~lb:[1;2] "PCElementRemove"
    | LookupOrElse _      -> pop ~lb:[1;2] "LookupOrElse"
    (*| External         efn_id               -> pop "External" *)
    in pp_set_margin str_formatter 80; flush_str_formatter (aux e)

let rec nice_code_of_expr e =
   let rcr ex = (nice_code_of_expr ex) in
   let rec ttostr t = (match t with
      | TUnit -> "unit"
      | TBase( b_t ) -> (Type.ocaml_of_type b_t)
      | TTuple(tlist) -> 
         "<"^(ListExtras.string_of_list ttostr tlist)^">"
      | Collection(_,subt) -> "Collection("^(ttostr subt)^")"
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
      | Const c -> Constants.string_of_const c
      | Var (id,t) -> id
      | Tuple e_l -> "<"^(ListExtras.string_of_list rcr e_l)^">"
      
      | Project (ce, idx) -> "K3.SR.Project("^(rcr ce)^","^
                           (ListExtras.ocaml_of_list string_of_int idx)^")"
      
      | Singleton ce      -> "Singleton("^(rcr ce)^")"
      | Combine e_l       -> "Combine { "^
                           (ListExtras.string_of_list ~sep:";" rcr e_l)^"}"
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
      | ExternalLambda(fn_id,fn_arg,fn_t) ->
            "ExternalLambda(\""^fn_id^"\","^
              (string_of_arg fn_arg)^","^(ttostr fn_t)^")"
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
      | Filter(fn_e,ce) -> 
            "Filter("^(rcr fn_e)^","^(rcr ce)^")"
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
      | LookupOrElse(me,ke,ve) -> "LookupOrElse("^(rcr me)^","^
            (ListExtras.ocaml_of_list rcr ke)^","^(rcr ve)^")"

(* Native collection constructors *)
let collection_of_list (l : expr_t list) =
    if l = [] then failwith "invalid list for construction" else
    Combine(List.map (fun v -> Singleton(v)) l)

let collection_of_float_list (l : float list) =
    if l = [] then failwith "invalid list for construction" else
    collection_of_list (List.map (fun v -> Const(Constants.CFloat(v))) l)


(* Incremental section *)
type map_t = string * (Type.var_t list) * (Type.var_t list) * Type.type_t
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
    | _ -> ListAsSet.multiunion 
            (List.map get_expr_map_schema 
                (List.flatten (get_branches e)))
    end

let code_of_prog ((_,(maps,_),triggers,_):prog_t): string = (
   "--------------------- MAPS ----------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (mapn, mapiv, mapov, mapt) ->
      "DECLARE "^mapn^"("^(Type.string_of_type mapt)^")"^
      (ListExtras.ocaml_of_list Type.string_of_type (List.map snd mapiv))^
      (ListExtras.ocaml_of_list Type.string_of_type (List.map snd mapov))
   ) maps)^"\n\n"^
   "--------------------- TRIGGERS ----------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (event, stmts) ->
      (Schema.string_of_event event)^" : {"^
      (ListExtras.string_of_list ~sep:"\n\t" string_of_expr stmts)^
      "}"
   ) triggers)^"\n"
)

let nice_code_of_prog ((db_schema, (maps,patts),
                        triggers, tl_queries):prog_t) : 
                      string = (
   let string_of_var_type (n, t) = n ^ " : " ^ (Type.string_of_type t) in
   "--------------------- SCHEMA ----------------------\n"^
   Schema.code_of_schema db_schema ^ "\n" ^
   "--------------------- MAPS ----------------------\n"^
   (ListExtras.string_of_list ~sep:"\n\n" (fun (mapn, mapiv, mapov, mapt) ->
      mapn^"("^(Type.string_of_type mapt)^")"^
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
      (Schema.string_of_event event) ^ "\n{\n" ^
      (ListExtras.string_of_list ~sep:"\n" 
         (fun e -> (nice_string_of_expr e maps) ^ ";\n") stmts) ^
      "\n}"
   ) triggers) ^ "\n"
)

module VarMap = Map.Make (String);;
exception AnnotationException of expr_t option * type_t option * string
(**
This function takes a K3 expression where the type of collections is unknown,
determines the type of those collections and returns the K3 expression with 
those types.

To determine the type of a collection, two mechanisms are used:
- The types of variables are stored in a hasmap, mapping variable names 
  to types
- If the type of arguments of a lambda function contains collections, 
  the type of those collections is determined by checking which type of 
  values are given when the function gets called
*)
let annotate_collections e =
  let bail ?(t = None) ?(exp = None) msg =
     raise (AnnotationException(exp, t, msg))
  in
  let rec _annotate_collections ?(argt = [TUnit]) e var_map = 
    let fnret f = 
      match f with
      | Fn(_, r) -> r 
      | _ -> bail ~t:(Some(f)) "Function expected"
    in
    let add_args args m =
      match args with
      | AVar(id, t) -> VarMap.add id t m
      | ATuple(ls) -> List.fold_left (fun a (id, t) -> VarMap.add id t a) m ls
    in
    let rec transfer_annotation t1 t2 = 
      match t1, t2 with 
      | Collection(t1ct, t1t), Collection(t2ct, t2t) -> 
         Collection(t2ct, transfer_annotation t1t t2t)
      | TTuple(t1l), TTuple(t2l) -> 
         TTuple(List.map2 (fun x1 x2 -> transfer_annotation x1 x2) t1l t2l)
      | t1, t2 -> t1
    in
    let replace_arg_types args ts = 
      match args, ts with
      | AVar(id, at), rt -> AVar(id, transfer_annotation at rt)
      | ATuple(l), TTuple(ts) -> 
          ATuple(List.map2 (fun (id, at) rt -> 
                    (id, transfer_annotation at rt)
                ) l ts)
      | _, t -> bail ~t:(Some(t)) ("Expected Tuple for "^(string_of_arg args))
    in
    let type_of_schema t = List.map (fun (i, t) -> t) t in
    let type_of_collection t = 
      match t with
      | Collection(_, tp) -> tp
      | _ -> bail ~t:(Some(t)) ("Collection expected but " ^ 
                                (string_of_type t) ^ " found in " ^ 
                                (string_of_expr e))
    in
    let key_val t =
      match t with
      | TTuple(ts) -> (
        let rec _key_val l k = 
        match l with 
          | [x] -> (k, x)
          | x :: xs -> _key_val xs (x :: k)
          | [] -> ([], TUnit)
        in 
          _key_val ts [])
      | _ -> ([], t)
    in
    let rec unzip l = 
      match l with 
      | (a, b) :: xs -> let a_uz, b_uz = unzip xs in (a :: a_uz, b:: b_uz)
      | [] -> ([], [])
    in
    let annotate_list l = 
      unzip (List.map (fun x -> _annotate_collections x var_map) l)
    in
    let type_of_op t1 t2 = TBase(
      match (t1, t2) with
      | (TBase(Type.TBool), TBase(Type.TFloat))
      | (TBase(Type.TFloat), TBase(Type.TBool))
      | (TBase(Type.TFloat), TBase(Type.TFloat)) -> Type.TFloat
      | (TBase(Type.TBool), TBase(Type.TInt))
      | (TBase(Type.TInt), TBase(Type.TBool))
      | (TBase(Type.TInt), TBase(Type.TInt)) -> Type.TInt
      | (TBase(Type.TFloat), TBase(Type.TInt)) -> Type.TFloat
      | (TBase(Type.TInt), TBase(Type.TFloat)) -> Type.TFloat
      (* NOTE: calculations with two bools result in 
         an int (e.g. true + true) *)
      | (TBase(Type.TBool), TBase(Type.TBool)) -> Type.TInt
      | (TBase(Type.TDate), TBase(Type.TDate)) -> Type.TDate
      | _ -> bail ("Failed to match " ^ (string_of_type t1) ^ 
                       " and " ^ (string_of_type t2))
    )
    in
    match e with
    | Const c -> (e, TBase(Constants.type_of_const c))
    | Var (id,t) -> 
      if VarMap.mem id var_map then
        let tp = VarMap.find id var_map in
        (Var(id, tp), transfer_annotation t tp)
      else
        (Var(id, t), t)
    | Tuple e_l -> 
      let els, tps = annotate_list e_l in 
         (Tuple(els), TTuple(tps))
    | Project (ce, idx) -> bail "not implemented" (* TODO: implement! *)
    | Singleton ce      -> 
      let ace, act = _annotate_collections ce var_map in
         (Singleton(ace), Collection(Intermediate, act))
    | Combine e_l ->
      let ce1_e, ce1_t = _annotate_collections (List.hd e_l) var_map in
      let cerest_e = List.map (fun x -> fst (_annotate_collections x var_map))
                              (List.tl e_l)
      in
         (Combine(ce1_e :: cerest_e), ce1_t)
    | Add  (ce1,ce2)    -> 
      let ce1_e, ce1_t = _annotate_collections ce1 var_map in
      let ce2_e, ce2_t = _annotate_collections ce2 var_map in
         (Add(ce1_e, ce2_e), type_of_op ce1_t ce2_t)
    | Mult (ce1,ce2)    -> 
      let ce1_e, ce1_t = _annotate_collections ce1 var_map in
      let ce2_e, ce2_t = _annotate_collections ce2 var_map in
         (Mult(ce1_e, ce2_e), type_of_op ce1_t ce2_t)
    | Eq   (ce1,ce2)    -> 
      let ce1_e, _ = _annotate_collections ce1 var_map in
      let ce2_e, _ = _annotate_collections ce2 var_map in
         (Eq(ce1_e, ce2_e), TBase(Type.TBool))
    | Neq  (ce1,ce2)    ->
      let ce1_e, _ = _annotate_collections ce1 var_map in
      let ce2_e, _ = _annotate_collections ce2 var_map in
         (Neq(ce1_e, ce2_e), TBase(Type.TBool))
    | Lt   (ce1,ce2)    ->       
      let ce1_e, _ = _annotate_collections ce1 var_map in
      let ce2_e, _ = _annotate_collections ce2 var_map in
         (Lt(ce1_e, ce2_e), TBase(Type.TBool))
    | Leq  (ce1,ce2)    ->       
      let ce1_e, _ = _annotate_collections ce1 var_map in
      let ce2_e, _ = _annotate_collections ce2 var_map in
         (Leq(ce1_e, ce2_e), TBase(Type.TBool))
    | IfThenElse0 (ce1,ce2)  -> 
      let ce1e, _ = _annotate_collections ~argt:argt ce1 var_map in
      let ce2e, ce2t = _annotate_collections ~argt:argt ce2 var_map in
         (IfThenElse0(ce1e, ce2e), ce2t)
    | Comment(c, cexpr) ->
      let ce, ct = _annotate_collections ~argt:argt cexpr var_map in
         (Comment(c, ce), ct)
    | IfThenElse  (pe,te,ee) -> 
      let pe_e, _    = _annotate_collections ~argt:argt pe var_map in
      let te_e, te_t = _annotate_collections ~argt:argt te var_map in
      let ee_e, _    = _annotate_collections ~argt:argt ee var_map in
         (IfThenElse(pe_e, te_e, ee_e), te_t)
    | Block   e_l        -> 
      let le, lt = annotate_list e_l in
         (Block(le), List.hd (List.rev lt))
    | Iterate (fn_e, c) ->
      let ce, ct = _annotate_collections c var_map in
      let fe, ft = 
        match ct with
        | Collection(_, argt) -> _annotate_collections 
                                        ~argt:[argt] fn_e var_map
        | _ -> bail ~t:(Some(ct)) "Expected collection"
      in
         (Iterate(fe, ce), TUnit)
    | Lambda  (arg_e,ce) -> 
      let arg1t = List.hd argt in
      let newargs = replace_arg_types arg_e arg1t in
      let c_e, c_t = _annotate_collections ce (add_args newargs var_map) in
         (Lambda(newargs, c_e), Fn(argt, c_t))
    | AssocLambda(arg1,arg2,be) ->
      let arg1t = List.hd argt in
      let arg2t = List.nth argt 1 in
      let newargs1 = replace_arg_types arg1 arg1t in
      let newargs2 = replace_arg_types arg2 arg2t in
      let newvar_map = add_args newargs1 (add_args newargs2 var_map) in
      let c_e, c_t = _annotate_collections be newvar_map in
         (AssocLambda(newargs1, newargs2, c_e), Fn(argt, c_t))
    | ExternalLambda(fn_id,arg,fn_t) ->
       (e, Fn(argt, fn_t))
    | Apply(fn_e,arg_e) -> 
      let ae, at = _annotate_collections arg_e var_map in
      let fe, ft = _annotate_collections ~argt:[at] fn_e var_map in
         (Apply(fe, ae), fnret ft)
    | Map(fn_e,ce) -> 
      let c_e, c_t = _annotate_collections ce var_map in
      let f_e, f_t = _annotate_collections 
                            ~argt:[type_of_collection c_t] 
                            fn_e var_map 
      in
         (Map(f_e, c_e), Collection(Intermediate, fnret f_t))
    | Flatten(ce) -> 
      let c_e, c_t = _annotate_collections ce var_map in
      (match c_t with
        | Collection(_, elems) -> (
          let k, v = key_val elems in 
          match v with
            | Collection(_, TTuple(t)) -> 
               (Flatten(c_e), Collection(Intermediate, TTuple(t @ k)))
            | Collection(_, t) -> 
               (Flatten(c_e), Collection(Intermediate, TTuple(t :: k)))
            | _ -> bail ~t:(Some(v)) "Expected nested collection")
        | _ -> bail "Expected collection"
      )
    | Aggregate(fn_e,ie,ce) -> 
      let c_e, c_t = _annotate_collections ce var_map in
      let i_e, i_t = _annotate_collections ie var_map in
      let f_e, f_t = _annotate_collections 
                        ~argt:((type_of_collection c_t) :: [i_t]) 
                        fn_e var_map 
      in
         (Aggregate(f_e, i_e, c_e), i_t)
    | GroupByAggregate(fn_e,ie,ge,ce) -> 
      let c_e, c_t = _annotate_collections ce var_map in
      let g_e, g_t = _annotate_collections 
                        ~argt:[type_of_collection c_t] 
                        ge var_map 
      in
      let i_e, i_t = _annotate_collections ie var_map in
      let f_e, f_t = _annotate_collections 
                        ~argt:((type_of_collection c_t) :: [i_t]) 
                        fn_e var_map 
      in
      let c_tpe = 
        match fnret g_t with
          | TTuple(ts) -> TTuple(ts @ [i_t])
          | t -> TTuple(t :: [i_t])
      in
      (GroupByAggregate(f_e, i_e, g_e, c_e), Collection(Intermediate, c_tpe))
    | SingletonPC(id,t) -> (e, t)
    | InPC(id,ins,t) -> (e, Collection(Persistent, 
                                       TTuple((type_of_schema ins) @ [t])))
    | OutPC(id,outs,t) -> (e, Collection(Persistent, 
                                         TTuple((type_of_schema outs) @ [t])))
    | PC(id,ins,outs,t) -> 
      (e, Collection(Persistent, TTuple((type_of_schema ins) @ 
        [Collection(Persistent, TTuple((type_of_schema outs) @ [t]))])))
    | Member(me,ke) -> 
      (Member(fst (_annotate_collections me var_map), 
              fst (annotate_list ke)), TBase(Type.TBool))
    | Lookup(me,ke) -> 
      let ce, ct = _annotate_collections me var_map in
         (Lookup(ce, fst (annotate_list ke)), 
          snd (key_val (type_of_collection ct)))
    | Slice(me,sch,pat_ve) -> 
      let m_e, m_t = _annotate_collections me var_map in
      let pats = 
        List.map (fun (id, exp) -> 
           (id, fst (_annotate_collections exp var_map))) pat_ve 
      in
         (Slice(m_e, sch, pats), 
          Collection(Intermediate, type_of_collection m_t))
    | Filter(fn_e,ce) -> 
      let c_e, c_t = _annotate_collections ce var_map in
      let f_e, f_t = _annotate_collections 
                            ~argt:[type_of_collection c_t] 
                            fn_e var_map 
      in
        (Filter(f_e, c_e), 
         Collection(Intermediate, type_of_collection c_t))
    | PCUpdate(me,ke,te) -> 
      let m_e, _ = _annotate_collections me var_map in
      let k_e, _ = annotate_list ke in
      let t_e, _ = _annotate_collections te var_map in
         (PCUpdate(m_e, k_e, t_e), TUnit)
    | PCValueUpdate(me,ine,oute,ve) -> 
      let m_e, _ = _annotate_collections me var_map in
      let ine_e, _ = annotate_list ine in
      let oute_e, _ = annotate_list oute in
      let ve_e, _ = _annotate_collections ve var_map in
         (PCValueUpdate(m_e, ine_e, oute_e, ve_e), TUnit)
    | PCElementRemove(me,ine,oute) -> 
      let m_e, _ = _annotate_collections me var_map in
      let ine_e, _ = annotate_list ine in
      let oute_e, _ = annotate_list oute in
         (PCElementRemove(m_e, ine_e, oute_e), TUnit)
    | Unit -> (e, TUnit)
    | LookupOrElse(me,ke,ve) -> 
      let ce, ct = _annotate_collections me var_map in
      (LookupOrElse(ce, fst (annotate_list ke), fst
      (_annotate_collections ve var_map)), snd (key_val (type_of_collection ct)))
  in
  if Debug.active "NO-COLLECTION-ANNOTATION" 
  then e 
  else fst (_annotate_collections e VarMap.empty)

exception ExtractionException of expr_t option * type_t option * string
let rec extract_patterns_from_k3 e : Patterns.pattern_map =
   let bail ?(t = None) ?(exp = None) msg =
      raise (ExtractionException(exp, t, msg))
   in
   let extract_patterns_from_expr_list e_l : Patterns.pattern_map =
      List.fold_left (fun pmap expr -> Patterns.merge_pattern_maps 
                                          (extract_patterns_from_k3 expr) pmap) 
         (Patterns.empty_pattern_map()) e_l
   in
   match e with
   | Tuple e_l -> extract_patterns_from_expr_list e_l
   | Project (ce, idx) -> bail "not implemented" (* TODO: implement! *)
   | Singleton e -> extract_patterns_from_k3 e
   | Combine e_l -> extract_patterns_from_expr_list e_l
   | Add  (e1,e2)
   | Mult (e1,e2)
   | Eq   (e1,e2)
   | Neq  (e1,e2)
   | Lt   (e1,e2)
   | Leq  (e1,e2)
   | IfThenElse0 (e1,e2) -> extract_patterns_from_expr_list [e1; e2]
   | Comment(_, e) -> extract_patterns_from_k3 e
   | IfThenElse  (e1,e2,e3) -> extract_patterns_from_expr_list [e1; e2; e3]
   | Block e_l -> extract_patterns_from_expr_list e_l
   | Iterate (e1,e2) -> extract_patterns_from_expr_list [e1; e2]
   | Lambda  (_,e)
   | AssocLambda(_,_,e) -> extract_patterns_from_k3 e
   | Apply(e1,e2) 
   | Map(e1,e2) -> extract_patterns_from_expr_list [e1; e2]
   | Flatten(e) -> extract_patterns_from_k3 e
   | Aggregate(e1,e2,e3) -> extract_patterns_from_expr_list [e1; e2; e3]
   | Member(e,e_l) -> extract_patterns_from_expr_list (e :: e_l)
   | Lookup(e,e_l) -> extract_patterns_from_expr_list (e :: e_l)
   | Slice(OutPC(id, _, _), schema, pattern) ->
      let ids, exprs = List.split pattern in
      let schema_ids, _ = List.split schema in 
      Patterns.merge_pattern_maps 
         (extract_patterns_from_expr_list exprs) 
            (Patterns.singleton_pattern_map 
               (id, Patterns.make_out_pattern schema_ids ids))
   | PCUpdate(e1,e_l,e2) -> extract_patterns_from_expr_list (e1 :: e2 :: e_l)
   | PCValueUpdate(e1,e_l1,e_l2,e2) -> 
      extract_patterns_from_expr_list (e1 :: e2 :: e_l1 @ e_l2)
   | PCElementRemove(e,e_l1,e_l2) -> 
      extract_patterns_from_expr_list (e :: e_l1 @ e_l2)
   | LookupOrElse(e1,e_l,e2) -> 
      extract_patterns_from_expr_list (e1 :: e2 :: e_l)
   | _ -> Patterns.empty_pattern_map()

let mk_var_name = 
   FreshVariable.declare_class "functional/K3" "unique_vars" 

exception NotImplementedException of expr_t option * type_t option * string

(**
   [unique_vars_expr ~renamings(...) e]
   
   Takes an expression and changes variable names such that they are unique
   in that expression. This helps to avoid name clashes
   when transforming a program (e.g. to an imperative representation)

   @param renamings (optional) Variables in scope with their renamings
   @param e         A K3 expression
   @return          The K3 expression with unique variables
*)
let unique_vars_expr ?(renamings = VarMap.empty) e : expr_t = 
   let rec _unique_vars e renamings : expr_t =
      let bail ?(t = None) ?(exp = None) msg =
         raise (NotImplementedException(exp, t, msg))
      in
      (* mk_unique_args generates a unique name for each argument and
         adds the renaming to the map of renamings *)
      let mk_unique_args renamings args = 
         let modify_renamings renamings id =
            let fresh_var = mk_var_name ~inline:("_" ^ id) () in
            (VarMap.add id fresh_var renamings, fresh_var) 
         in
         match args with
         | AVar (id, t) -> 
            let n_renamings, n_id = modify_renamings renamings id in
            (n_renamings, AVar (n_id, t))
         | ATuple argl ->
            let n_renamings, n_argl = 
               List.fold_left (fun (acc, args) (id, tp) ->
                  let n_renamings, n_id = modify_renamings acc id in
                  (n_renamings, args @ [n_id, tp])
               ) (renamings, []) argl
            in 
            (n_renamings, ATuple n_argl)
      in
      let rcr e = _unique_vars e renamings in
      let rcrl e_l = List.map rcr e_l in
      match e with
      | Const c -> e
      | Var (id, t) -> 
         Var (VarMap.find id renamings, t)
      | Tuple e_l -> Tuple (rcrl e_l)
      | Project (ce, idx) -> bail "not implemented" (* TODO: implement! *)
      | Singleton ce -> Singleton (rcr ce) 
      | Combine e_l -> Combine (rcrl e_l)
      | Add (ce1, ce2) -> Add (rcr ce1, rcr ce2)
      | Mult (ce1, ce2) -> Mult (rcr ce1, rcr ce2)
      | Eq (ce1, ce2) -> Eq (rcr ce1, rcr ce2)
      | Neq (ce1, ce2) -> Neq (rcr ce1, rcr ce2)
      | Lt (ce1, ce2) -> Lt (rcr ce1, rcr ce2)
      | Leq (ce1, ce2) -> Leq (rcr ce1, rcr ce2)
      | IfThenElse0 (ce1, ce2) -> IfThenElse0 (rcr ce1, rcr ce2)
      | Comment (c, cexpr) -> Comment (c, rcr cexpr)
      | IfThenElse (pe, te, ee) -> IfThenElse (rcr pe, rcr te, rcr ee)
      | Block e_l -> Block (List.map rcr e_l)
      | Iterate (fn_e, c) -> Iterate (rcr fn_e, rcr c)
      | Lambda  (arg_e, ce) -> 
         let n_renamings, n_arg_e = mk_unique_args renamings arg_e in
         let n_ce = _unique_vars ce n_renamings in
         Lambda (n_arg_e, n_ce)
      | AssocLambda (arg1, arg2, ce) -> 
         let n_renamings, n_arg1 = mk_unique_args renamings arg1 in
         let n_renamings, n_arg2 = mk_unique_args n_renamings arg2 in
         let n_ce = _unique_vars ce n_renamings in
         AssocLambda (n_arg1, n_arg2, n_ce)
      | ExternalLambda (fn_id, arg, fn_t) -> e
      | Apply (fn_e, arg_e) -> Apply (rcr fn_e, rcr arg_e)
      | Map (fn_e, ce) -> Map (rcr fn_e, rcr ce)
      | Flatten (ce) -> Flatten (rcr ce)
      | Aggregate (fn_e, ie, ce) -> Aggregate (rcr fn_e, rcr ie, rcr ce)
      | GroupByAggregate(fn_e, ie, ge, ce) ->
         GroupByAggregate(rcr fn_e, rcr ie, rcr ge, rcr ce)
      | SingletonPC _
      | InPC _
      | OutPC _
      | PC _ -> e
      | Member (me, ke) -> Member (rcr me, rcrl ke)
      | Lookup (me, ke) -> Lookup (rcr me, rcrl ke)
      | Slice (me, sch, pat_ve) -> 
         let ids, e_l = List.split pat_ve in
         Slice (rcr me, sch, List.combine ids (rcrl e_l))
      | Filter (fn_e, ce) -> Filter (rcr fn_e, rcr ce)
      | PCUpdate (me, ke, te) -> PCUpdate (rcr me, rcrl ke, rcr te)
      | PCValueUpdate (me, ine, oute, ve) -> 
         PCValueUpdate (rcr me, rcrl ine, rcrl oute, rcr ve)
      | PCElementRemove (me, ine, oute) -> 
         PCElementRemove (rcr me, rcrl ine, rcrl oute)
      | Unit -> e
      | LookupOrElse (me, ke, ve) -> LookupOrElse (rcr me, rcrl ke, rcr ve)
   in
   _unique_vars e renamings

(**
   [unique_vars_prog prog]
   
   Takes a program and changes variable names such that they are unique
   in the trigger that they belong to. This helps to avoid name clashes
   when transforming a program (e.g. to an imperative representation)

   @param prog    A K3 program.
   @return        The K3 program with unique variables.
*)
let unique_vars_prog (prog:prog_t) : prog_t =
   let db_schema, (maps,patts), triggers, tl_queries = prog in
   let n_tl_queries = 
      List.map (fun (n, e) -> (n, unique_vars_expr e)) tl_queries 
   in
   let n_triggers = 
      List.map (fun (t, stmts) -> (t, 
            let vars = 
               match t with
               | Schema.InsertEvent (_, vs, _)
               | Schema.DeleteEvent (_, vs, _) -> vs
               | Schema.BatchUpdate _ -> 
                    failwith "K3 does not support batch updates yet"
               | Schema.CorrectiveUpdate (_, vs1, vs2, v, _) -> 
                  v :: vs1 @ vs2
               | Schema.SystemInitializedEvent -> []
            in
            let ids, _ = List.split vars in
            let renamings = List.fold_left (fun acc x -> 
                                             VarMap.add x x acc) 
                                           VarMap.empty ids 
            in
            List.map (fun stmt -> 
                        unique_vars_expr ~renamings:renamings stmt) 
                     stmts
         )
      ) 
      triggers
   in
   (db_schema, (maps,patts), n_triggers, n_tl_queries)
