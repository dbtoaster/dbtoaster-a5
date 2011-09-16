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
open Format

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
   
	   (********** Terminals **********)
	     Const         of M3.const_t
         (* Const(m3Const)
            A constant
            returns: 
               [typeof(m3Const) (= TFloat | TInt)] *)
	   | Var           of id_t        * type_t
        (* Var(varID, varType)
           A reference to a variable
           returns: 
              [varType] *)
	
	   (* Tuples, i.e. unnamed records *)
	   | Tuple         of expr_t list
         (* Tuple([tupleContents])
            A tuple consisting of tupleContents.  
            Notes: 
               *  All K3 instructions designed to operate on tuples (e.g., Map) 
                  will treat non-tuple types as a tuple of size 1.
               *  Tuples nested in collections are special.  Query operators 
                  that operate over tuples nested in collections ignore the last
                  field of the tuple.  For example: When performing a Lookup on
                  a collection consisting of a <float,float> tuple, you only 
                  provide 1 float.  The return value of the Lookup is the last
                  field of the tuple.
            returns:
               [TTuple[for(e in tupleContents){ typeof(e) }]]*)
	   | Project       of expr_t      * int list
        (* Project(aTuple, [indices])
           restriction: typeof(aTuple) = TTuple[*]
           Pick out the elements of aTuple at the indicated indices
           returns:
              if sizeof(indices) = 1 then 
                 [typeof(indices[0]th element of aTuple)]
              else
                 [TTuple[for(n in indices){ typeof(nth element of aTuple) }]]
        *)
	
	   (* Collection construction *)
	   | Singleton     of expr_t
        (* Singleton(collectionElement)
           Create a 1-element collection defined by collectionElement
           returns: 
              [Collection[typeof(collectionElement)]] *)
	   | Combine       of expr_t      * expr_t 
	     (* Combine(col1,col2)
	        restriction: typeof(col1) = typeof(col2) = Collection[*]
	        Combine two collections into a single collection
	        returns: 
              [typeof(col1) (= typeof(col2))] *)
	
	   (********** Arithmetic and comparison operators, conditionals **********) 
	   | Add           of expr_t      * expr_t
	     (* Add(a,b)
	        restriction: typeof(a) = TFloat|TInt
	        restriction: typeof(b) = TFloat|TInt
	        Add two numbers together
	        returns: 
              [max(typeof(a), typeof(b)) (= TFloat if either is a TFloat)]
	     *)
	   | Mult          of expr_t      * expr_t
	     (* Mult(a,b)
	        restriction: typeof(a) = TFloat|TInt
	        restriction: typeof(b) = TFloat|TInt
	        Multiply two numbers together
	        returns: 
              [max(typeof(a), typeof(b))]
	     *)
	   | Eq            of expr_t      * expr_t
	     (* Eq(a,b)
	        restriction: typeof(a) = TFloat|TInt
	        restriction: typeof(b) = TFloat|TInt
	        Test two numbers for equality (returns 1 if equal, else 0)
	        returns:
              [TInt]
	     *)
	   | Neq           of expr_t      * expr_t
	     (* Neq(a,b)
	        restriction: typeof(a) = TFloat|TInt
	        restriction: typeof(b) = TFloat|TInt
	        Test two numbers for non-equality (returns 1 if not equal, else 0)
	        returns:
              [TInt]
	     *)
	   | Lt            of expr_t      * expr_t
	     (* Lt(a,b)
	        restriction: typeof(a) = TFloat|TInt
	        restriction: typeof(b) = TFloat|TInt
	        Compare two numbers (returns 1 if a < b, else 0)
	        returns:
              [TInt]
	     *)
	   | Leq           of expr_t      * expr_t
	     (* Leq(a,b)
	        restiction: typeof(a) = TFloat|TInt; typeof(b) = TFloat|TInt
	        Compare two numbers (returns 1 if a <= b, else 0)
	        returns:
              [TInt]
	     *)
	   | IfThenElse0   of expr_t      * expr_t
	     (* IfThenElse0(condition,statement)
	        restriction: typeof(condition) = TInt
	        restriction: typeof(statement) = TInt|TFloat
	        If condition is 0 then return 0, otherwise execute statement and
	        return its return value.
	        returns:
              [typeof(statement)]
	     *)

      (*** Control flow: conditionals, sequences, side-effecting iterations ***)
	   | IfThenElse    of expr_t      * expr_t   * expr_t
	     (* IfThenElse(condition,trueBranch,falseBranch)
	        restriction: typeof(condition) = TInt
	        restriction: typeof(trueBranch) = typeof(falseBranch)
	        If condition is 0 then execute and return falseBranch, otherwise
	        execute and return trueBranch
	        returns:
              [typeof(trueBranch) (= typeof(falseBranch))]
	     *)
	   | Block         of expr_t list 
	     (* Block(expressions)
	        restriction: Every element of expressions except the last one has
	                     type TUnit
	        Execute every statement in expressions and return the return value
	        of the last one
	        returns:
              [typeof(expressions[sizeof(expressions)-1])]
	     *)
	        
       | Iterate       of expr_t      * expr_t  
         (* Iterate(function, collection)
            restriction: typeof(function) = Fn[[a], TUnit]
            restriction: typeof(collection) = Collection(a)
            Apply function to every element of collection; ignoring the return
            type.
            returns:
              [TUnit]
         *)
	
	   (********** Functions **********)
	   | Lambda        of arg_t       * expr_t
	     (* Lambda(arg, body)
	        Create a function; When it is applied, the variable(s) defined in 
	        arg will be accessible in body through the Var() operator.
	        returns:
              if(typeof(expr_t) = Fn[innerArgs,innerBodyType] then
                 [Fn[innerArgs@[arg], innerBodyType]]
              else
                 [Fn[[arg], typeof(expr_t)]]
	     *)
	        
	   | AssocLambda   of arg_t       * arg_t    * expr_t
	     (* AssocLambda(arg1,arg2,body)
	        Create an associative function; As function above, can be used as
	        a shorthand for Lambda(arg2,Lambda(arg1,body)).
	     *)
	   | Apply         of expr_t      * expr_t
	     (* Apply(function,argument)
	        restriction: typeof(function) = Fn[rest @ [arg], retType]
	        restriction: typeof(argument) = arg (ATuple = TTuple | AVar = type)
	        Curry function with argument, and execute it if there are no more
	        arguments to be evalated.
	        returns:
              if(sizeof(rest) > 0) then
                 [Fn[rest, retType]]
              else
                 [retType]
	     *)
	        
	
	   (********** Structural recursion operators **********)
	   | Map              of expr_t      * expr_t 
	     (* Man(function, collection)
	        restriction: typeof(function) = Fn[[cType], retType]
	        restriction: typeof(collection) = Collection[cType]
	        Apply function to each element of collection and return a Collection
	        consisting of thereturn values.
	        returns:
              [Collection[retType]]
	     *)
	   | Flatten          of expr_t 
	     (* Flatten(collection)
	        restriction: typeof(collection) = Collection(Collection(cType))
	        Remove one level of nesting from collection
	        returns:
              [Collection[cType]]
	     *)
	   | Aggregate        of expr_t      * expr_t   * expr_t
	     (* Aggregate(function, initial, collection)
	        restriction: typeof(function) = Fn[[cType; aggType], aggType]
	        restriction: typeof(initial) = aggType
	        restriction: typeof(collection) = Collection[cType]
	        Fold over all elements of collection using function and the specified
	        initial value
	        returns:
              [aggType]
	     *)
	   | GroupByAggregate of expr_t      * expr_t   * expr_t * expr_t
	     (* GroupByAggregate(function,initial,classifier,collection)
	        restriction: typeof(function) = Fn[[cType; aggType], aggType]
	        restriction: typeof(initial) = aggType
	        restriction: typeof(classifier) = Fn[[cType], gbType]
	        restriction: typeof(collection) = Collection[cType]
	        restriction: gbType != TUnit
	        Partition the elements of collection into groups using classifier, 
	        and then fold over each group using function with the specified
	        initial value.
	        returns:
              if(typeof(gbType) = TTuple[contents]) then
                 [TTuple[contents @ [aggType]]]
              else
                 [TTuple[gbType; aggType]]
	     *)

       (********** Tuple collection operators **********)
       (*   Note: 
               Tuple collections operators are designed to access tuples as a
               key-value store.  For an n-field tuple, the first n-1 fields are
               the "key", and the nth field is the "value".  Also note that
               for arbitrary collections, uniqueness of keys is not implicitly 
               guaranteed -- Persistent Collections below guarantee uniqueness
               for the keys provided as arguments to PCUpdate/PCValueUpdate.
       *)
       
       | Member      of expr_t      * expr_t list  
         (* Member(collection, [keyList])
            restriction: typeof(collection) = Collection[TTuple[cList]]
            restriction: sizeof(keyList) = sizeof(cTypeList) - 1
            restriction: for(key,cType in keyList,cList){ typeof(key) = cType }
            Return 1 if a tuple who's first n-1 fields correspond to the 
            evaluation of keyList is present in collection, and 0 otherwise.
            returns:
               [TInt]
         *)
       | Lookup      of expr_t      * expr_t list
         (* Lookup(collection, [keyList])
            restriction: typeof(collection) = Collection[TTuple[cList]]
            restriction: sizeof(keyList) = sizeof(cTypeList) - 1
            restriction: for(key,cType in keyList,cList){ typeof(key) = cType }
            If a tuple who's first n-1 fields correspond to the evaluation of
            keyList is present in collection then return its nth field.  Raise
            an exception otherwise (behavior undefined in this case).
            returns:
               typeof(cList[sizeof(cList)-1])
         *)
       | Slice       of expr_t      * schema      * (id_t * expr_t) list
         (* Slice(collection, schema, [partialKey])
            restriction: typeof(collection) = Collection[schema]
            restriction: Each field in partialKey matches a field in schema
            restriction: The type of each expr_t in partialKey matches its id_t
            partialKey is a subset of the first n-1 fields of schema (which is
            itself the schema of collection).  Each expression in partialKey is
            evaluated and used to filter the elements of collection -- A 
            collection with an identical schema is returned, but it contains 
            only values where each field corresponding to a field of partialKey
            matches the evaluation of the corresponding expression
            returns:
               [typeof(collection)]
         *)

	   (********** Persistent collections **********)
	   | SingletonPC   of coll_id_t   * type_t
	     (* SingletonPC(name, type)
	        A reference to a persistent global variable.  The value of this 
	        variable may be changed by calling PCValueUpdate with no keys.
	        returns: 
	           [type]
	     *)
	   | OutPC         of coll_id_t   * schema   * type_t
	     (* OutPC(name, schema, type)
	        A reference to a persistent global collection optimized for slicing.
	        The value of this collection may be changed in two ways: The entire
	        collection may be replaced in bulk by calling PCUpdate with no keys
	        or individual elements of this collection may be updated by calling
	        PCValueUpdate with the "key" (as defined above under Tuple collection
	        operators) in the *second* (output variable) field.
	        returns:
	           [Collection[TTuple[schema@[type]]]]
	     *)
	   | InPC          of coll_id_t   * schema   * type_t
	     (* InPC(name, schema, type)
	        A reference to a persistent global collection optimized for 
	        membership testing. The value of this collection may be changed in 
	        two ways: The entire collection may be replaced in bulk by calling 
	        PCUpdate with no keys or individual elements of this collection may 
	        be updated by calling PCValueUpdate with the "key" (as defined above 
	        under Tuple collection operators) in the *first* (input variable)
	        field.
	        returns:
	           [Collection[TTuple[schema@[type]]]]
	     *)
	   | PC            of coll_id_t   * schema   * schema    * type_t
	     (* PC(name, inSchema, outSchema, type)
	        A reference to a persistent global collection consisting of an OutPC
	        nested inside an InPC.  An entire OutPC may be replaced by calling
	        PCUpdate with the "key" (as defined above under Tuple collection
	        operators) if the InPC element to be replaced.  Individual values
	        may be replaced by calling PCValueUpdate with the "key"s of the InPC
	        and the OutPC respectively.
	        returns:
	           [Collection[TTuple[
	              inSchema@[Collection[TTuple[outSchema@[type]]]]]]]
	     *)

	   | PCUpdate      of expr_t      * expr_t list * expr_t
	     (* PCUpdate(pcollection, keyList, value)
	        restriction: pcollection is defined by OutPC, InPC or PC
	        restriction: keyList is empty unless pcollection is a PC
	        restriction: value matches the type of pcollection or the OutPC
	                     portion of pcollection if it is a PC
	        Bulk replace the contents of an OutPC, InPC or the OutPC component
	        of a PC.  If pcollection is an OutPC or InPC, keyList must be empty
	        and the entire collection will be replaced by value.  If pcollection
	        is a PC, then keyList is evaluated and the resulting key will be 
	        used to determine which InPC element to set/replace.
	        returns:
	           [TUnit]
	     *)
	   | PCValueUpdate of expr_t      * expr_t list * expr_t list * expr_t 
	     (* PCValueUpdate(pcollection, inKey, outKey, value)
	        restriction: pcollection is defined by SingletonPC, OutPC, InPC or PC
	        restriction: inKey is empty unless pcollection is an InPC or PC
	        restriction: outKey is empty unless pcollection is an OutPC or PC
	        restriction: value matches the persistent collection's nested type
	        Replace a single element of a SingletonPC, OutPC, InPC or PC.  inKey
	        is used to index into an InPC or the InPC component of a PC.  outKey 
	        is used to index into an OutPC or the OutPC component of a PC.  If
	        either field is not relevant, it must be left blank.
	        returns:
	           [TUnit]
	     *)
    
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
    val string_of_expr : expr_t -> string
    val code_of_expr   : expr_t -> string
    
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
      TUnit -> "TUnit" | TFloat -> "TFloat" | TInt -> "TInt"
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
    | Const c -> ob(); ps ("Const(CFloat("^(string_of_const c)^"))"); cb()
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
      | TFloat -> "K3.SR.TFloat"
      | TInt -> "K3.SR.TInt"
      | TTuple(tlist) -> 
         "K3.SR.TTuple("^(Util.list_to_string ttostr tlist)^")"
      | Collection(subt) -> "K3.SR.Collection("^(ttostr subt)^")"
      | Fn(argt,rett) -> 
         "K3.SR.Fn("^(Util.list_to_string ttostr argt)^","^(ttostr rett)^")"
   )^")" in
   let string_of_vpair (v,vt) = "\""^v^"\","^(ttostr vt) in
   let vltostr = Util.list_to_string string_of_vpair in
   let argstr arg = 
      match arg with
         | AVar(v,vt) -> 
            "K3.SR.AVar("^(string_of_vpair (v,vt))^")"
         | ATuple(vlist) -> 
            "K3.SR.ATuple("^(vltostr vlist)^")"
   in
   match e with
      | Const c -> "K3.SR.Const(M3.CFloat("^(string_of_const c)^"))"
      | Var (id,t) -> "K3.SR.Var(\""^id^"\","^(ttostr t)^")"
      | Tuple e_l -> "K3.SR.Tuple("^(Util.list_to_string rcr e_l)^")"
      
      | Project (ce, idx) -> "K3.SR.Project("^(rcr ce)^","^
                           (Util.list_to_string string_of_int idx)^")"
      
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
      | IfThenElse  (pe,te,ee) -> 
            "K3.SR.IfThenElse("^(rcr pe)^","^(rcr te)^","^(rcr ee)^")"
      
      | Block   e_l        -> "K3.SR.Block("^(Util.list_to_string rcr e_l)^")"
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
            "K3.SR.Member("^(rcr me)^","^(Util.list_to_string rcr ke)^")"  
      | Lookup(me,ke) -> 
            "K3.SR.Lookup("^(rcr me)^","^(Util.list_to_string rcr ke)^")"
      | Slice(me,sch,pat_ve) -> 
            let pat_str = Util.list_to_string (fun (id,expr) ->
               "\""^id^"\","^(rcr expr)
            ) pat_ve in
            "K3.SR.Slice("^(rcr me)^","^(vltostr sch)^",("^pat_str^"))"
      | PCUpdate(me,ke,te) -> 
            "K3.SR.PCUpdate("^(rcr me)^","^(Util.list_to_string rcr ke)^","^
                            (rcr te)^")"
      | PCValueUpdate(me,ine,oute,ve) -> 
            "K3.SR.PCValueUpdate("^(rcr me)^","^(Util.list_to_string rcr ine)^
                                 ","^(Util.list_to_string rcr oute)^","^
                                 (rcr ve)^")"

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
