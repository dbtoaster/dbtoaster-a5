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
      | Comment       of string      * expr_t
        (* Comment(comment, expression)
           A comment tag.  The comment itself is ignored, and expression is 
           evaluated in its place
           returns:
             [typeof(expression)]
         *)
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
    val pre_map_expr : (expr_t -> expr_t) -> expr_t -> expr_t
    val post_map_expr : (expr_t -> expr_t) -> expr_t -> expr_t

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
    type trigger = M3.pm_t * M3.rel_id_t * M3.var_t list * statement list
    type program =
        M3.map_type_t list * M3Common.Patterns.pattern_map * trigger list
end

module SR : SRSig
