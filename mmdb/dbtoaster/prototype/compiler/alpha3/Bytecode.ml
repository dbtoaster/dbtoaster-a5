open Algebra

module SimplifiedAlgebra =
struct
    type ds_var_t = string
    type 'term_t conjunct_t = comp_t * 'term_t * 'term_t

    (* Map entries for arithmetic map terms *)
    type map_key_t =
        | MKConst   of const_t
        | MKVar     of var_t
        | MKSum     of map_key_t list
        | MKProduct of map_key_t list

    type map_entry_t = ds_var_t * (map_key_t list)

    type datastructure =
        | Multiset of (type_t list)
        | Map of (type_t list) * type_t

    (* Strict one- or two-level polynomials *)
    module type PolyBase = sig type t end

    module type PolynomialSig =
    sig
        type leaf_t
        type prod_t = PProduct of leaf_t list
        type expr_t =
            PTVal of leaf_t | PTProduct of prod_t | PSum of prod_t list

        val mk_val : leaf_t -> expr_t
        val mk_prod : leaf_t list -> prod_t
        val mk_sum : prod_t list -> expr_t
        val mk_val_prod : expr_t list -> expr_t
        val mk_prod_sum : expr_t list -> expr_t

        val fold : ('b list -> 'b) -> ('b list -> 'b) ->
            (leaf_t -> 'b) -> expr_t -> 'b
    end

    module Polynomial =
        functor (B : PolyBase) ->
    struct
        type leaf_t = B.t
        type prod_t = PProduct of leaf_t list
        type expr_t =
            PTVal of leaf_t | PTProduct of prod_t | PSum of prod_t list

        let mk_val lf = PTVal(lf)
        let mk_prod ll = PProduct(ll)
        let mk_sum pl = PSum(pl)

        let mk_val_prod tl =
            let mk_expr_leaf_l t = match t with
                | PTVal(x) -> [x]
                | PTProduct(PProduct(l)) -> l
                | PSum(pl) -> 
                      if List.length pl = 1 then
                          begin match (List.hd pl) with
                              | PProduct(l) -> l
                          end
                      else raise (Failure "mk_val_prod")
            in
                PTProduct(PProduct(List.flatten (List.map mk_expr_leaf_l tl)))

        let mk_prod_sum tl =
            let mk_expr_pprod t = match t with
                | PTVal(x) -> [PProduct([x])]
                | PTProduct(x) -> [x] 
                | PSum(l) ->
                      if List.length l = 1 then l
                      else raise (Failure "mk_prod_sum")
            in
                PSum(List.flatten (List.map mk_expr_pprod tl))

        let rec fold sum_f prod_f leaf_f t = match t with
            | PTVal(lf) -> leaf_f lf
            | PTProduct(PProduct(l)) -> prod_f (List.map leaf_f l)
            | PSum(l) -> 
                  let pl = List.map (fun x -> PTProduct(x)) l in 
                      sum_f(List.map (fold sum_f prod_f leaf_f) pl)
    end

    (* Nested polynomial relational and map algebra *)
    type 'term_t relalg_lf_t =
        | Empty
        | ConstantNullarySingleton
        | AtomicConstraint of 'term_t conjunct_t
        | Rel of ds_var_t * (var_t list) * datastructure

    type ('term_t, 'relalg_t) mapalg_lf_t =
        | Const   of const_t
        | Var     of var_t
        | AggSum  of 'term_t  * 'relalg_t

    module rec RelAlgBase : 
    sig
        type t = MapAlg.expr_t relalg_lf_t
    end =
    struct
        type t = MapAlg.expr_t relalg_lf_t
    end
    and RelAlg : PolynomialSig with type leaf_t = RelAlgBase.t
        = Polynomial(RelAlgBase)

    and MapAlgBase :
    sig
        type t = (MapAlg.expr_t, RelAlg.expr_t) mapalg_lf_t
    end =
    struct
        type t = (MapAlg.expr_t, RelAlg.expr_t) mapalg_lf_t
    end
    and MapAlg : PolynomialSig with type leaf_t = MapAlgBase.t
        = Polynomial(MapAlgBase)


    (* Arithmetic map entry expressions *)
    type arith_mapalg_lf_t =
        | AMConst    of const_t
        | AMVar      of var_t
        | AMMapEntry of map_entry_t

    type 'arith_term_t arith_mapalg_lf_expr_t =
        | AMVal      of arith_mapalg_lf_t
        | AMIfThen   of (('arith_term_t conjunct_t) list) * 'arith_term_t

    module rec ArithMapAlgBase :
    sig
        type t = ArithMapAlg.expr_t arith_mapalg_lf_expr_t
    end =
    struct
        type t = ArithMapAlg.expr_t arith_mapalg_lf_expr_t
    end
    and ArithMapAlg :
        PolynomialSig with type leaf_t = ArithMapAlgBase.t
        = Polynomial(ArithMapAlgBase)


    (* Polynomial relational algebra and map algebra types *)
    type relalg_t = RelAlg.expr_t
    type mapalg_t = MapAlg.expr_t
    type arith_mapalg_t = ArithMapAlg.expr_t

    let rec fold_relalg sum_f prod_f leaf_f r =
        let prod_aux =
            function | RelAlg.PProduct(l) -> prod_f (List.map leaf_f l)
        in
            begin match r with
                | RelAlg.PTVal(lf) -> leaf_f lf
                | RelAlg.PTProduct(prod_l) -> prod_aux prod_l
                | RelAlg.PSum(l) -> sum_f (List.map prod_aux l)
            end

    let rec fold_term sum_f prod_f leaf_f t =
        let prod_aux =
            function | MapAlg.PProduct(l) -> prod_f (List.map leaf_f l)
        in
            begin match t with
                | MapAlg.PTVal(lf) -> leaf_f lf
                | MapAlg.PTProduct(prod_l) -> prod_aux prod_l
                | MapAlg.PSum(l) -> sum_f (List.map prod_aux l)
            end

    let rec relalg_vars (r : relalg_t) : var_t list =
        let leaf_f lf =
            match lf with
                | Empty -> []
                | ConstantNullarySingleton -> []
                | Rel(_,s,_) -> Util.ListAsSet.no_duplicates s
                | AtomicConstraint (_, c1, c2) ->
                      Util.ListAsSet.union (term_vars c1) (term_vars c2)
        in
            fold_relalg Util.ListAsSet.multiunion
                Util.ListAsSet.multiunion leaf_f r

    and term_vars (t: mapalg_t) : var_t list =
        let leaf_f x = match x with
            | Var(y) -> [y]
            | AggSum(f, r) -> Util.ListAsSet.union (term_vars f) (relalg_vars r)
            | _ -> []
        in
            fold_term Util.ListAsSet.multiunion
                Util.ListAsSet.multiunion leaf_f t

    let rec declared_plan_vars (r : relalg_t) : var_t list =
        let leaf_f lf = match lf with
            | Rel(_,v,_) -> Util.ListAsSet.no_duplicates v
            | AtomicConstraint(_, t1, t2) ->
                  Util.ListAsSet.union
                      (declared_term_vars t1) (declared_term_vars t2)
            | _ -> []
        in
            fold_relalg
                Util.ListAsSet.multiunion Util.ListAsSet.multiunion leaf_f r

    and declared_term_vars (t: mapalg_t) : var_t list =
        let leaf_f lf = match lf with
            | AggSum(f,r) -> Util.ListAsSet.union
                  (declared_term_vars f) (declared_plan_vars r)
            | _ -> []
        in
            fold_term
                Util.ListAsSet.multiunion Util.ListAsSet.multiunion leaf_f t

    let free_relalg_vars (r: relalg_t) : var_t list =
        Util.ListAsSet.diff (relalg_vars r) (declared_plan_vars r)

    let free_term_vars (t: mapalg_t) : var_t list = 
        Util.ListAsSet.diff (term_vars t) (declared_term_vars t)

    let rec relalg_schema (r : relalg_t) : var_t list =
        let leaf_f lf =
            match lf with
                | Rel (_,sch,_) -> sch
                | AtomicConstraint _ -> []
                | Empty -> []
                | ConstantNullarySingleton -> []
        in
            fold_relalg
                Util.ListAsSet.multiunion Util.ListAsSet.multiunion leaf_f r

    let rec term_type (t: mapalg_t) (extra_vars : var_t list) : type_t =
        let debug_undefined_var (n,t) r =
            print_endline ("Could not find var "^n)
        in
        let promote_type a b =
            let msg = "Type promotion mismatch:" in
                match (a,b) with
                    | (TString, TString) -> TString
                    | (TString, _) -> raise (Failure msg)
                    | (TDouble, TLong) | (TLong, TDouble) ->
                          raise (Failure (msg^" precision exception"))
                    | (TDouble, _) | (_, TDouble) -> TDouble
                    | (TLong,_) | (_, TLong) -> TLong
                    | _ -> TInt
        in
        let leaf_f lf = match lf with
            | Const (Int(_))    -> TInt
            | Const (Double(_)) -> TDouble
            | Const (Long(_))   -> TLong
            | Const (String(_)) -> TString
            | Var (n,t) -> t
            | AggSum (f, r) ->
                  (* validate types of vars used in f with those defined in r *)
                  let r_vars = relalg_schema r in
                  let f_vars = free_term_vars f in
                  let eq_name v1 v2 = (fst v1) = (fst v2) in
                  let inconsistent_vars =
                      List.filter (fun ((n,t) as v) ->
                          let (n2,t2) = 
                              try List.find (eq_name v)
                                  (Util.ListAsSet.union r_vars extra_vars)
                              with Not_found ->
                                  debug_undefined_var v r;
                                  raise (Failure
                                      ("No such var "^n^" in relational part"))
                          in t <> t2)
                          f_vars
                  in
                      if inconsistent_vars = [] then term_type f extra_vars
                      else raise (Failure
                          "Type inconsistencies in aggregate variable usage.")
        in
        let list_f l = List.fold_left promote_type (List.hd l) (List.tl l) in
        let prod_f = function | MapAlg.PProduct(l) -> list_f (List.map leaf_f l)
        in
            begin match t with
                | MapAlg.PTVal(lf) -> leaf_f lf
                | MapAlg.PTProduct(prod_l) -> prod_f prod_l
                | MapAlg.PSum(l) -> list_f (List.map prod_f l)
            end
end

module InstructionSet =
struct
    open SimplifiedAlgebra

    (* Relational assignment statement *)
    type rel_assign_t =
        | MultisetInsert of datastructure *
                            ds_var_t * (var_t list) * relalg_t
        | MultisetDelete of datastructure *
                            ds_var_t * (var_t list) * relalg_t

    (* Map assignment statement *)
    type map_assign_t =
        | MapSet    of datastructure * map_entry_t * arith_mapalg_t
        | MapUpdate of datastructure * map_entry_t * arith_mapalg_t
        | MapInsert of datastructure * map_entry_t * mapalg_t
        | MapDelete of datastructure * map_entry_t

    (* Assignment blocks *)
    type flat_assign_t =
        | RelAssign of rel_assign_t list
        | MapAssign of map_assign_t list

    (* Nested map assignment block *)
    type nested_assign_t =
        | Assign of flat_assign_t list
        | IfThen of (arith_mapalg_t conjunct_t list) * (nested_assign_t list)
        | ForEach of datastructure *
              ds_var_t * (var_t list) * (nested_assign_t list)

    (* map defined * environment * assignments *)
    type nested_block_t = ds_var_t * (var_t list) * (nested_assign_t list)


    (* Partially ordered assignments *)
    module NestedBlockComparator =
    struct
        type t = nested_block_t
        let compare = Pervasives.compare
    end

    module NestedBlockSet = Set.Make(NestedBlockComparator)

    type assignment_blocks = NestedBlockSet.t list

    let fold (rel_f: rel_assign_t -> 'a)
             (map_f: map_assign_t -> 'b)
             (rel_assign_f: 'a list -> 'c)
             (map_assign_f: 'b list -> 'c)
             (nested_assign_f: nested_assign_t -> 'c list -> 'e)
             (nested_f: nested_assign_t -> 'd -> 'e)
             (block_f: 'e list -> 'd) 
             (b: nested_assign_t list)
            =
        let assign_aux l = List.map (function
            | RelAssign(r) -> rel_assign_f (List.map rel_f r)
            | MapAssign(m) -> map_assign_f (List.map map_f m))
            l
        in
        let rec block_aux l = List.map (function
            | Assign(fal) as x -> nested_assign_f x (assign_aux fal)

            | IfThen(cl, nal) as x ->
                  nested_f x (block_f (block_aux nal))

            | ForEach(ds, v, loop_vars, nal) as x ->
                  nested_f x (block_f (block_aux nal))) l
        in
            block_f (block_aux b)

    let substitute_variables bc mapping =
        let subs_vars vl = List.map (Util.Vars.apply_mapping mapping) vl in
        let subs_entry (m,e) =
            let rec subs_mk k = match k with 
                | MKVar(v) -> MKVar(Util.Vars.apply_mapping mapping v)
                | MKSum(l) -> MKSum(List.map subs_mk l)
                | MKProduct(l) -> MKProduct(List.map subs_mk l)
                | _ -> k                
            in
                (m, List.map subs_mk e)
        in

        let rec subs_relalg r =
            let leaf_f lf = RelAlg.PTVal(match lf with
                | Empty -> Empty
                | ConstantNullarySingleton -> ConstantNullarySingleton
                | AtomicConstraint (op,t1,t2) ->
                      AtomicConstraint(op, subs_mapalg t1, subs_mapalg t2)
                | Rel (dsv, f, ds) -> Rel(dsv, subs_vars f, ds))
            in
                RelAlg.fold RelAlg.mk_prod_sum RelAlg.mk_val_prod leaf_f r
        and subs_mapalg m =
            let leaf_f lf = MapAlg.PTVal(match lf with
                | Const _ -> lf
                | Var(v) -> Var(Util.Vars.apply_mapping mapping v)
                | AggSum(f,r) -> AggSum(subs_mapalg f, subs_relalg r))
            in
                MapAlg.fold MapAlg.mk_prod_sum MapAlg.mk_val_prod leaf_f m
        in

        let rec subs_arith_conjuncts conj_l =
            List.map (fun (op, a1, a2) ->
                (op, subs_arith a1, subs_arith a2)) conj_l
        and subs_arith a =
            let leaf_f lf = ArithMapAlg.PTVal(match lf with
                | AMVal(AMConst(c)) -> lf
                | AMVal(AMVar(v)) ->
                      AMVal(AMVar(Util.Vars.apply_mapping mapping v))
                | AMVal(AMMapEntry(e)) -> AMVal(AMMapEntry(subs_entry e))
                | AMIfThen(cl, a) ->
                      AMIfThen(subs_arith_conjuncts cl, subs_arith a))
            in
                ArithMapAlg.fold 
                    ArithMapAlg.mk_prod_sum ArithMapAlg.mk_val_prod leaf_f a
        in

        let subs_rel_f = function
            | MultisetInsert(ds, dsv, tv, r) ->
                  MultisetInsert(ds,dsv, subs_vars tv, subs_relalg r)
            | MultisetDelete(ds, dsv, tv, r) ->
                  MultisetDelete(ds,dsv, subs_vars tv, subs_relalg r)
        in
        let subs_map_f = function
            | MapSet (ds,e,arith) ->
                  MapSet(ds, subs_entry e, subs_arith arith)
            | MapUpdate (ds,e,arith) ->
                  MapUpdate(ds, subs_entry e, subs_arith arith)
            | MapInsert (ds,e,m) ->
                  MapInsert(ds, subs_entry e, subs_mapalg m)
            | MapDelete (ds,e) -> MapDelete(ds, subs_entry e)
        in
        let subs_rel_assign_f children = RelAssign(children) in
        let subs_map_assign_f children = MapAssign(children) in
        let subs_nested_assign_f n flat_ch = match n with
            | Assign(_) -> Assign(flat_ch)
            | _ -> raise (Failure "nested_assign_f")
        in
        let subs_nested_f n ch = match n with
            | IfThen(cl, _) -> IfThen(subs_arith_conjuncts cl, ch)
            | ForEach(ds,v,lv,_) -> ForEach(ds,v, subs_vars lv, ch)
            | _ -> raise (Failure "nested_f")
        in
        let subs_block_f x = x in
            fold subs_rel_f subs_map_f subs_rel_assign_f subs_map_assign_f
                subs_nested_assign_f subs_nested_f subs_block_f bc

    let bytecode_as_string bc =
        let string_of_comparison op = match op with
            | Eq -> "=" | Neq -> "<>" | Lt -> "<" | Le -> "<=" in
        let string_of_const c = match c with
                | Int(i)    -> string_of_int i
                | Long(l)   -> Int64.to_string l
                | Double(d) -> string_of_float d
                | String(s) -> s
        in
        let string_of_vars l = String.concat "," (List.map fst l) in
        let string_of_entry k_l =
            let rec string_of_k k = match k with 
                | MKConst(c) -> string_of_const c
                | MKVar(n,_) -> n
                | MKSum(l) -> String.concat "+" (List.map string_of_k l)
                | MKProduct(l) -> String.concat "*" (List.map string_of_k l)
            in
                String.concat "," (List.map string_of_k k_l)
        in
        let rec string_of_relalg r =
            let leaf_f lf = match lf with
                | Empty -> "false"
                | ConstantNullarySingleton -> "true"
                | AtomicConstraint (op,t1,t2) ->
                      (string_of_mapalg t1)^" "^(string_of_comparison op)^" "^
                          (string_of_mapalg t1)

                | Rel (dsv, f, ds) -> dsv^"("^(string_of_vars f)^")"
            in
                RelAlg.fold (String.concat " or ") (String.concat " and ") leaf_f r
        and string_of_mapalg m =
            let leaf_f lf = match lf with
                | Const(c) -> string_of_const c
                | Var((n,_)) -> n
                | AggSum(f,r) -> "Sum("^(string_of_mapalg f)^","^(string_of_relalg r)^")"
            in
                MapAlg.fold (String.concat "+") (String.concat "*") leaf_f m
        in
        let rec string_of_arith a =
            let leaf_f lf = match lf with
                | AMVal(AMConst(c)) -> string_of_const c
                | AMVal(AMVar(n,_)) -> n
                | AMVal(AMMapEntry(en,ek)) -> en^"["^(string_of_entry ek)^"]"
                | AMIfThen(cl, a) ->
                      "if ("^(string_of_arith_conjuncts cl)^")"^
                          " then { "^(string_of_arith a)^" }"
            in
                ArithMapAlg.fold (String.concat "+") (String.concat "*") leaf_f a
        and string_of_arith_conjuncts cl = 
            String.concat " and " (List.map (fun (op, a1, a2) ->
                (string_of_arith a1)^" "^(string_of_comparison op)^" "^
                    (string_of_arith a2)) cl)
        in
        let string_of_rel x =
            let (op,dsv,tv,r) = match x with
                | MultisetInsert(ds, dsv, tv, r) -> ("insert",dsv,tv,r)
                | MultisetDelete(ds, dsv, tv, r) -> ("delete",dsv,tv,r)
            in
                dsv^"."^op^"("^(string_of_vars tv)^", "^(string_of_relalg r)^")"
        in
        let string_of_map x =
            let (op,mapn,entry,value) = match x with
                | MapSet (ds,(en,ek),arith) ->
                      ("set", en, string_of_entry ek, string_of_arith arith)
                | MapUpdate (ds,(en,ek),arith) ->
                      ("update", en, string_of_entry ek, string_of_arith arith)
                | MapInsert (ds,(en,ek),m) ->
                      ("insert", en, string_of_entry ek, string_of_mapalg m)
                | MapDelete (ds,(en,ek)) ->
                      ("delete", en, string_of_entry ek, "")
            in
                mapn^"."^op^"("^entry^
                    (if entry = "" || value = "" then "" else ",")^value^")"
        in
        let string_of_rel_assign ch = "RA["^(String.concat ", " ch)^"]" in
        let string_of_map_assign ch = "MA["^(String.concat ", " ch)^"]" in
        let string_of_nested_assign n flat_ch = match n with
            | Assign(_) -> "A["^(String.concat ", " flat_ch)^"]"
            | _ -> raise (Failure "nested_assign_f")
        in
        let string_of_nested n ch = match n with
            | IfThen(cl, _) ->
                  "if ("^(string_of_arith_conjuncts cl)^")"^
                      " then { "^(String.concat "; " ch)^" }"
            | ForEach(ds,v,lv,_) ->
                  "for each ("^(string_of_vars lv)^" in "^v^") "^
                      "{ "^(String.concat "; " ch)^" }"
            | _ -> raise (Failure "nested_f")
        in
        let string_of_block x = ["B{ "^(String.concat "; " x)^" }"]
        in
            String.concat ";"
            (fold string_of_rel string_of_map
                string_of_rel_assign string_of_map_assign
                string_of_nested_assign string_of_nested string_of_block bc)
end


module type GeneratorSignature =
sig
    type term_map_bindings = (term_t * (string * (var_t list))) list
    type var_bindings = (var_t * var_t) list

    type declaration =
        SimplifiedAlgebra.ds_var_t * SimplifiedAlgebra.datastructure

    type ordered_blocks = InstructionSet.NestedBlockSet.t list
    type handler_blocks = (string * ordered_blocks) list

    val get_declarations : unit -> declaration list
    val clear_declarations : unit -> unit

    val create_handler_block :
        (string * InstructionSet.nested_block_t) list -> handler_blocks

    val merge_handler_blocks :
        handler_blocks -> handler_blocks -> handler_blocks

    val append_handler_blocks :
        handler_blocks -> handler_blocks -> handler_blocks

    val create_dependent_handler_block :
        (string * InstructionSet.nested_block_t) list -> handler_blocks

    val term_as_bytecode :
        string -> readable_term_t -> var_t list -> var_t list ->
        (var_t * var_t * readable_relalg_lf_t) list ->
        term_map_bindings -> var_bindings
            -> InstructionSet.nested_assign_t list

    val generate_bigsum_maintenance :
        string -> var_t -> var_t -> string -> handler_blocks
end

module Generator : GeneratorSignature =
struct
    module A = SimplifiedAlgebra
    module I = InstructionSet

    module RB = A.RelAlgBase
    module R = A.RelAlg

    module MB = A.MapAlgBase
    module M = A.MapAlg

    module AMB = A.ArithMapAlgBase
    module AM = A.ArithMapAlg

    module BS = InstructionSet.NestedBlockSet

    type declaration = A.ds_var_t * A.datastructure

    type ordered_blocks = BS.t list
    type handler_blocks = (string * ordered_blocks) list

    exception BCGenException of string

    (* Parameters *)
    let bigsum_ds_suffix = "_dom"

    (* Hashtable for data structure declarations *)
    let decls = Hashtbl.create 20

    let add_declaration decl_name declaration = 
        begin if not(Hashtbl.mem decls decl_name) then
            Hashtbl.add decls decl_name declaration
        end

    let get_declarations () =
        Hashtbl.fold (fun k v acc -> acc@[k,v]) decls []

    let clear_declarations () = Hashtbl.clear decls

    (* Misc helpers *) 
    type ('a, 'b) table_fn_t = ('a * 'b) list
    type term_map_bindings = (term_t * (string * (var_t list))) list
    type var_bindings = (var_t * var_t) list

    let apply_or_fail (bindings: ('a, 'b) table_fn_t) (x: 'a) : 'b =
        let g (y, z) = if(x = y) then [z] else [] in
        let x2 = List.flatten (List.map g bindings)
        in
            if (List.length x2) = 1 then (List.hd x2)
            else if (List.length x2) = 0 then
                raise (BCGenException "No mapping found during apply or fail.")
            else raise
                (BCGenException
                    "Non functional mapping exception for apply or fail.")

    (* Handler block helpers *)

    (* (string * block set) list -> handler_blocks *)
    let create_handler_block rel_blocks =
        List.fold_left
            (fun acc (reln, block) ->
                if List.mem_assoc reln acc then
                    let existing_blocks = List.assoc reln acc in
                    let new_blocks =
                        [BS.add block (List.hd existing_blocks)]
                    in
                        (List.remove_assoc reln acc)@[(reln, new_blocks)]
                else
                    let new_blocks = [BS.add block BS.empty] in
                        acc@[(reln, new_blocks)])
            [] rel_blocks

    let merge_handler_blocks l r =
        let combine_list a b f =
            let (short,long) = 
                if List.length a < List.length b then (a,b) else (b,a)
            in
            let (long_common, long_rem,_) =
                List.fold_left
                    (fun (c,r,cnt) x ->
                        if cnt > 0 then (c@[x], r, cnt-1)
                        else (c, r@[x], cnt-1))
                    ([], [], List.length short) long
            in
                (List.map2 f short long_common)@long_rem
        in
        List.fold_left
            (fun acc (reln, bs) ->
                if List.mem_assoc reln acc then
                    let existing_blocks = List.assoc reln acc in
                    let new_blocks = combine_list existing_blocks bs BS.union in
                        (List.remove_assoc reln acc)@[(reln, new_blocks)]
                else acc@[(reln, bs)])
            l r

    let append_handler_blocks l r =
        List.fold_left
            (fun acc (reln, bs) ->
                if List.mem_assoc reln acc then
                    let existing_blocks = List.assoc reln acc in
                    let new_blocks = existing_blocks@bs in
                        (List.remove_assoc reln acc)@[(reln, new_blocks)]
                else acc@[(reln, bs)])
            l r

    (* Returns if the block set defines any environment vars *)
    (* var_t list -> block set -> bool *)
    let find_dependent benv blockset =
        let bs_maps = BS.fold
            (fun (bmap,_,_) bmap_acc -> bmap::bmap_acc)
            blockset []
        in
        let env_matches =
            List.filter (fun (ev, evt) ->
                List.exists (fun mn -> mn = ev) bs_maps) benv
        in
            (List.length env_matches) > 0

    (* Adds the block to a set after each dependency *)
    (* (block set) list -> block -> (block set) list *)
    let add_dependent_block blocks b =
        let (bmap, benv, bassign) = b in
        let rev_blocks = List.rev blocks in
        let (dep_found, pos) =
            List.fold_left
                (fun (added, counter) dep_bs ->
                    if added then (added, counter)
                    else
                        let has_dependent = find_dependent benv dep_bs in
                            if has_dependent then (true, counter)
                            else(false, counter+1))
                (false, 0) rev_blocks
        in
            if dep_found then
                if pos = 0 then
                    (* Create a new blockset *)
                    blocks@[BS.add b BS.empty]
                else
                    (* Add before position *)
                    fst (List.fold_left
                        (fun (acc,cnt) blockset ->
                            if cnt = (pos-1) then
                                ((BS.add b blockset)::acc, cnt+1)
                            else (acc, cnt+1))
                        ([], 0) rev_blocks)
            else if blocks = [] then
                (* Create a new list *)
                [BS.add b BS.empty]
            else
                (* Add to head of original list *)
                (BS.add b (List.hd blocks))::(List.tl blocks)

    (* Creates handler blocks preserving dependencies *)
    (* (string * block set) list -> handler_blocks *)
    let create_dependent_handler_block rel_blocks =
        List.fold_left
            (fun acc (reln, block) ->
                if List.mem_assoc reln acc then
                    let existing_blocks = List.assoc reln acc in
                    let new_blocks =
                        add_dependent_block existing_blocks block
                    in
                        (List.remove_assoc reln acc)@[(reln, new_blocks)]
                else
                    let new_blocks = [BS.add block BS.empty] in
                        acc@[(reln, new_blocks)])
            [] rel_blocks

    (* Bytecode construction *)

    let mk_zero type_t = match type_t with
        | TInt -> Int(0)
        | TDouble -> Double(0.0)
        | TLong -> Long(Int64.zero)
        | TString -> String("")

    let rec replace_with_args arg_bindings map_key =
        match map_key with
            | A.MKVar(x) -> A.MKVar(Util.apply arg_bindings x x)
            | A.MKSum(l) ->
                  A.MKSum(List.map (replace_with_args arg_bindings) l)
            | A.MKProduct(l) ->
                  A.MKProduct(List.map (replace_with_args arg_bindings) l)
            | _ -> map_key

    (* nested_assign_t list -> bool *)
    let is_functional (b : I.nested_assign_t list) : bool =
        let is_map_arith = function
            | I.MapSet _ | I.MapUpdate _ -> true
            | _ -> false in
        let is_map_arith_list fa = match fa with
            | I.MapAssign(mal) -> List.for_all is_map_arith mal
            | _ -> false
        in
        let is_map_assign_list na = match na with
             | I.Assign (al) -> List.for_all is_map_arith_list al
             | _ -> false
        in
            List.for_all is_map_assign_list b

    (* nested_assign_t list -> bool *)
    let is_single_functional b = (List.length b = 1) && is_functional b

    (* flat_assign_t -> bool *)
    let is_single_assignment fa = match fa with
        | I.RelAssign(l) -> List.length l = 1
        | I.MapAssign(l) -> List.length l = 1
        
    (* nested_assign_t -> bool *)
    let rec is_single_nested_assignment a = match a with
        | I.Assign(l) -> List.length l = 1 && (is_single_assignment (List.hd l))
        | I.IfThen(_,l) | I.ForEach(_,_,_,l) ->
              List.length l = 1 && (is_single_nested_assignment (List.hd l))
        
    (* nested_assign_t list -> bool *)
    let is_single_block b =
        (List.length b = 1) && is_single_nested_assignment (List.hd b)

    (* nested_assign_t list -> (arith_mapalg_t list -> arith_mapalg_t) ->
     *     arith_mapalg_t *)
    let block_as_arith b f =
        let map_arith_aux fa = match fa with
            | I.MapSet(_,_,arith) | I.MapUpdate(_,_,arith) -> arith
            | _ -> raise (BCGenException "Expected a map update.")
        in
        let map_assign_aux ma = match ma with
            | I.MapAssign(fal) -> List.map map_arith_aux fal
            | _ -> raise (BCGenException "Expected a map assignment.")
        in
        let assign_aux b = 
            match b with
                | I.Assign(mal) -> List.flatten (List.map map_assign_aux mal)
                | _ -> raise (BCGenException "Expected an assignment.")
        in
        let arith_list = List.flatten (List.map assign_aux b) in
            f arith_list

    (* unit -> datastructure * map_entry_t *)
    let temp_counter = ref 0
    let create_temporary base_name val_type =
        let c = !temp_counter in
            incr temp_counter;
            (A.Map([], val_type), (base_name^(string_of_int c), []))

    let rec constraints_as_nblock
            (map_entry: A.map_entry_t)
            (map_ds : A.datastructure)
            (then_term: readable_term_t)
            (extra_vars: var_t list)
            (bigsum_vars_and_rels: (var_t * var_t * readable_relalg_lf_t) list)
            (map_bindings: term_map_bindings)
            (arg_bindings: var_bindings)
            (r: readable_relalg_t) : I.nested_assign_t list
            =
        let leaves : readable_relalg_lf_t list =
            fold_relalg List.flatten List.flatten (fun x -> [x]) r
        in
        let is_constraint =
            function | AtomicConstraint _ -> true | _ -> false in
        let create_block t map_entry map_ds =
            term_as_nblock map_entry map_ds
                extra_vars bigsum_vars_and_rels map_bindings arg_bindings t
        in
        let create_block_or_arith t =
            let t_type = term_type t extra_vars in
            let (t_ds, t_me) = create_temporary "cstr" t_type in
            let t_nblock = create_block t t_me t_ds in
                if is_single_functional t_nblock then
                    ([], block_as_arith t_nblock List.hd)
                else 
                    (* Temporary map initialization *)
                    let init_val =
                        AM.PTVal(A.AMVal(A.AMConst(mk_zero t_type))) in
                    let init_mu = I.MapSet(t_ds, t_me, init_val) in
                    let init_block = [I.Assign([I.MapAssign([init_mu])])]
                    in
                        (init_block@t_nblock,
                        AM.PTVal(A.AMVal(A.AMMapEntry(t_me))))
        in
        let (assign_ll, conjunct_l) = List.split (List.map
            (function
                | AtomicConstraint(op,t1,t2) ->
                      let (t1_b, t1_arith) = create_block_or_arith t1 in
                      let (t2_b, t2_arith) = create_block_or_arith t2 in
                          (t1_b@t2_b, (op, t1_arith, t2_arith))

                | _ -> raise (BCGenException
                      ("Invalid relational algebra term, "^
                          "expected constraints only.")))
            (List.filter is_constraint leaves))
        in
        let assign_l = List.flatten assign_ll in
        let then_block = create_block then_term map_entry map_ds in
            if assign_l = [] && is_single_functional then_block then
                let arith = AM.PTVal(A.AMIfThen(
                    conjunct_l, block_as_arith then_block List.hd))
                in
                let mu = I.MapUpdate(map_ds, map_entry, arith) in
                    [I.Assign([I.MapAssign([mu])])]
            else
                assign_l@([I.IfThen(conjunct_l, then_block)])

    and term_lf_as_nblock
            (map_entry: A.map_entry_t)
            (map_ds: A.datastructure)
            (extra_vars: var_t list)
            (bigsum_vars_and_rels: (var_t * var_t * readable_relalg_lf_t) list)
            (map_bindings: term_map_bindings)
            (arg_bindings: var_bindings)
            (lf: readable_term_lf_t) : I.nested_assign_t list
            =
        let bigsum_vars = List.map (fun (x,_,_) -> x) bigsum_vars_and_rels in
        let single_map_assign x = [I.Assign([I.MapAssign([x])])] in
        let get_bigsum_ds l =
            List.map (fun v ->
                let var_and_rel_l = List.filter
                    (fun (x,_,_) -> x = v) bigsum_vars_and_rels
                in
                    match var_and_rel_l with
                        | [] -> raise (BCGenException
                              ("No bigsum datastructure found."))

                        | [(((bsvn, bsvt) as bsv), _, _)] ->
                              (bsv, A.Multiset([bsvt]), bsvn^bigsum_ds_suffix)

                        | _ -> raise (BCGenException
                              ("Found duplicate bigsum datastructures.")))
                l
        in
        let create_nested_for_loops b bsv_ds_vars_l =
            List.fold_left
                (fun acc (bsv, ds, dsvar) ->
                    print_endline ("Creating for loop for: "^(fst bsv));
                    [I.ForEach(ds, dsvar, [bsv], acc)])
                b bsv_ds_vars_l
        in
        let get_var x = Util.apply arg_bindings x x in
            begin match lf with
                | Const (c) -> single_map_assign
                      (I.MapUpdate(map_ds, map_entry,
                          AM.PTVal(A.AMVal(A.AMConst(c)))))

                | Var(x) -> single_map_assign
                      (I.MapUpdate(map_ds, map_entry,
                          AM.PTVal(A.AMVal(A.AMVar(get_var x)))))

                | AggSum(f,r) ->
                      if constraints_only (make_relalg r) then
                          let f_vars = term_vars (make_term f) in
                          let local_bigsum_vars =
                              Util.ListAsSet.inter bigsum_vars f_vars in
                          let cstr_block =
                              constraints_as_nblock
                                  map_entry map_ds f
                                  extra_vars bigsum_vars_and_rels
                                  map_bindings arg_bindings r
                          in
                              if local_bigsum_vars <> [] then
                                  (* Nested for loops over local bigsum vars *)
                                  let lv = get_bigsum_ds local_bigsum_vars in
                                      create_nested_for_loops cstr_block lv
                              else
                                  cstr_block
                      else
                          let (lf_map_name, lf_map_params) =
                              apply_or_fail map_bindings (make_term (RVal(lf)))
                          in
                          let renamed_lf_map_params = List.map get_var lf_map_params in
                          let lf_map_entry =
                              (lf_map_name,
                              List.map (fun x -> A.MKVar(get_var x)) renamed_lf_map_params)
                          in
                          let lf_bigsum_vars =
                              (* TODO: initial value computation for bigsum vars *)
                              List.filter (fun v ->
                                  List.exists (fun (w,_,_) -> v=w) bigsum_vars_and_rels)
                                  renamed_lf_map_params
                          in
                              print_endline ("term_lf_as_nblock "^lf_map_name^" leaf map bigsum: "^
                                  (String.concat "," (List.map fst lf_bigsum_vars)));
                              single_map_assign(
                                  I.MapUpdate(map_ds, map_entry,
                                      AM.PTVal(
                                          A.AMVal(A.AMMapEntry(lf_map_entry)))))
            end

    and term_as_nblock
            (map_entry: A.map_entry_t)
            (map_ds: A.datastructure)
            (extra_vars: var_t list)
            (bigsum_vars_and_rels: (var_t * var_t * readable_relalg_lf_t) list)
            (map_bindings: term_map_bindings)
            (arg_bindings: var_bindings)
            (t: readable_term_t) : I.nested_assign_t list
            =
        let combine_arith_sum al = 
            let new_al = List.map
                (fun x -> match x with
                    | AM.PTVal(x) -> AM.PProduct([x])
                    | AM.PTProduct(y) -> y
                    | _ -> raise (BCGenException ("Invalid product arith.")))
                al
            in
                AM.PSum(new_al)
        in
        let combine_arith_product vl =
            let new_al = List.flatten (List.map
                (fun x -> match x with
                    | AM.PTVal(x) -> [x]
                    | _ -> raise (BCGenException ("Invalid val arith.")))
                vl)
            in
                AM.PTProduct(AM.PProduct(new_al))
        in
        let ensure_prod_or_lf = function
            | RProd(_) as x -> x
            | RVal(_) as x -> x
            | _ -> raise (BCGenException "Expected a product") in
        let ensure_lf = function
            | RVal(y) as x -> x
            | _ -> raise (BCGenException "Expected a leaf value")
        in
        let add_declarations ds_me_l =
            List.iter
                (fun (ds, (name, _)) -> add_declaration name ds)
                ds_me_l
        in
        let list_f l check_f combine_arith_f combine_f =
            let valid_l = List.map check_f l in
            let block_maps = List.map
                (fun x -> 
                    let x_type = term_type x extra_vars in
                        (create_temporary
                            (let (mapn,_) = map_entry in mapn) x_type,
                        x_type))
                valid_l
            in
            let sub_blocks = List.map2
                (fun c_t ((mds, me), rt) ->
                    (term_as_nblock me mds extra_vars
                        bigsum_vars_and_rels map_bindings arg_bindings c_t,
                    ((mds, me), rt)))
                valid_l block_maps
            in
            let (fn_blocks, accum_blocks) =
                List.partition (fun (b, _) -> is_functional b) sub_blocks
            in
            let fn_arith = block_as_arith
                (List.flatten (List.map fst fn_blocks))
                combine_arith_f
            in
            let update_arith = combine_f fn_arith accum_blocks in
            let init_and_accum_blocks =
                (* Temporary map initialization *)
                List.map (fun (b, ((ds,e),t)) -> 
                    let init_val = AM.PTVal(A.AMVal(A.AMConst(mk_zero t))) in
                    let init_mu = I.MapSet(ds,e,init_val) in
                    let init = [I.Assign([I.MapAssign([init_mu])])] in
                        init@b)
                    accum_blocks
            in
            let decls =
                (* Note: defer declaration if no accumulation, since this
                 * is a purely functional update which may be simplified *)
                if accum_blocks = [] then []
                else
                    (List.map (fun (_, (dse,_)) -> dse) accum_blocks)@
                        [(map_ds, map_entry)]
            in
            (* TODO: try to collapse single accumulation blocks *)
            let mu = I.MapUpdate(map_ds, map_entry, update_arith) in
                add_declarations decls;
                (List.flatten init_and_accum_blocks)@
                    [I.Assign([I.MapAssign([mu])])]
        in
        let combine_sum fn_arith accum_blocks =
            let accum_block_map_entries =
                List.map (fun (_, ((ds,e),_)) ->
                    AM.PProduct([A.AMVal(A.AMMapEntry(e))]))
                    accum_blocks
            in
                match fn_arith with
                    | AM.PSum(x) -> AM.PSum(x@accum_block_map_entries)
                    | _ -> raise (BCGenException ("Invalid partial sum."))
        in
        let combine_product fn_arith accum_blocks =
            let accum_block_map_entries =
                List.map (fun (_, ((ds,e),_)) ->
                    A.AMVal(A.AMMapEntry(e))) accum_blocks
            in
                match fn_arith with
                    | AM.PTProduct(AM.PProduct(x)) ->
                          AM.PTProduct(AM.PProduct(x@accum_block_map_entries))
                    | _ -> raise (BCGenException ("Invalid partial product."))
        in

        (*
        print_endline ("term_as_nblock input: "^
            (term_as_string (make_term t) []));
        *)

        begin match t with
            | RSum(l) ->
                  list_f l ensure_prod_or_lf combine_arith_sum combine_sum
                      
            | RProd(l) ->
                  list_f l ensure_lf combine_arith_product combine_product

            | RVal(lf) -> term_lf_as_nblock map_entry map_ds
                  extra_vars bigsum_vars_and_rels map_bindings arg_bindings lf
        end

    let term_as_bytecode
            (mapn: string)
            (t : readable_term_t)
            (params: var_t list)
            (loop_vars: var_t list)
            (bigsum_vars_and_rels: (var_t * var_t * readable_relalg_lf_t) list)
            (map_bindings: term_map_bindings)
            (arg_bindings: var_bindings) : I.nested_assign_t list
            =
        print_endline ("term_as_bytecode input: "^
            (term_as_string (make_term t) []));
        print_endline ("term_as_bytecode params: "^
            (String.concat "," (List.map fst params)));
        print_endline ("term_as_bytecode loop_vars: "^
            (String.concat "," (List.map fst loop_vars)));
        
        let extra_vars = Util.ListAsSet.multiunion
            [loop_vars;
            (List.map (fun (x,_,_) -> x) bigsum_vars_and_rels);
            (List.map fst arg_bindings)]
        in
        let (map_keys, key_types) = List.split (List.map
            (fun ((n,t) as x) -> (A.MKVar(x), t)) params)
        in
        let new_map_keys =
            List.map (replace_with_args arg_bindings) map_keys in
        let t_type = term_type t extra_vars in
        let map_entry = (mapn, new_map_keys) in
        let map_ds = A.Map(key_types, t_type) in
        let term_block = term_as_nblock map_entry map_ds
            extra_vars bigsum_vars_and_rels map_bindings arg_bindings t
        in

        (* Ensure declaration of argument map and bigsum var domains
         * TODO: since bigsum_vars_and_rels is a global list of all bigsum
         * vars, we try to declare these datastructures for each call to
         * term_as_bytecode. This is inefficient, but not significant.*)
        let decls = [(mapn, map_ds)]@
            (List.map (fun (bsv,_,_) ->
                ((fst bsv)^bigsum_ds_suffix, A.Multiset([snd bsv])))
                bigsum_vars_and_rels)
        in
            List.iter (fun (n,d) -> add_declaration n d) decls;
            let r =
                if loop_vars <> [] then
                    [I.ForEach(map_ds, mapn, loop_vars, term_block)]
                else
                    term_block
            in
                print_endline ("term_as_bytecode result: "^(I.bytecode_as_string r));
                r

    (* string -> var_t -> var_t -> string -> handler_blocks *)
    let generate_bigsum_maintenance
            (event_type: string) (bigsum_var: var_t)
            (orig_var: var_t) (rel_n: string)
            =
        let ds = A.Multiset([snd bigsum_var]) in
        let ds_name = (fst bigsum_var)^bigsum_ds_suffix in
        let mi = match event_type with
            | "insert" -> I.MultisetInsert(
                  ds, ds_name, [orig_var], R.PTVal(A.ConstantNullarySingleton))

            | "delete" -> I.MultisetDelete(
                  ds, ds_name, [orig_var], R.PTVal(A.ConstantNullarySingleton))

            | _ -> raise (BCGenException ("Invalid event type: "^event_type))
        in
        let r_block = (ds_name, [], [I.Assign([I.RelAssign([mi])])]) in
            [(rel_n, [BS.add r_block BS.empty])]
            
end
