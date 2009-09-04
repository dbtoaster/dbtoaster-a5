open Algebra

(* type computation rules *)
exception InvalidTypeOperation

type r_type = [ `Int | `Long | `Float | `Double | `String | `Unknown]

let string_of_type t = 
    match t with 
	| `Int -> "int"
	| `Long -> "long"
	| `Float -> "float"
	| `Double -> "double"
	| `String -> "string"
        | `Unknown -> "unknown"

(* basic type inferrence rules. It doesn't have to be strict type checker 
 * because we're generating C++ file.  *)
let type_op_rule l r =
    match (l, r) with
        | (`Unknown, `Unknown) -> `Unknown
        | (`Unknown, _) -> r
        | (_, `Unknown) -> l
	| (`Int, `Int) -> `Int
	| (`Int, `Long) | (`Long, `Int) | (`Long, `Long) -> `Long
	| (`Int, `Float) | (`Float, `Int) | (`Long, `Float) 
        | (`Float, `Long) | (`Float, `Float) -> `Float
        | (`Int, `Double) | (`Double, `Int) | (`Long, `Double)
        | (`Double, `Long) | (`Float, `Double) | (`Double, `Float)
        | (`Double, `Double) -> `Double
        | (`String, `String) -> `String
	| (_, _) -> 
	    (print_endline ("Type operation between "^(string_of_type l)^" and "^(string_of_type r));
	    raise InvalidTypeOperation)

let triple_type_op_rule p l r =
    type_op_rule p (type_op_rule l r)

let string_to_type s =
    match s with
	| "int" -> `Int
	| "long" -> `Long
	| "float" -> `Float
        | "double" -> `Double
	| "string" -> `String
        | "unknown" -> `Unknown
	| _ -> print_endline ("Invalid type "^s);
              raise InvalidTypeOperation

let type_to_string t = 
    match t with
	| `Int -> "int"
	| `Long -> "long"
	| `Float -> "float"
	| `Double -> "double"
	| `String -> "string"
        | `Unknown -> "unknown" 

(* generates type list from m_expr *)
let generate_type_list m_expr_l base_rels events = 
    let all_fields = 
        (List.fold_left 
            ( fun acc x ->   
                match x with
                    | `Relation (n, l) -> acc@l
                    | _ -> raise InvalidExpression)
            [] base_rels)@
        (List.fold_left
            ( fun acc x ->
                match x with 
                    | `Insert (n, l) -> acc@l
                    | _ -> acc)
            [] events)
    in
    let update fields ptype n = 
        if List.mem_assoc n fields then 
            let ntype = type_op_rule ptype (string_to_type (List.assoc n fields)) in
                (ntype, fields)
        else 
            match ptype with
                | `Unknown -> (`Unknown, fields)
                | _ -> (ptype, (n, type_to_string ptype)::fields)
    in
    let rec gtl_aux_me m_expr ptype fields = 
        match m_expr with
            | `METerm (`Int _) -> (type_op_rule ptype `Int, fields)
            | `METerm (`Float _) -> (type_op_rule ptype `Float, fields)
            | `METerm (`String _) -> (type_op_rule ptype `String, fields)
            | `METerm (`Long _) -> (type_op_rule ptype `Long, fields)
            | `METerm (`Variable n)
            | `METerm (`Attribute(`Qualified (_, n)))
            | `METerm (`Attribute(`Unqualified (n))) ->
                  update fields ptype n 
            | `Sum (l, r) | `Minus (l, r) | `Product (l, r) 
            | `Min (l, r) | `Max (l, r) ->
                  let (ltype, lfields) = gtl_aux_me l ptype fields in
                      begin
                          match ltype with 
                              | `Unknown ->
                                    let (rtype, rfields) = gtl_aux_me r ptype fields in
                                    let (ntype, nfields) = gtl_aux_me l rtype rfields in
                                        (triple_type_op_rule ptype ntype rtype, nfields)
                              | _ -> 
                                    let (ntype, nfields) = gtl_aux_me r ltype lfields in
                                        (triple_type_op_rule ptype ltype ntype, nfields)                    
                      end
            | `MapAggregate (_, m, p) -> 
                  let (mtype, mfields) = gtl_aux_me m ptype fields in
                      begin
                          match mtype with
                              | `Unknown ->
                                    let pfields = gtl_aux_p p fields in
                                        gtl_aux_me m ptype pfields
                              | _ -> (mtype, gtl_aux_p p mfields )
                      end
            | _ ->
                  print_endline ("gtl_aux_m_expr");
                  raise InvalidExpression
    and gtl_aux_p plan fields =
        match plan with
            | `Select (bp, p) -> gtl_aux_p p (gtl_aux_b bp fields)
            | `Project (_, p) -> gtl_aux_p p fields
            | `Union (pl) -> 
                  List.fold_left 
                      (fun f x -> gtl_aux_p x f) fields pl
            | `Cross (l, r) ->
                  gtl_aux_p r (gtl_aux_p l fields) 
	    | `Relation _ -> fields
            | _ -> 
                  print_endline ("gtl_aux_p "^string_of_plan plan);
                  raise InvalidExpression
    and gtl_aux_b bp fields = 
        match bp with
            | `And (l, r) | `Or (l, r) ->
                  gtl_aux_b l (gtl_aux_b r fields)
            | `Not b -> gtl_aux_b b fields
            | `BTerm (bt) -> gtl_aux_bterm bt fields
    and gtl_aux_bterm bt fields = 
        match bt with
            | `True | `False -> fields
            | `LT(l, r) | `LE(l, r) | `GT(l, r) | `GE(l, r) 
            | `EQ(l, r) | `NE(l, r) ->
                  let (ltype, lfields) = gtl_aux_e l `Unknown fields in
                      begin
                      match ltype with
                          | `Unknown -> 
                                let (rtype, rfields) = gtl_aux_e r `Unknown fields in
                                let (_, ret_fields) = gtl_aux_e l rtype rfields in
                                    ret_fields 
                          | _ -> 
                                let (_, ret_fields) = gtl_aux_e r ltype lfields in
                                    ret_fields
                      end
            | `MEQ(m) | `MLT(m) | `MLE(m) | `MGT(m)
            | `MGE(m) | `MNEQ(m) ->
                  let (_, ret_fields) = gtl_aux_me m `Unknown fields in
                      ret_fields 
    and gtl_aux_e e ptype fields = 
        match e with
            | `ETerm (`Int _) -> (type_op_rule ptype `Int, fields)
            | `ETerm (`Float _) -> (type_op_rule ptype `Float, fields)
            | `ETerm (`String _) -> (type_op_rule ptype `String, fields)
            | `ETerm (`Long _) -> (type_op_rule ptype `Long, fields)
            | `ETerm (`Variable n) 
            | `ETerm (`Attribute(`Qualified(_, n))) 
            | `ETerm (`Attribute(`Unqualified (n))) -> 
                  update fields ptype n
            | `UnaryMinus(ue) -> gtl_aux_e ue ptype fields
            | `Sum (l, r) | `Product (l, r) 
            | `Minus (l, r) | `Divide (l, r) ->
                 let (ltype, lfields) = gtl_aux_e l ptype fields in
                     begin
                         match ltype with
                             | `Unknown ->
                                   let (rtype, rfields) = gtl_aux_e r ptype fields in
                                   let (ntype, nfields) = gtl_aux_e l rtype rfields in
                                       (triple_type_op_rule ptype rtype ntype, nfields)
                             | _ ->
                                   let (ntype, nfields) = gtl_aux_e r ltype lfields in
                                       (triple_type_op_rule ptype ltype ntype, nfields)
                     end
            | _ -> 
                  print_endline "gtl_aux_e";
                  raise InvalidExpression
    in 
    let new_fields = 
        List.fold_left 
            ( fun fl me -> 
                let (_, nf) = gtl_aux_me me `Unknown fl in
                let i_uba = List.map field_of_attribute_identifier 
                    (get_unbound_attributes_from_map_expression me true)
                in 
                let testlist = List.filter (fun x -> List.mem_assoc x nf) i_uba
                in
                    if List.length i_uba != List.length testlist then
                        (print_endline ("generates type_list: unresolved type");
                        raise InvalidTypeOperation);
                    nf)
            all_fields m_expr_l
    in            
(*    let (new_type, new_fields) = gtl_aux_me m_expr `Unknown all_fields 
    in  
    let testlist = List.filter (fun x -> List.mem_assoc x new_fields) i_uba
    in 
        if List.length i_uba != List.length testlist then
            (print_endline ("TYPEINF: something wrong");
            raise InvalidTypeOperation)
        else
            print_endline ("Result type is "^type_to_string new_type); *)
        List.iter (fun (f, t) -> print_endline (f^":"^t)) new_fields;
        new_fields
 
(* looks for vname in type_list which must be there *)
let search_type_list vname type_list =
    if List.mem_assoc vname type_list then
        string_to_type (List.assoc vname type_list)
    else 
        (print_endline (vname^" is not on type_list");
        raise InvalidTypeOperation)

(* try to find type from declaration list first *)
let search_decls_type_list vname type_list decls =
    (* search through decl list *)
    let d_list = List.filter (
        fun x -> match x with
	    | `Variable (v, _) | `Map (v, _, _) -> vname = v
	    | _ -> false)
	decls
    in if List.length d_list != 0 then
	match List.hd d_list with
	    | `Variable ( _, t) | `Map ( _, _, t) -> string_to_type t
	    | _ -> raise InvalidExpression
    (* search through map list *)
    else search_type_list vname type_list 

let type_inf_expr expr type_list decls =
    let rec tie_aux_expr expr= 
    	match expr with
            | `ETerm e -> tie_aux_eterm e 
 	    | `Sum (l, r) | `Product (l, r) | `Minus (l, r) -> 
    	        type_op_rule (tie_aux_expr l) (tie_aux_expr r)
	    | `Function _ | `UnaryMinus _ | `Divide _ -> 
                print_endline "type_inf_expr wrong expression";
                raise InvalidExpression
    and tie_aux_eterm eterm =
        match eterm with
    	    | `Int _ -> `Int
	    | `Float _ -> `Float
	    | `String _ -> `String
	    | `Long _ -> `Long
	    | `Variable var ->
	        search_decls_type_list var type_list decls
	    | `Attribute (`Qualified (_, a)) 
	    | `Attribute (`Unqualified (a)) ->
	        search_type_list a type_list
    in
    let ret_type =  tie_aux_expr expr    
    in 
(*    print_endline ("result of type inferencing of "^string_of_expression expr^" : "^(string_of_type ret_type));*)
    type_to_string ret_type

let type_inf_mexpr m_expr type_list decls =
    let rec tiaux_mterm mterm : r_type= 
        match mterm with
	    | `Int _ -> `Int
	    | `Float _ -> `Float
	    | `String _ -> `String
	    | `Long _ -> `Long
	    | `Variable var -> 
		search_decls_type_list var type_list decls 
	    | `Attribute ( `Qualified (r, a)) -> 
		search_type_list a type_list 
	    | `Attribute ( `Unqualified (a)) ->
		search_type_list a type_list
    and tiaux_m_expr m_expr =
	match m_expr with
	    | `Sum (l, r) | `Minus (l, r) | `Product (l, r) | `Min ( l, r) | `Max ( l, r) 
		-> type_op_rule (tiaux_m_expr l) (tiaux_m_expr r)
	    | `METerm m | `Delete(_, m) -> tiaux_mterm m 
	    | `Delta _ | `New _ -> raise InvalidExpression (* these will not be remaining *)
	    | `MaintainMap (_, _, _, m) | `Incr ( _, _, _, _, m) | `IncrDiff (_, _, _, _, m)
	    | `Insert (_, _, m) | `Update( _, _, _, m) 
	    | `MapAggregate (_, m, _) -> tiaux_m_expr m
	    | `IfThenElse(_, m1, m2) -> 
		let l = tiaux_m_expr m1 in
		let r = tiaux_m_expr m2 in
		if l = r then l
		else (print_endline "typeop ifthenelse"; raise InvalidTypeOperation)
    in
    let ret_type =  tiaux_m_expr m_expr
    in 
(*    print_endline ("result of type inferencing of "^string_of_map_expression m_expr^" : "^(string_of_type ret_type));*)
    type_to_string ret_type

(* retrieve map expression from map list using hash value and id *)
let get_m_expr_with_mapid id maps map_vars =
    let maps_and_hvs = List.combine maps (List.map dbt_hash maps) in
    let hvs = List.map (fun (hv, _ ) -> hv)
	(List.filter ( fun (hv, var) -> var = id) map_vars) in
    if List.length hvs != 0 then
	let me = List.map ( fun (m,_) -> m)
	    (List.filter (fun (_, hv) -> hv = List.hd hvs) maps_and_hvs)
	in [List.hd (me)]
    else []

let type_inf_mexpr_map m_expr type_list maps map_vars =
    let rec tiaux_mterm mterm = 
        match mterm with
	    | `Int _ -> `Int
	    | `Float _ -> `Float
	    | `String _ -> `String
	    | `Long _ -> `Long
	    | `Variable v -> 
		let new_mexpr = get_m_expr_with_mapid v maps map_vars in
		if List.length new_mexpr != 0 then
		    tiaux_m_expr (List.hd new_mexpr)
		else 
		    search_type_list v type_list
	    | `Attribute ( `Qualified (_, a)) 
	    | `Attribute ( `Unqualified (a)) ->
		    search_type_list a type_list
    and tiaux_m_expr m_expr =
	match m_expr with
	    | `Sum (l, r) | `Minus (l, r) | `Product (l, r) | `Min ( l, r) | `Max ( l, r) 
		-> type_op_rule (tiaux_m_expr l) (tiaux_m_expr r)
	    | `METerm m | `Delete(_, m) -> tiaux_mterm m 
	    | `Delta _ | `New _ -> raise InvalidExpression (* these will not be remaining *)
	    | `MaintainMap (_, _, _, m) | `Incr ( _, _, _, _, m) | `IncrDiff (_, _, _, _, m)
	    | `Insert (_, _, m) | `Update( _, _, _, m) 
	    | `MapAggregate (_, m, _) -> tiaux_m_expr m
	    | `IfThenElse(_, m1, m2) -> 
		let l = tiaux_m_expr m1 in
		let r = tiaux_m_expr m2 in
		if l = r then l
		else raise InvalidTypeOperation
    in
    let ret_type =  tiaux_m_expr m_expr
    in 
(*    print_endline ("result of type inferencing of "^string_of_map_expression m_expr^" : "^(string_of_type ret_type)); *)
    type_to_string ret_type

let type_inf_arith_expr a_expr decls =
    let rec tiae_aux_expr a_expr = 
  	match a_expr with
	    | `Sum ( l, r) | `Minus (l, r) | `Product (l, r) | `Min (l, r) | `Max (l, r) ->
		type_op_rule (tiae_aux_expr l) (tiae_aux_expr r)
	    | `CTerm c ->
		tiae_aux_cterm c
    and tiae_aux_cterm cterm = 
	match cterm with
	    | `Int _ -> `Int
	    | `Float _ -> `Float
	    | `String _ -> `String
	    | `Long _ -> `Long
	    | `Variable id   
	    | `MapAccess (id, _) | `MapContains (id, _) 
	    | `MapIterator (`Begin id) | `MapIterator (`End id) -> 
		let t_list = List.map 
		    (fun x -> match x with
			| `Declare(`Variable (_, t)) | `Declare(`Map (_, _, t)) -> string_to_type t 
			| _ -> print_endline "type_inf_arith_expr wrong"; raise InvalidExpression)
			( List.filter 
		            (fun x -> match x with 
			        | `Declare(`Variable (n, _)) | `Declare(`Map (n, _, _)) -> n = id
			        | _ -> false (* check later *))
		            decls )
		in List.hd t_list
            | `DomainContains (did, keys) -> raise InvalidExpression
            | `DomainIterator (`Begin id) | `DomainIterator (`End id) -> raise InvalidExpression
    in 
	type_to_string (tiae_aux_expr a_expr)
