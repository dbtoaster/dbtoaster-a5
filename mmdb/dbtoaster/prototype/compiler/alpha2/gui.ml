open Algebra
open Xml

(*********************
 * TreeML writing
 *********************)

let base_declarations = [
    "<attributeDecl name=\"attribute_identifier\" type=\"String\"/>";
    "<attributeDecl name=\"variable_identifier\"  type=\"String\"/>";
    "<attributeDecl name=\"function_identifier\"  type=\"String\"/>";
    "<attributeDecl name=\"type_identifier\"      type=\"String\"/>";
    "<attributeDecl name=\"field_identifier\"     type=\"String\"/>";
    "<attributeDecl name=\"relation_identifier\"  type=\"String\"/>";
    "<attributeDecl name=\"state_identifier\"     type=\"String\"/>";
    "<attributeDecl name=\"delta\"                type=\"String\"/>";
    "<attributeDecl name=\"aggregate\"            type=\"String\"/>";
    "<attributeDecl name=\"oplus\"                type=\"String\"/>";
    "<attributeDecl name=\"poplus\"               type=\"String\"/>";
    "<attributeDecl name=\"eterm-type\"           type=\"String\"/>";
    "<attributeDecl name=\"eterm-val\"            type=\"String\"/>";
    "<attributeDecl name=\"meterm-type\"          type=\"String\"/>";
    "<attributeDecl name=\"meterm-val\"           type=\"String\"/>";
]

let indent s = "    "^s
let make_label s = match s with | "" -> s | _ -> (" label=\""^s^"\"")

let make_leaf ?(label = "") subtree =
        ["<leaf"^(make_label label)^">"]@(List.map indent subtree)@["</leaf>"]

let make_branch ?(label = "") subtree =
    ["<branch"^(make_label label)^">"]@(List.map indent subtree)@["</branch>"]

let make_tree subtree = ["<tree>"]@(List.map indent subtree)@["</tree>"]

let treeml_of_attribute_identifier aid =
    "<attribute name=\"attribute_identifier\" value=\""^
        (match aid with
            | `Qualified (n,c) -> "qualified,"^n^","^c
            | `Unqualified c -> "unqualified,"^c)^"\"/>"

let treeml_of_variable_identifier v =
    "<attribute name=\"variable_identifier\" value=\""^v^"\"/>"

let treeml_of_function_identifier f =
    "<attribute name=\"function_identifier\" value=\""^f^"\"/>"

let treeml_of_type_identifier t =
    "<attribute name=\"type_identifier\" value=\""^t^"\"/>"

let treeml_of_field_identifier f =
    "<attribute name=\"field_identifier\" value=\""^f^"\"/>"

let treeml_of_relation_identifier r =
    "<attribute name=\"relation_identifier\" value=\""^r^"\"/>"

let treeml_of_state_identifier s =
    "<attribute name=\"state_identifier\" value=\""^s^"\"/>"

let treeml_of_field (id, ty) =
    [(treeml_of_field_identifier id); 
    (treeml_of_type_identifier ty)]

(* field list -> string list *)
let treeml_of_field_list fl =
    List.flatten (List.map treeml_of_field fl)

(* attribute_identifier list -> string list *)
let treeml_of_domain d =
    make_leaf (List.map treeml_of_attribute_identifier d)

(* delta -> string list *)
let treeml_of_delta delta =
    let (dt, id, f) = 
        match delta with
            | `Insert (r,f) -> ("insert", r, f)
            | `Delete (r,f) -> ("delete", r, f)
    in
        ["<attribute name=\"delta\" value=\""^dt^","^id^"\"/>"]@
            (treeml_of_field_list f)

let treeml_of_aggregate_function agg =
    "<attribute name=\"aggregate\" value=\""^
	(match agg with | `Sum -> "sum" | `Min -> "min" | `Max -> "max" )^"\"/>"

(* TODO: what about the map expr and state id parts of Decrmin and Decrmax? *)
let treeml_of_oplus o = 
    "<attribute name=\"oplus\" value=\""^
	(match o with | `Plus -> "plus" | `Minus -> "minus"
            | `Min -> "min" | `Max -> "max"
            | `Decrmin _ -> "decrmin"
            | `Decrmax _ -> "decrmax")^"\"/>"

let treeml_of_poplus o = 
    "<attribute name=\"poplus\" value=\""^
	(match o with | `Union -> "union" | `Diff -> "diff" )^"\"/>"

(*
  eterm -> string list

  eterm_base_type = int | float | string | long
  eterm := <attr name="eterm-type" value=eterm_base_type/>,<attr name="eterm-val" value=data/>
  eterm := <attr name="eterm-type" value="attr"/>,attribute_identifier
  eterm := <attr name="eterm-type" value="var"/>,variable_identifier
*)

let treeml_of_eterm eterm =
    let typ_attr t = 
	"<attribute name=\"eterm-type\" value=\""^t^"\"/>"
    in
    let typ_val v =
	"<attribute name=\"eterm-val\" value=\""^v^"\"/>"
    in
    match eterm with
	| `Int i -> [ (typ_attr "int"); (typ_val (string_of_int i)) ]

	| `Float f -> [ (typ_attr "float"); (typ_val (string_of_float f)) ]

	| `String s -> [ (typ_attr "string"); (typ_val s) ]

	| `Long l -> [ (typ_attr "long"); (typ_val (Int64.to_string l)) ]

	| `Attribute id ->
	      [ (typ_attr "attr"); (treeml_of_attribute_identifier id) ]

	| `Variable v -> [ (typ_attr "var"); (treeml_of_variable_identifier v) ]

(*
  meterm -> string list

  meterm_base_type = int | float | string | long
  meterm := <attr name="meterm-type" value=meterm_base_type/>,<attr name="meterm-val" value=data/>
  meterm := <attr name="meterm-type" value="attr"/>,attribute_identifier
  meterm := <attr name="meterm-type" value="var"/>,variable_identifier
*)
let treeml_of_meterm meterm =
    let typ_attr t = 
	"<attribute name=\"meterm-type\" value=\""^t^"\"/>"
    in
    let typ_val v =
	"<attribute name=\"meterm-val\" value=\""^v^"\"/>"
    in
    match meterm with
	| `Int i -> [ (typ_attr "int"); (typ_val (string_of_int i)) ]

	| `Float f -> [ (typ_attr "float"); (typ_val (string_of_float f)) ]

	| `String s -> [ (typ_attr "string"); (typ_val s) ]

	| `Long l -> [ (typ_attr "long"); (typ_val (Int64.to_string l)) ]

	| `Attribute id ->
	      [ (typ_attr "attr"); (treeml_of_attribute_identifier id) ]

	| `Variable v -> [ (typ_attr "var"); (treeml_of_variable_identifier v) ]


(*
 * Viz TreeML format
 *
 *)

let rec viz_treeml_of_expression expr indent_fn = 
    let binary_expr typ l r =
	[ "<branch>"; (indent_fn ("<attribute name=\"op\" value=\""^typ^"\"/>"))]@
	(List.map indent_fn (viz_treeml_of_expression l indent_fn))@
	(List.map indent_fn (viz_treeml_of_expression r indent_fn))@
        [ "</branch>" ]
    in
    match expr with
	| `ETerm et ->
	      [ "<leaf>"; (indent_fn "<attribute name=\"op\" value=\"eterm\"/>")]@
              (List.map indent_fn (treeml_of_eterm et))@["</leaf>"]

	| `UnaryMinus e ->
	      [ "<branch>"; (indent_fn "<attribute name=\"op\" value=\"-\"/>")]@
	      (List.map indent_fn (viz_treeml_of_expression e indent_fn))@["</branch>"]

	| `Sum(l,r) -> binary_expr "+" l r
	| `Product(l,r) -> binary_expr "*" l r
	| `Minus(l,r) -> binary_expr "-" l r
	| `Divide(l,r) -> binary_expr "/" l r
	| `Function(id, args) ->
	      ["<branch>"; 
	       (indent_fn "<attribute name=\"op\" value=\"function\"/>");
	       (indent_fn ("<attribute name=\"functionid\" value=\""^id^"\"/>"));]@
	       (List.map indent_fn (List.flatten
		   (List.map (fun ae -> viz_treeml_of_expression ae indent_fn) args)))@
	      [ "</branch>" ]

let rec viz_treeml_of_bterm bterm indent_fn =
    let map_expr op e =
	["<attribute name=\"op\" value=\""^op^"\"/>"]@
        (viz_treeml_of_map_expression e indent_fn)
    in
    let binary_expr op l r =
	["<attribute name=\"op\" value=\""^op^"\"/>"]@
        (viz_treeml_of_expression l indent_fn)@
	(viz_treeml_of_expression r indent_fn)
    in
    match bterm with
	| `LT (l,r) -> binary_expr "&lt;" l r
	| `LE (l,r) -> binary_expr "&lt;=" l r
	| `GT (l,r) -> binary_expr "&gt;" l r
	| `GE  (l,r) -> binary_expr "&gt;=" l r
	| `EQ  (l,r) -> binary_expr "=" l r
	| `NE  (l,r) -> binary_expr "!=" l r
	| `MEQ e -> map_expr "meq" e
	| `MNEQ e -> map_expr "mneq" e
	| `MLT e -> map_expr "m&lt;" e
	| `MLE e -> map_expr "m&lt;=" e
	| `MGT e -> map_expr "m&gt;" e
	| `MGE e -> map_expr "m&gt;=" e

and viz_treeml_of_bool_expression b_expr indent_fn =
    match b_expr with
	| `BTerm bt ->
	      ["<branch>"; (indent_fn "<attribute name=\"op\" value=\"bterm\"/>")]@
		  (List.map indent_fn (viz_treeml_of_bterm bt indent_fn))@["</branch>"]

	| `Not be ->
	      ["<branch>"; (indent_fn "<attribute name=\"op\" value=\"not\"/>")]@
		  (List.map indent_fn (viz_treeml_of_bool_expression be indent_fn))@["</branch>"]

	| `And (l,r) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"and\"/>")]@
		  (List.map indent_fn (viz_treeml_of_bool_expression l indent_fn))@
		  (List.map indent_fn (viz_treeml_of_bool_expression r indent_fn))@["</branch>"]

	| `Or (l,r) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"or\"/>")]@
		  (List.map indent_fn (viz_treeml_of_bool_expression l indent_fn))@
		  (List.map indent_fn (viz_treeml_of_bool_expression r indent_fn))@["</branch>"]

and viz_treeml_of_map_expression m_expr indent_fn = 
    let binary_node typ l r =
	["<branch>";
	    (indent_fn ("<attribute name=\"op\" value=\""^typ^"\"/>"))]@
	    (List.map indent_fn (viz_treeml_of_map_expression l indent_fn))@
	    (List.map indent_fn (viz_treeml_of_map_expression r indent_fn))@["</branch>"]
    in
    match m_expr with
	| `METerm x ->
	      ["<leaf>";
		  (indent_fn "<attribute name=\"op\" value=\"meterm\"/>")]@
		  (List.map indent_fn (treeml_of_meterm x))@["</leaf>"]

	| `Sum(l,r) -> binary_node "m+" l r
	| `Minus(l,r) -> binary_node "m-" l r
	| `Product(l,r) -> binary_node "m*" l r
	| `Min(l,r) -> binary_node "min" l r
	| `Max(l,r) -> binary_node "max" l r
	| `MapAggregate(agg, f, q) ->
	      let agg_fn = match agg with | `Sum -> "sum" | `Min -> "min" | `Max -> "max" in
	      ["<branch>";
		  (indent_fn ("<attribute name=\"op\" value=\""^agg_fn^"\"/>"));
		  (indent_fn (treeml_of_aggregate_function agg))]@
		  (List.map indent_fn (viz_treeml_of_map_expression f indent_fn))@
		  (List.map indent_fn (viz_treeml_of_plan q indent_fn))@
		  ["</branch>"]

	| `Delta (e, f) ->
	      ["<branch>";
	      (indent_fn "<attribute name=\"op\" value=\"delta\"/>")]@
              (List.map indent_fn (treeml_of_delta e))@
              (List.map indent_fn (viz_treeml_of_map_expression f indent_fn))@["</branch>"]

	| `New f ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"new\"/>")]@
		  (List.map indent_fn (viz_treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Incr (s, o, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"incr\"/>");
		  (indent_fn (treeml_of_state_identifier s));
		  (indent_fn (treeml_of_oplus o))]@
		  (List.map indent_fn (viz_treeml_of_map_expression f indent_fn))@["</branch>"]

	| `IncrDiff (s, o, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"incrdiff\"/>");
		  (indent_fn (treeml_of_state_identifier s));
		  (indent_fn (treeml_of_oplus o))]@
		  (List.map indent_fn (viz_treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Init (s, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"init\"/>");
		  (indent_fn (treeml_of_state_identifier s))]@
		  (List.map indent_fn (viz_treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Insert (s, m, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"insert\"/>");
		  (indent_fn (treeml_of_state_identifier s))]@
		  (List.map indent_fn (treeml_of_meterm m))@
		  (List.map indent_fn (viz_treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Update (s, o, m, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"insert\"/>");
		  (indent_fn (treeml_of_state_identifier s));
		  (indent_fn (treeml_of_oplus o))]@
		  (List.map indent_fn (treeml_of_meterm m))@
		  (List.map indent_fn (viz_treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Delete (sid, x) ->
	      ["<leaf>";
		  (indent_fn "<attribute name=\"op\" value=\"delete\"/>");
		  (indent_fn (treeml_of_state_identifier sid))]@
		  (List.map indent_fn (treeml_of_meterm x))@["</leaf>"]

	| `IfThenElse(b,l,r) -> 
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"ifthenelse\"/>")]@
		  (List.map indent_fn (viz_treeml_of_bterm b indent_fn))@
	          (List.map indent_fn (viz_treeml_of_map_expression l indent_fn))@
	          (List.map indent_fn (viz_treeml_of_map_expression r indent_fn))@["</branch>"]


and viz_treeml_of_plan p indent_fn = 
    let viz_treeml_of_relation rtyp r f =
	["<leaf>";
	    (indent_fn ("<attribute name=\"op\" value=\""^rtyp^"\"/>"));
	    (indent_fn ("<attribute name=\"rid\" value=\""^r^"\"/>"))]@
	    (List.map (fun (id,typ) ->
		(indent_fn ("<attribute name=\"field\" value=\""^id^","^typ^"\"/>")))
		 f)@["</leaf>"]
    in
    let viz_treeml_of_projections projs =
	List.map
	    (fun (oid, expr) ->
		 "<attribute name=\"projection\" value=\""^
		     (string_of_attribute_identifier oid)^","^
		     (string_of_expression expr)^"\"/>")
	    projs
    in
    match p with
	| `TrueRelation ->
              ["<leaf>";
              (indent_fn "<attribute name=\"op\" value=\"truerelation\">");
              "</leaf>"]

	| `FalseRelation ->
              ["<leaf>";
              (indent_fn "<attribute name=\"op\" value=\"falserelation\">");
              "</leaf>"]

	| `Relation (r,f) -> viz_treeml_of_relation "relation" r f

	| `Select (pred, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"select\"/>")]@
		  (List.map indent_fn (viz_treeml_of_bool_expression pred indent_fn))@
		  (List.map indent_fn (viz_treeml_of_plan c indent_fn))@["</branch>"]

	| `Project (projs, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"project\"/>")]@
		  (List.map indent_fn (viz_treeml_of_projections projs))@
		  (List.map indent_fn (viz_treeml_of_plan c indent_fn))@["</branch>"]

	| `Union ch ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"union\"/>")]@
		  (List.map indent_fn (List.flatten
		      (List.map (fun c -> viz_treeml_of_plan c indent_fn) ch)))@
		  ["</branch>"]

	| `Cross(l,r) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"cross\"/>")]@
		  (List.map indent_fn (viz_treeml_of_plan l indent_fn))@
		  (List.map indent_fn (viz_treeml_of_plan r indent_fn))@["</branch>"]

	| `DeltaPlan (e, c) ->
	      ["<branch>";
	      (indent_fn "<attribute name=\"op\" value=\"delta\"/>");]@
              (List.map indent_fn (treeml_of_delta e))@
              (List.map indent_fn (viz_treeml_of_plan c indent_fn))@["</branch>"]
	      
	| `NewPlan (c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"newplan\"/>")]@
		  (List.map indent_fn (viz_treeml_of_plan c indent_fn))@["</branch>"]

	| `IncrPlan (sid, p, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"incrplan\"/>");
		  (indent_fn (treeml_of_state_identifier sid));
		  (indent_fn (treeml_of_poplus p))]@
		  (List.map indent_fn (viz_treeml_of_plan c indent_fn))@["</branch>"]

	| `IncrDiffPlan (sid, p, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"incrdiffplan\"/>");
		  (indent_fn (treeml_of_state_identifier sid));
		  (indent_fn (treeml_of_poplus p))]@
		  (List.map indent_fn (viz_treeml_of_plan c indent_fn))@["</branch>"]


(*
 * Top level viz I/O function
 *)
let viz_declarations = base_declarations@[
    "<attributeDecl name=\"rid\" type=\"String\"/>";
    "<attributeDecl name=\"field\" type=\"String\"/>";
    "<attributeDecl name=\"projection\" type=\"String\"/>";
    "<attributeDecl name=\"op\" type=\"String\"/>";
]

let viz_treeml_string_of_map_expression m_expr =
    let indent s = "    "^s in
    let delim l = String.concat "\n" l in
    let declarations =
	(["<declarations>"]@
        (List.map indent viz_declarations)@
        [ "</declarations>" ])
    in
	(delim
            (make_tree
                (declarations@
		    (List.map indent
                        (viz_treeml_of_map_expression m_expr indent)))))



(*
 * I/O TreeML format
 *
 *)

(*
  expression -> (string -> string) -> string list

  expr_node = uminus | sum | product | minus | divide | function
  expression := <branch><attr name="expression" value=expr_node/>(expression)+</branch>
  expression := <leaf><attr name="expression" value="eterm"/>eterm</leaf>
  expression := <leaf><attr name="expression" value="None"/></leaf>
*)

let rec treeml_of_expression expr = 
    let make_expr typ = "<attribute name=\"expression\" value=\""^typ^"\"/>" in
    let tml_expr e = treeml_of_expression e in
    let binary_expr typ l r =
        make_branch ([make_expr typ]@(tml_expr l)@(tml_expr r))
    in
    match expr with
	| `ETerm et ->
              make_branch
                  ([make_expr "eterm"]@
                  (make_leaf (treeml_of_eterm et)))

	| `UnaryMinus e -> make_branch ([make_expr "uminus"]@(tml_expr e))
	| `Sum(l,r) -> binary_expr "sum" l r
	| `Product(l,r) -> binary_expr "product" l r
	| `Minus(l,r) -> binary_expr "minus" l r
	| `Divide(l,r) -> binary_expr "divide" l r

	| `Function(id, args) ->
              make_branch
                  ([make_expr "function";
                  (treeml_of_function_identifier id)]@
                  (List.flatten (List.map tml_expr args)))

(* expression option -> (string -> string) -> string list *)
let treeml_of_expression_option e_opt =
    match e_opt with
        | None -> make_leaf (["<attribute name=\"expression\" value=\"none\"/>"])
        | Some e -> treeml_of_expression e

(* (attribute_identifier * expression option) list -> (string -> string) -> string list *)
let treeml_of_bindings bindings =
    match bindings with
        | [] -> make_leaf ~label:"emptybindings" []
        | _ ->
              make_branch (List.flatten
                  (List.map
                      (fun (a,e_opt) ->
                          [treeml_of_attribute_identifier a]@
                          (treeml_of_expression_option e_opt))
                      bindings))


(*
  bterm -> (string -> string) -> string list

  bterm_expr_node = &lt; | &lt;= | &gt; | &gt;= | = | !=
  bterm_expr_node = m&lt; | m&lt;= | m&gt; | m&gt;= | m= | m!=
  bterm := <branch><attr name="bterm" value=bterm_expr_node/>expression,expression</branch>
  bterm := <branch><attr name="bterm" value=bterm_mexpr_node/>map_expression</branch>
*)

let rec treeml_of_bterm bterm =
    let make_bterm typ = "<attribute name=\"bterm\" value=\""^typ^"\"/>" in
    let tml_expr e = treeml_of_expression e in
    let tml_map_expr e = treeml_of_map_expression e in
    let map_expr op e = make_branch ([make_bterm op]@(tml_map_expr e)) in
    let binary_expr op l r =
        make_branch ([make_bterm op]@(tml_expr l)@(tml_expr r))
    in
    match bterm with
        | `True -> make_leaf [make_bterm "true"]
        | `False -> make_leaf [make_bterm "false"]
	| `LT (l,r) -> binary_expr "lt" l r
	| `LE (l,r) -> binary_expr "le" l r
	| `GT (l,r) -> binary_expr "gt" l r
	| `GE  (l,r) -> binary_expr "ge" l r
	| `EQ  (l,r) -> binary_expr "eq" l r
	| `NE  (l,r) -> binary_expr "neq" l r
	| `MEQ e -> map_expr "meq" e
	| `MNEQ e -> map_expr "mneq" e
	| `MLT e -> map_expr "mlt" e
	| `MLE e -> map_expr "mle" e
	| `MGT e -> map_expr "mgt" e
	| `MGE e -> map_expr "mge" e

(*
  boolean_expression -> (string -> string) -> string list

  bool_expr_node = not | and | or
  bool_expr := <branch><attr name="boolexpression" value="bterm">bterm</branch>
  bool_expr := <branch><attr name="boolexpression" value=bool_expr_node>(bool_expr)+</branch>
*)

and treeml_of_bool_expression b_expr =
    let make_bexpr typ = "<attribute name=\"boolexpression\" value=\""^typ^"\"/>" in
    let tml_bexpr be = treeml_of_bool_expression be
    in
    match b_expr with
	| `BTerm bt ->
              make_branch
                  ([make_bexpr "bterm"]@
                  (treeml_of_bterm bt))

	| `Not be -> make_branch ([make_bexpr "not"]@(tml_bexpr be))
	| `And (l,r) -> make_branch ([make_bexpr "and"]@(tml_bexpr l)@(tml_bexpr r))
	| `Or (l,r) -> make_branch ([make_bexpr "or"]@(tml_bexpr l)@(tml_bexpr r))


(*
  map_expression -> (string -> string) -> string list

  binary_map_expr_node = sum | minus | product | min | max
  incr_map_expr_node = incr | incrdiff

  map_expr := <leaf><attr name="mapexpression" value="meterm"/>meterm</leaf>

  map_expr := <branch><attr name="mapexpression" value=binary_map_expr_node/>map_expr,map_expr</branch>

  map_expr := <branch><attr name="mapexpression" value="aggregate"/>map_expr,plan</branch>

  map_expr := <branch><attr name="mapexpression" value="delta"/>delta,map_expr</branch>
  map_expr := <branch><attr name="mapexpression" value="new"/>map_expr</branch>

  map_expr := <branch><attr name="mapexpression" value=incr_map_expr_node/>
                  state_identifier,oplus,map_expr,bindings,map_expr
              </branch>
  map_expr := <branch><attr name="mapexpression" value="init"/>state_identifier,bindings,map_expr</branch>

  map_expr := <branch><attr name="mapexpression" value="insert"/>state_identifier,meterm,map_expr</branch>
  map_expr := <branch><attr name="mapexpression" value="update"/>state_identifier,oplus,meterm,map_expr</branch>
  map_expr := <branch><attr name="mapexpression" value="delete"/>state_identifier,meterm</branch>

  map_expr := <branch><attr name="mapexpression" value="ifthenelse"/>bterm,map_expr,map_expr</branch>

*)

and treeml_of_map_expression m_expr =
    let make_mexpr typ = "<attribute name=\"mapexpression\" value=\""^typ^"\"/>" in
    let tml_map m_expr = treeml_of_map_expression m_expr
    in
    let tml_plan plan = treeml_of_plan plan in
    let binary_node typ l r = make_branch ([make_mexpr typ]@(tml_map l)@(tml_map r)) in
    match m_expr with
	| `METerm x ->
              make_branch
                  ([make_mexpr "meterm"]@
                  (make_leaf (treeml_of_meterm x)))

	| `Sum(l,r) -> binary_node "sum" l r
	| `Minus(l,r) -> binary_node "minus" l r
	| `Product(l,r) -> binary_node "product" l r
	| `Min(l,r) -> binary_node "min" l r
	| `Max(l,r) -> binary_node "max" l r

	| `MapAggregate(agg, f, q) ->
              make_branch
                  ([make_mexpr "mapaggregate";
	          (treeml_of_aggregate_function agg)]@
                  (tml_map f)@(tml_plan q))

	| `Delta (e, f) ->
              make_branch
                  ([make_mexpr "delta"]@
                      (make_branch (treeml_of_delta e))@
                      (tml_map f))

	| `New f -> make_branch ([make_mexpr "new"]@(tml_map f))

	| `Incr (s, o, re, bd, f) ->
              make_branch
                  ([make_mexpr "incr";
		  (treeml_of_state_identifier s);
		  (treeml_of_oplus o)]@
                  (tml_map re)@
                  (treeml_of_bindings bd)@
                  (tml_map f))

	| `IncrDiff (s, o, re, bd, f) ->
              make_branch
                  ([make_mexpr "incrdiff";
		  (treeml_of_state_identifier s);
		  (treeml_of_oplus o)]@
                  (tml_map re)@
                  (treeml_of_bindings bd)@
                  (tml_map f))

	| `Init (s, bd, f) ->
              make_branch
                  ([make_mexpr "init";
		  (treeml_of_state_identifier s)]@
                  (treeml_of_bindings bd)@
		  (tml_map f))

	| `Insert (s, m, f) ->
              make_branch
                  ([make_mexpr "insert";
		  (treeml_of_state_identifier s)]@
                  (make_leaf (treeml_of_meterm m))@
		  (tml_map f))

	| `Update (s, o, m, f) ->
              make_branch
                  ([make_mexpr "update";
		  (treeml_of_state_identifier s);
		  (treeml_of_oplus o)]@
		  (make_leaf (treeml_of_meterm m))@
		  (tml_map f))

	| `Delete (sid, x) ->
              make_branch
                  ([make_mexpr "delete";
		  (treeml_of_state_identifier sid)]@
		  (make_leaf (treeml_of_meterm x)))

	| `IfThenElse(b,l,r) -> 
              make_branch
                  ([make_mexpr "ifthenelse"]@
		  (treeml_of_bterm b)@
	          (tml_map l)@(tml_map r))

(*
  plan -> (string -> string) -> string list

  projections := (attribute_identifier,expression)+
*)

and treeml_of_plan p = 
    let make_plan typ = ["<attribute name=\"plan\" value=\""^typ^"\"/>"] in
    let tml_expr expr = treeml_of_expression expr in
    let tml_pred pred = treeml_of_bool_expression pred in
    let tml_plan p = treeml_of_plan p in

    let treeml_of_projections projections =
        (List.flatten
            (List.map
                (fun (a,e) ->
                    [treeml_of_attribute_identifier a]@
                    (tml_expr e))
                projections))
    in

    match p with
	| `TrueRelation -> make_leaf (make_plan "truerelation")

	| `FalseRelation -> make_leaf (make_plan "falserelation")

	| `Relation (r,f) -> 
              let subtree = 
	          (make_plan "relation")@
                  [treeml_of_relation_identifier r]@
                  (treeml_of_field_list f)
              in
                  make_leaf subtree

        | `Domain (sid, d) ->
              make_branch
                  ((make_plan "domain")@
                  [treeml_of_state_identifier sid]@
                  (treeml_of_domain d))

	| `Select (pred, c) ->
              let subtree = (make_plan "select")@(tml_pred pred)@(tml_plan c) in
                  make_branch subtree

	| `Project (projs, c) ->
              let subtree =
                  (make_plan "project")@
                  (make_branch (treeml_of_projections projs))@
                  (tml_plan c)
              in
                  make_branch subtree

	| `Union ch ->
              make_branch ((make_plan "union")@(List.flatten (List.map tml_plan ch)))

	| `Cross(l,r) ->
              make_branch ((make_plan "cross")@(tml_plan l)@(tml_plan r))

	| `DeltaPlan (e, c) ->
              make_branch ((make_plan "delta")@
                  (make_branch (treeml_of_delta e))@
                  (tml_plan c))
	      
	| `NewPlan (c) ->
              make_branch ((make_plan "new")@(tml_plan c))

	| `IncrPlan (sid, p, d, bd, c) ->
              make_branch
                  ((make_plan "incrplan")@
		      [(treeml_of_state_identifier sid);
		      (treeml_of_poplus p)]@
                      (treeml_of_domain d)@
                      (treeml_of_bindings bd)@
                      (tml_plan c))

	| `IncrDiffPlan (sid, p, d, bd, c) ->
              make_branch
                  ((make_plan "incrdiffplan")@
		      [(treeml_of_state_identifier sid);
		      (treeml_of_poplus p)]@
                      (treeml_of_domain d)@
                      (treeml_of_bindings bd)@
                      (tml_plan c))

(*
 * Top level I/O function
 *)
let io_declarations = base_declarations@[
    "<attributeDecl name=\"expression\"           type=\"String\"/>";
    "<attributeDecl name=\"bterm\"                type=\"String\"/>";
    "<attributeDecl name=\"boolexpression\"       type=\"String\"/>";
    "<attributeDecl name=\"mapexpression\"        type=\"String\"/>";
    "<attributeDecl name=\"plan\"                 type=\"String\"/>";
]

let treeml_string_of_map_expression m_expr =
    let delim l = String.concat "\n" l in
    let declarations =
	(["<declarations>"]@
        (List.map indent io_declarations)@
        [ "</declarations>" ])
    in
	(delim (make_tree (declarations@(treeml_of_map_expression m_expr))))


(*********************
 * TreeML reading
 *********************)

exception InvalidTreeML of string

let get_value tml name =
    if (tag tml) = "attribute" && (attrib tml "name" = name)
    then (attrib tml "value")
    else raise (InvalidTreeML ("node: "^(to_string tml)))

let get_attribute_identifier tml =
    let attr_data = get_value tml "attribute_identifier" in
    let (ty, f, n_opt) =
        let attr_fl = Str.split (Str.regexp ",") attr_data in
            match (List.length attr_fl) with
                | 2 -> (List.nth attr_fl 0, List.nth attr_fl 1, None)
                | 3 -> (List.nth attr_fl 0, List.nth attr_fl 2, Some(List.nth attr_fl 1))
                | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))
    in
        match (ty, n_opt) with
            | ("qualified", Some(n)) -> `Qualified(n,f)
            | ("unqualified", _) -> `Unqualified(f)
            | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

let get_variable_identifier tml = get_value tml "variable_identifier"
let get_function_identifier tml = get_value tml "function_identifier"
let get_type_identifier tml = get_value tml "type_identifier"
let get_field_identifier tml = get_value tml "field_identifier"
let get_relation_identifier tml = get_value tml "relation_identifier"
let get_state_identifier tml = get_value tml "state_identifier"

let get_field_list tml_l =
    let (_, labelled_tml) =
        List.fold_left
            (fun (even, tml_acc) tml ->
                (not even, tml_acc@[(even, tml)]))
            (true,[]) tml_l
    in
    let (tml_ids, tml_types) =
        let (x,y) = List.partition fst labelled_tml in
        let ((_,x2), (_,y2)) = (List.split x, List.split y) in
            (x2,y2)
    in
        List.map2
            (fun tml_id tml_ty ->
                let id = get_field_identifier tml_id in
                let ty = get_type_identifier tml_ty in
                    (id,ty))
            tml_ids tml_types

let get_domain tml =
    match (children tml) with
        | [] -> raise (InvalidTreeML ("node: "^(to_string tml)))
        | x -> List.map get_attribute_identifier x

let get_delta tml =
    match (children tml) with
        | h::t ->
              let fields = get_field_list t in
              let delta_data = get_value h "delta" in
              let (dt, id) =
                  let comma_idx = String.index delta_data ',' in
                      (String.sub delta_data 0 comma_idx,
                      String.sub delta_data (comma_idx+1)
                          ((String.length delta_data) - (comma_idx+1)))
              in
                  begin
                      match dt with
                          | "insert" -> `Insert(id,fields)
                          | "delete" -> `Delete(id,fields)
                          | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))
                      end

        | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

let get_aggregate_function tml =
    let agg_str = get_value tml "aggregate" in
        match agg_str with
            | "sum" -> `Sum
            | "min" -> `Min
            | "max" -> `Max
            | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

(* TODO: decrmin, decrmax inner map expressions *)
let get_oplus tml =
    let oplus_str = get_value tml "oplus" in
        match oplus_str with
            | "plus" -> `Plus
            | "minus" -> `Minus
            | "min" -> `Min
            | "max" -> `Max
            | "decrmin" -> `Decrmin (`METerm (`Int(0)), "dummy_state")
            | "decrmax" -> `Decrmax (`METerm (`Int(0)), "dummy_state")
            | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

let get_poplus tml =
    let poplus_str = get_value tml "poplus" in
        match poplus_str with
            | "union" -> `Union
            | "diff" -> `Diff
            | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))


let get_eterm_node_type tml = get_value tml "eterm-type"
let get_meterm_node_type tml = get_value tml "meterm-type"
let get_expression_node_type tml = get_value tml "expression"
let get_bterm_node_type tml = get_value tml "bterm"
let get_bool_expression_node_type tml = get_value tml "boolexpression"
let get_map_expression_node_type tml = get_value tml "mapexpression"
let get_plan_node_type tml = get_value tml "plan"

let get_eterm tml =
    match children tml with
        | [] -> raise (InvalidTreeML ("node: "^(to_string tml)))
        | tml_l ->
              begin
                  if (List.length tml_l) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                  else
                      let et_type = get_eterm_node_type (List.hd tml_l) in
                          match et_type with
                              | "int" -> `Int(int_of_string (get_value (List.nth tml_l 1) "eterm-val"))
                              | "float" -> `Float(float_of_string (get_value (List.nth tml_l 1) "eterm-val"))
                              | "long" -> `Long(Int64.of_string (get_value (List.nth tml_l 1) "eterm-val"))
                              | "string" -> `String(get_value (List.nth tml_l 1) "eterm-val")
                              | "attr" -> `Attribute(get_attribute_identifier (List.nth tml_l 1))
                              | "var" -> `Variable(get_variable_identifier (List.nth tml_l 1))
                              | _ -> raise (InvalidTreeML ("node: "^(String.concat ", " (List.map to_string tml_l))))
              end

let get_meterm tml =
    match children tml with
        | [] -> raise (InvalidTreeML ("node: "^(to_string tml)))
        | tml_l ->
              begin
                  if (List.length tml_l) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                  else
                      let met_type = get_meterm_node_type (List.hd tml_l) in
                          match met_type with
                              | "int" -> `Int(int_of_string (get_value (List.nth tml_l 1) "meterm-val"))
                              | "float" -> `Float(float_of_string (get_value (List.nth tml_l 1) "meterm-val"))
                              | "long" -> `Long(Int64.of_string (get_value (List.nth tml_l 1) "meterm-val"))
                              | "string" -> `String(get_value (List.nth tml_l 1) "meterm-val")
                              | "attr" -> `Attribute(get_attribute_identifier (List.nth tml_l 1))
                              | "var" -> `Variable(get_variable_identifier (List.nth tml_l 1))
                              | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))
              end

let rec get_expression tml =
    let get_binary tml_l fn =
        if (List.length tml_l) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
        else
            let l = get_expression (List.hd tml_l) in
            let r = get_expression (List.nth tml_l 1) in
                fn l r
    in
    let ch = children tml in
        match ch with
            | h::t -> 
                  let node_type = get_expression_node_type h in
                      begin
                          match node_type with
                              | "eterm" ->
                                    if (List.length t) != 1 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else `ETerm(get_eterm (List.hd t))

                              | "uminus" ->
                                    if (List.length t) != 1 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else `UnaryMinus(get_expression (List.hd t))

                              | "sum" -> get_binary t (fun x y -> `Sum(x,y))
                              | "product" -> get_binary t (fun x y -> `Product(x,y))
                              | "minus" -> get_binary t (fun x y -> `Minus(x,y))
                              | "divide" -> get_binary t (fun x y -> `Divide(x,y))
                              | "function" ->
                                    if (List.length t) = 0 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        let fn_id = get_function_identifier (List.hd t) in
                                        let args = List.map get_expression (List.tl t) in
                                            `Function(fn_id, args)

                              | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))
                      end

            | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

let get_expression_option tml =
    let ch = children tml in
        match ch with
            | h::t ->
                  let node_type = get_expression_node_type h in
                      begin
                          match node_type with
                              | "none" -> None
                              | _ -> Some(get_expression tml)
                      end

            | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

let get_projections tml =
    match (children tml) with
        | [] -> raise (InvalidTreeML ("node: "^(to_string tml)))
        | tml_l ->
              begin
                  let (attrs_tml_l, e_tml_l) =
                      let (_, labelled_tml) =
                          List.fold_left
                              (fun (even, tml_acc) tml -> (not even, tml_acc@[(even, tml)]))
                              (true, []) tml_l
                      in
                      let (x,y) = List.partition fst labelled_tml in
                      let ((_,x2), (_,y2)) = (List.split x, List.split y) in
                          (x2,y2)
                  in
                      List.map2
                          (fun attr_ml e_tml ->
                              let aid = get_attribute_identifier attr_ml in
                              let e = get_expression e_tml in
                                  (aid, e))
                          attrs_tml_l e_tml_l
              end

let get_bindings tml =
    match children tml with
        | [] -> []
        | tml_l ->
              let (attrs_tml, e_opts_tml) =
                  let (_, labelled_tml) =
                      List.fold_left
                          (fun (even, tml_acc) tml -> (not even, tml_acc@[(even, tml)]))
                          (true, []) tml_l
                  in
                  let (x,y) = List.partition fst labelled_tml in
                  let ((_,x2), (_,y2)) = (List.split x, List.split y) in
                      (x2,y2)
              in
                  List.map2
                      (fun attr_tml e_opt_tml -> 
                          let aid = get_attribute_identifier attr_tml in
                          let e_opt = get_expression_option e_opt_tml in
                              (aid, e_opt))
                      attrs_tml e_opts_tml

let rec get_bterm tml =
    let get_binary tml_l fn =
        if (List.length tml_l) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
        else
            let l = get_expression (List.hd tml_l) in
            let r = get_expression (List.nth tml_l 1) in
                fn l r
    in
    let get_unary tml_l fn =
        if (List.length tml_l) != 1 then raise (InvalidTreeML ("node: "^(to_string tml)))
        else
            fn (get_map_expression (List.hd tml_l))
    in
    match (children tml) with
        | h::t ->
              let node_type = get_bterm_node_type h in
                  begin
                      match node_type with
                          | "true" -> `True
                          | "false" -> `False
                          | "eq" ->   get_binary t (fun x y -> `EQ(x,y))
                          | "neq" ->  get_binary t (fun x y -> `NE(x,y))
                          | "lt" ->   get_binary t (fun x y -> `LT(x,y))
                          | "le" ->  get_binary t (fun x y -> `LE(x,y))
                          | "gt" ->   get_binary t (fun x y -> `GT(x,y))
                          | "ge" ->  get_binary t (fun x y -> `GE(x,y))

                          | "meq" ->  get_unary t (fun x -> `MEQ(x))
                          | "mneq" -> get_unary t (fun x -> `MNEQ(x))
                          | "mlt" ->  get_unary t (fun x -> `MLT(x))
                          | "mle" -> get_unary t (fun x -> `MLE(x))
                          | "mgt" ->  get_unary t (fun x -> `MGT(x))
                          | "mge" -> get_unary t (fun x -> `MGE(x))

                          | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))
                  end

        | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

and get_bool_expression tml =
    let get_binary tml_l fn =
        if (List.length tml_l) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
        else
            let l = get_bool_expression (List.hd tml_l) in
            let r = get_bool_expression (List.nth tml_l 1) in
                fn l r
    in
    match (children tml) with
        | h::t ->
              let node_type = get_bool_expression_node_type h in
                  begin
                      match node_type with
                          | "bterm" ->
                                if (List.length t) != 1 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                else `BTerm(get_bterm (List.hd t))

                          | "not" ->
                                if (List.length t) != 1 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                else `Not(get_bool_expression (List.hd t))

                          | "and" -> get_binary t (fun x y -> `And(x,y))
                          | "or" -> get_binary t (fun x y -> `Or(x,y))

                          | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))
                  end
        | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

(* input: branch or leaf node *)
and get_map_expression tml =
    let get_binary_map_expression tml_l fn = 
        if (List.length tml_l) != 2 then
            raise (InvalidTreeML ("node: "^(to_string tml)))
        else
            let l = get_map_expression (List.nth tml_l 0) in
            let r = get_map_expression (List.nth tml_l 1) in
                fn l r
    in
    let ch = children tml in
        match ch with
            | h::t ->
                  let node_type = get_map_expression_node_type h in
                      begin
                          match node_type with
                              | "meterm" ->
                                    if (List.length t) != 1 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else `METerm(get_meterm (List.hd t))

                              | "sum" -> get_binary_map_expression t (fun x y -> `Sum(x,y))
                              | "minus" -> get_binary_map_expression t (fun x y -> `Minus(x,y))
                              | "product" -> get_binary_map_expression t (fun x y -> `Product(x,y))
                              | "min" -> get_binary_map_expression t (fun x y -> `Min(x,y))
                              | "max" -> get_binary_map_expression t (fun x y -> `Max(x,y))

                              | "mapaggregate" ->
                                    if (List.length t) != 3 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        let agg = get_aggregate_function (List.nth t 0) in
                                        let f = get_map_expression (List.nth t 1) in
                                        let q = get_plan (List.nth t 2) in
                                            `MapAggregate(agg, f, q)

                              | "delta" ->
                                    if (List.length t) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        let d = get_delta (List.nth t 0) in
                                        let e = get_map_expression (List.nth t 1) in
                                            `Delta (d, e)

                              | "new" ->
                                    if (List.length t) != 1 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else `New(get_map_expression (List.hd t))

                              | "incr"
                              | "incrdiff" ->
                                    if (List.length t) != 5 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        begin
                                            let sid = get_state_identifier (List.hd t) in
                                            let op = get_oplus (List.nth t 1) in
                                            let re = get_map_expression (List.nth t 2) in
                                            let bd = get_bindings (List.nth t 3) in
                                            let e = get_map_expression (List.nth t 4) in
                                                if node_type = "incr" then
                                                    `Incr(sid, op, re, bd, e)
                                                else
                                                    `IncrDiff(sid, op, re, bd, e)
                                        end

                              | "init" ->
                                    if (List.length t) != 3 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        begin
                                            let sid = get_state_identifier (List.hd t) in
                                            let bd = get_bindings (List.nth t 1) in
                                            let e = get_map_expression (List.nth t 2) in
                                                `Init(sid, bd, e)
                                        end

                              | "insert" ->
                                    if (List.length t) != 3 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        begin
                                            let sid = get_state_identifier (List.hd t) in
                                            let m = get_meterm (List.nth t 1) in
                                            let f = get_map_expression (List.nth t 2) in
                                                `Insert(sid, m, f)
                                        end

                              | "update" ->
                                    if (List.length t) != 4 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        begin
                                            let sid = get_state_identifier (List.hd t) in
                                            let o = get_oplus (List.nth t 1) in
                                            let m = get_meterm (List.nth t 2) in
                                            let f = get_map_expression (List.nth t 3) in
                                                `Update(sid, o, m, f)
                                        end

                              | "delete" ->
                                    if (List.length t) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        begin
                                            let sid = get_state_identifier (List.hd t) in
                                            let m = get_meterm (List.nth t 1) in
                                                `Delete(sid, m)
                                        end

                              | "ifthenelse" ->
                                    if (List.length t) != 3 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        begin
                                            let bt = get_bterm (List.hd t) in
                                            let l = get_map_expression (List.nth t 1) in
                                            let r = get_map_expression (List.nth t 2) in
                                                `IfThenElse(bt, l, r)
                                        end

                              | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))
                      end

            | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

and get_plan tml =
    let ch = children tml in
        match ch with
            | h::t ->
                  let node_type = get_plan_node_type h in
                      begin
                          match node_type with
                              | "truerelation" -> `TrueRelation
                              | "falserelation" -> `FalseRelation
                              | "relation" ->
                                    if (List.length t) < 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        let n = get_relation_identifier (List.hd t) in
                                        let f = get_field_list (List.tl t) in
                                            `Relation(n,f)

                              | "domain" ->
                                    if (List.length t) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        let sid = get_state_identifier (List.hd t) in
                                        let d = get_domain (List.nth t 1) in
                                            `Domain(sid, d)

                              | "select" ->
                                    if (List.length t) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        let pred = get_bool_expression (List.hd t) in
                                        let cq = get_plan (List.nth t 1) in
                                            `Select(pred,cq)

                              | "project" ->
                                    if (List.length t) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else 
                                        let projs = get_projections (List.hd t) in
                                        let cq = get_plan (List.nth t 1) in
                                            `Project(projs, cq)

                              | "union" -> `Union(List.map get_plan t)

                              | "cross" ->
                                    if (List.length t) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        `Cross(get_plan (List.hd t), get_plan (List.nth t 1))

                              | "delta" ->
                                    if (List.length t) != 2 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        `DeltaPlan(get_delta (List.hd t), get_plan (List.nth t 1))

                              | "new" ->
                                    if (List.length t) != 1 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else `NewPlan(get_plan (List.hd t))

                              | "incrplan"
                              | "incrdiffplan" ->
                                    if (List.length t) != 5 then raise (InvalidTreeML ("node: "^(to_string tml)))
                                    else
                                        let sid = get_state_identifier (List.hd t) in
                                        let pop = get_poplus (List.nth t 1) in
                                        let d = get_domain (List.nth t 2) in
                                        let bd = get_bindings (List.nth t 3) in
                                        let cq = get_plan (List.nth t 4) in
                                            if node_type = "incrplan"
                                            then `IncrPlan(sid, pop, d, bd, cq)
                                            else `IncrDiffPlan(sid, pop, d, bd, cq)

                              | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))
                      end

            | _ -> raise (InvalidTreeML ("node: "^(to_string tml)))

let parse_treeml treeml_str =
    let treeml = parse_string treeml_str in
    let start_nodes =
        if (tag treeml) = "tree" then
            List.filter
                (fun x -> let t = tag x in (t = "branch") || (t = "leaf"))
                (children treeml)
        else
            raise (InvalidTreeML ("node: "^(to_string treeml)))
    in
        List.map get_map_expression start_nodes


(*
 * Compilation traces
 *)
(* delta list -> (handler stages * (string * binding stages) list) list) -> string *)
let write_compilation_trace event_path stages_per_map_expr =
    let event_path_name = String.concat "_" (List.map get_bound_relation event_path) in
    let delim l = String.concat "\n" l in
    let make_stage stage_name l =
        (make_branch
            ([("<attribute name=\"stage-name\" value=\""^stage_name^"\"/>");
            ("<attribute name=\"stage-path\" value=\""^event_path_name^"\"/>")]@l))
    in
    let make_handler_stages stages_l =
        (make_branch
            (["<attribute name=\"stage-type\" value=\"handler\"/>"]@stages_l))
    in
    let make_bindings_stages var_and_stages_l =
        (List.flatten
            (List.map
                (fun (var,stages_l) ->
                    make_branch
                        (["<attribute name=\"stage-type\" value=\"binding,"^var^"\"/>"]@stages_l))
                var_and_stages_l))
    in
    let stage_names =
        ["frontier"; "init"; "pre_delta"; "delta"; "domain"; "pre_result"; "result"]
    in
    let trace_fn = ((String.concat "_"
        (List.map (fun e -> match e with
            | `Insert(r,_) -> "i"^r | `Delete(r,_) -> "d"^r) event_path))^
        ".trace")
    in
    let trace_out = open_out trace_fn in
    let trace_tml =
        make_branch
            (List.flatten (List.map
                (fun (hs, bsl) ->
                    let string_of_stage stage_name stage =
                        make_stage stage_name (treeml_of_map_expression stage)
                    in
                    let hs_tml = make_handler_stages
                        (List.flatten (List.map2 string_of_stage stage_names hs))
                    in
                    let bsl_tml =
                        make_bindings_stages
                            (List.map (fun (var,bs) ->
                                (var, (List.flatten (List.map2 string_of_stage stage_names bs)))) bsl)
                    in
                        hs_tml@bsl_tml)
                stages_per_map_expr))
    in
    let declarations =
	(["<declarations>"]@
        (List.map indent
            (io_declarations@
                ["<attributeDecl name=\"stage-type\"           type=\"String\"/>";
                 "<attributeDecl name=\"stage-name\"           type=\"String\"/>";
                 "<attributeDecl name=\"stage-path\"           type=\"String\"/>"]))@
        [ "</declarations>" ])
    in

        output_string trace_out (delim (make_tree (declarations@trace_tml)));
        close_out trace_out;
        trace_fn
