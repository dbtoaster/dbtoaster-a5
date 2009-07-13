open Algebra

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
	      [ (typ_attr "attr"); (typ_val (string_of_attribute_identifier id)) ]

	| `Variable v -> [ (typ_attr "var"); (typ_val v) ]

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
	      [ (typ_attr "attr"); (typ_val (string_of_attribute_identifier id)) ]

	| `Variable v -> [ (typ_attr "var"); (typ_val v) ]

let treeml_of_event evt =
    "<attribute name=\"event\" value=\""^
	(match evt with
	     | `Insert (r,_) -> "insert,"^r
	     | `Delete (r,_) -> "delete,"^r)^"\"/>"

let rec treeml_of_expression expr indent_fn = 
    let binary_expr typ l r =
	[ "<branch>"; (indent_fn ("<attribute name=\"op\" value=\""^typ^"\"/>"))]@
	(List.map indent_fn (treeml_of_expression l indent_fn))@
	(List.map indent_fn (treeml_of_expression r indent_fn))@
        [ "</branch>" ]
    in
    match expr with
	| `ETerm et ->
	      [ "<leaf>"; (indent_fn "<attribute name=\"op\" value=\"eterm\"/>")]@
              (List.map indent_fn (treeml_of_eterm et))@["</leaf>"]

	| `UnaryMinus e ->
	      [ "<branch>"; (indent_fn "<attribute name=\"op\" value=\"-\"/>")]@
	      (List.map indent_fn (treeml_of_expression e indent_fn))@["</branch>"]

	| `Sum(l,r) -> binary_expr "+" l r
	| `Product(l,r) -> binary_expr "*" l r
	| `Minus(l,r) -> binary_expr "-" l r
	| `Divide(l,r) -> binary_expr "/" l r
	| `Function(id, args) ->
	      ["<branch>"; 
	       (indent_fn "<attribute name=\"op\" value=\"function\"/>");
	       (indent_fn ("<attribute name=\"functionid\" value=\""^id^"\"/>"));]@
	       (List.map indent_fn (List.flatten
		   (List.map (fun ae -> treeml_of_expression ae indent_fn) args)))@
	      [ "</branch>" ]

let treeml_of_state_id s =
	"<attribute name=\"stateid\" value=\""^s^"\"/>"

let rec treeml_of_bterm bterm indent_fn =
    let map_expr op e =
	["<attribute name=\"op\" value=\""^op^"\"/>"]@
        (treeml_of_map_expression e indent_fn)
    in
    let binary_expr op l r =
	["<attribute name=\"op\" value=\""^op^"\"/>"]@
        (treeml_of_expression l indent_fn)@
	(treeml_of_expression r indent_fn)
    in
    match bterm with
	| `LT (l,r) -> binary_expr "&lt;" l r
	| `LE (l,r) -> binary_expr "&lt;=" l r
	| `GT (l,r) -> binary_expr "&gt;" l r
	| `GE  (l,r) -> binary_expr "&gt;=" l r
	| `EQ  (l,r) -> binary_expr "=" l r
	| `NE  (l,r) -> binary_expr "!=" l r
	| `MEQ e -> map_expr "m=" e
	| `MLT e -> map_expr "m&lt;" e

and treeml_of_bool_expression b_expr indent_fn =
    match b_expr with
	| `BTerm bt ->
	      ["<branch>"; (indent_fn "<attribute name=\"op\" value=\"bterm\"/>")]@
		  (List.map indent_fn (treeml_of_bterm bt indent_fn))@["</branch>"]

	| `Not be ->
	      ["<branch>"; (indent_fn "<attribute name=\"op\" value=\"not\"/>")]@
		  (List.map indent_fn (treeml_of_bool_expression be indent_fn))@["</branch>"]

	| `And (l,r) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"and\"/>")]@
		  (List.map indent_fn (treeml_of_bool_expression l indent_fn))@
		  (List.map indent_fn (treeml_of_bool_expression r indent_fn))@["</branch>"]

	| `Or (l,r) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"or\"/>")]@
		  (List.map indent_fn (treeml_of_bool_expression l indent_fn))@
		  (List.map indent_fn (treeml_of_bool_expression r indent_fn))@["</branch>"]

and treeml_of_map_expression m_expr indent_fn = 
    let treeml_of_aggregate_function agg =
	"<attribute name=\"aggregate\" value=\""^
	    (match agg with | `Sum -> "sum" | `Min -> "min" | `Max -> "max" )^"\"/>"
    in
    let treeml_of_oplus o = 
	"<attribute name=\"oplus\" value=\""^
	    (match o with | `Plus -> "m+" | `Minus -> "m-" | `Min -> "min" | `Max -> "max" | `Decrmin (_) -> "decrmin" )^"\"/>" (*TODO: check decrmin later *)
    in
    let binary_node typ l r =
	["<branch>";
	    (indent_fn ("<attribute name=\"op\" value=\""^typ^"\"/>"))]@
	    (List.map indent_fn (treeml_of_map_expression l indent_fn))@
	    (List.map indent_fn (treeml_of_map_expression r indent_fn))@["</branch>"]
    in
    match m_expr with
	| `METerm x ->
	      ["<leaf>";
		  (indent_fn "<attribute name=\"op\" value=\"meterm\"/>")]@
		  (List.map indent_fn (treeml_of_meterm x))@["</leaf>"]

	| `Delete (sid, x) ->
	      ["<leaf>";
		  (indent_fn "<attribute name=\"op\" value=\"delete\"/>");
		  (indent_fn (treeml_of_state_id sid))]@
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
		  (List.map indent_fn (treeml_of_map_expression f indent_fn))@
		  (List.map indent_fn (treeml_of_plan q indent_fn))@
		  ["</branch>"]

	| `Delta (e, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"delta\"/>");
		  (indent_fn (treeml_of_event e))]@
		  (List.map indent_fn (treeml_of_map_expression f indent_fn))@["</branch>"]

	| `New f ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"new\"/>")]@
		  (List.map indent_fn (treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Incr (s, o, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"incr\"/>");
		  (indent_fn (treeml_of_state_id s));
		  (indent_fn (treeml_of_oplus o))]@
		  (List.map indent_fn (treeml_of_map_expression f indent_fn))@["</branch>"]

	| `IncrDiff (s, o, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"incrdiff\"/>");
		  (indent_fn (treeml_of_state_id s));
		  (indent_fn (treeml_of_oplus o))]@
		  (List.map indent_fn (treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Init (s, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"init\"/>");
		  (indent_fn (treeml_of_state_id s))]@
		  (List.map indent_fn (treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Insert (s, m, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"insert\"/>");
		  (indent_fn (treeml_of_state_id s))]@
		  (List.map indent_fn (treeml_of_meterm m))@
		  (List.map indent_fn (treeml_of_map_expression f indent_fn))@["</branch>"]

	| `Update (s, o, m, f) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"insert\"/>");
		  (indent_fn (treeml_of_state_id s));
		  (indent_fn (treeml_of_oplus o))]@
		  (List.map indent_fn (treeml_of_meterm m))@
		  (List.map indent_fn (treeml_of_map_expression f indent_fn))@["</branch>"]

	| `IfThenElse(b,l,r) -> 
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"ifthenelse\"/>")]@
		  (List.map indent_fn (treeml_of_bterm b indent_fn))@
	          (List.map indent_fn (treeml_of_map_expression l indent_fn))@
	          (List.map indent_fn (treeml_of_map_expression r indent_fn))@["</branch>"]

and treeml_of_plan p indent_fn = 
    let treeml_of_relation rtyp r f =
	["<leaf>";
	    (indent_fn ("<attribute name=\"op\" value=\""^rtyp^"\"/>"));
	    (indent_fn ("<attribute name=\"rid\" value=\""^r^"\"/>"))]@
	    (List.map (fun (id,typ) ->
		(indent_fn ("<attribute name=\"field\" value=\""^id^","^typ^"\"/>")))
		 f)@["</leaf>"]
    in
    let treeml_of_poplus o = 
	"<attribute name=\"poplus\" value=\""^
	    (match o with | `Union -> "mU" | `Diff -> "m-" )^"\"/>"
    in
    let treeml_of_projections projs =
	List.map
	    (fun (oid, expr) ->
		 "<attribute name=\"projection\" value=\""^
		     (string_of_attribute_identifier oid)^","^
		     (string_of_expression expr)^"\"/>")
	    projs
    in
    match p with
	| `Relation (r,f) -> treeml_of_relation "relation" r f

	| `Select (pred, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"select\"/>")]@
		  (List.map indent_fn (treeml_of_bool_expression pred indent_fn))@
		  (List.map indent_fn (treeml_of_plan c indent_fn))@["</branch>"]

	| `Project (projs, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"project\"/>")]@
		  (List.map indent_fn (treeml_of_projections projs))@
		  (List.map indent_fn (treeml_of_plan c indent_fn))@["</branch>"]

	| `Union ch ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"union\"/>")]@
		  (List.map indent_fn (List.flatten
		      (List.map (fun c -> treeml_of_plan c indent_fn) ch)))@
		  ["</branch>"]

	| `Cross(l,r) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"cross\"/>")]@
		  (List.map indent_fn (treeml_of_plan l indent_fn))@
		  (List.map indent_fn (treeml_of_plan r indent_fn))@["</branch>"]

        (*
	| `NaturalJoin(l,r) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"naturaljoin\"/>")]@
		  (List.map indent_fn (treeml_of_plan l indent_fn))@
		  (List.map indent_fn (treeml_of_plan r indent_fn))@["</branch>"]

	| `Join (pred, l, r) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"join\"/>")]@
		  (List.map indent_fn (treeml_of_bool_expression pred indent_fn))@
		  (List.map indent_fn (treeml_of_plan l indent_fn))@
		  (List.map indent_fn (treeml_of_plan r indent_fn))@["</branch>"]
        *)

	| `TrueRelation -> ["<leaf>"; (indent_fn "<attribute name=\"op\" value=\"truerelation\">");"</leaf>"]

	| `FalseRelation -> ["<leaf>"; (indent_fn "<attribute name=\"op\" value=\"falserelation\">");"</leaf>"]

	| `DeltaPlan (e, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"delta\"/>");
		  (indent_fn (treeml_of_event e))]@
		  (List.map indent_fn (treeml_of_plan c indent_fn))@["</branch>"]
	      
	| `NewPlan (c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"newplan\"/>")]@
		  (List.map indent_fn (treeml_of_plan c indent_fn))@["</branch>"]

	| `IncrPlan (sid, p, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"incrplan\"/>");
		  (indent_fn (treeml_of_state_id sid));
		  (indent_fn (treeml_of_poplus p))]@
		  (List.map indent_fn (treeml_of_plan c indent_fn))@["</branch>"]

	| `IncrDiffPlan (sid, p, c) ->
	      ["<branch>";
		  (indent_fn "<attribute name=\"op\" value=\"incrdiffplan\"/>");
		  (indent_fn (treeml_of_state_id sid));
		  (indent_fn (treeml_of_poplus p))]@
		  (List.map indent_fn (treeml_of_plan c indent_fn))@["</branch>"]

let treeml_string_of_map_expression m_expr =
    let indent s = "    "^s in
    let delim l = String.concat "\n" l in
    let header = [ "<tree>" ] in
    let footer = [ "</tree>" ] in
    let declarations =
	List.map indent
	    (["<declarations>"]@
		 (List.map indent
		      [ "<attributeDecl name=\"event\" type=\"String\"/>";
			"<attributeDecl name=\"eterm-type\" type=\"String\"/>";
			"<attributeDecl name=\"eterm-val\" type=\"String\"/>";
			"<attributeDecl name=\"meterm-type\" type=\"String\"/>";
			"<attributeDecl name=\"meterm-val\" type=\"String\"/>";
			"<attributeDecl name=\"bterm\" type=\"String\"/>";
			"<attributeDecl name=\"functionid\" type=\"String\"/>";
			"<attributeDecl name=\"expr\" type=\"String\"/>";
			"<attributeDecl name=\"boolexpr\" type=\"String\"/>";
			"<attributeDecl name=\"aggregate\" type=\"String\"/>";
			"<attributeDecl name=\"stateid\" type=\"String\"/>";
			"<attributeDecl name=\"mapexpr\" type=\"String\"/>";
			"<attributeDecl name=\"rid\" type=\"String\"/>";
			"<attributeDecl name=\"field\" type=\"String\"/>";
			"<attributeDecl name=\"binding\" type=\"String\"/>";
			"<attributeDecl name=\"projection\" type=\"String\"/>";
			"<attributeDecl name=\"plan\" type=\"String\"/>";
			"<attributeDecl name=\"op\" type=\"String\"/>";])@
		 [ "</declarations>" ])
    in
	(delim
	     (header@declarations@
		  (List.map indent (treeml_of_map_expression m_expr indent))@
		  footer))
