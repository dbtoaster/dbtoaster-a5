(* K3 Cpp Generator
 * A backend that produces C++ source code for K3 programs.
 *)

open Util
open M3
open K3
open Str

module K3C =
struct

  type data_id = string
  type op_t=string
  type db_t=unit
  type source_impl_t=string
  type pexpr_t = Empty
    | Ident of data_id 
    | Tuple of pexpr_t list
    | Math_op of pexpr_t*op_t*pexpr_t 
    | Cond_op of pexpr_t*pexpr_t*pexpr_t 
      
  type stmt_t = Expr of pexpr_t*SR.type_t 
    | Decl of SR.type_t*data_id*pexpr_t 
      (*Declaration of a variable type, name, initilizaer*)
    | Assg of data_id*pexpr_t
      (*Simple assign of two objects of a similar type, name, value*)     
    | Assg_one of data_id*SR.type_t*pexpr_t*pexpr_t
      (*Assign value to a one-level map, name, key type, key, value*)     
    | Assg_two of data_id*SR.type_t*pexpr_t*SR.type_t*pexpr_t*pexpr_t
      (*Assign value to a two-level map, name, key1 type, key1,
         key2 type, key2, value*)       
    | If of pexpr_t*stmt_t list*stmt_t list
      (*If then else*)
    | Lambda of data_id*pexpr_t*stmt_t list
      (*Lambda argument, value, body*)
    | Forlambda of SR.type_t*data_id*data_id list*stmt_t list*data_id
      (*Lambda with iteration iterating type, name, argument list, body,
         optional accumulator*)
    | Add of data_id*SR.type_t*pexpr_t
      (*Insert an element into a collection name, elem type, elem*)      
    | Flatten of SR.type_t*data_id*data_id
      (*Flatten mutlilayer collection type, name, newname*)       
    | Slice of SR.type_t*data_id*data_id*data_id list*pexpr_t list*int list
      (*Slice iterating type, name, newname, argument list(don't eaxctly do much),
        sliced value list, pattern*)
    | Exists of data_id*data_id*SR.type_t*pexpr_t
      (*resultname, name, key type, key*)
    | Lookup of data_id*data_id*SR.type_t*pexpr_t
      (*resultname, name, key type, key*)      
    | Trigger of string*data_id list*stmt_t list
      (*only used in trigger, trigger name, argument list, body*)
    | Code of string
      (*only used in main, directly enclose some code*)
      
  type code_t = stmt_t list
  
  let add_op="+"
  let mult_op="*"
  let eq_op="=="
  let neq_op="!="
  let lt_op="<"
  let leq_op="<="
  let ifthenelse0_op="?"

(* Utility Functions *)

  let index=[|0;0;0;0;0;0;0;0;0|]
  let name_stream index:string=
    Array.set index 0 ((Array.get index 0)+1);
    "str"^(string_of_int (Array.get index 0))

  let name_map index:string=
    Array.set index 1 ((Array.get index 1)+1);
    "map"^(string_of_int (Array.get index 1))
    
  let name_accum index:string=
    Array.set index 2 ((Array.get index 2)+1);
    "accum"^(string_of_int (Array.get index 2))

  let name_accv index:string=
    Array.set index 3 ((Array.get index 3)+1);
    "accv"^(string_of_int (Array.get index 3))
    
  let name_slice index:string=
    Array.set index 4 ((Array.get index 4)+1);
    "slice"^(string_of_int (Array.get index 4))

  let name_flat index:string=
    Array.set index 5 ((Array.get index 5)+1);
    "flat"^(string_of_int (Array.get index 5))
    
  let name_itr index:string=
    Array.set index 6 ((Array.get index 6)+1);
    "itr"^(string_of_int (Array.get index 6))      
  
  let name_exists index:string=
    Array.set index 7 ((Array.get index 7)+1);
    "exists"^(string_of_int (Array.get index 7))
    
  let name_lookup index:string=
    Array.set index 8 ((Array.get index 8)+1);
    "lookup"^(string_of_int (Array.get index 8))      
    
  let exist_cexpr (code:code_t):bool=
    match (List.hd (List.rev code)) with Expr(_)->true
      | _->false        

  let print_stmt (stmt:stmt_t):string=
    match stmt with  Expr _->"Expr"
    | Decl _->"Decl"
    | Assg _->"Assg"
    | Assg_one _->"Assg_one"
    | Assg_two _->"Assg_two"
    | If _->"If"
    | Lambda _->"Lambda"
    | Forlambda _->"Forlambda"
    | Add _->"Add"
    | Flatten _->"Flatten"
    | Slice _->"Slice"
    | Exists _->"Exist"
    | Lookup _->"Lookup"
    | Trigger _->"Trigger"
    | Code _->"Code"
        
  let get_cexpr (code:code_t):pexpr_t=
    match (List.hd (List.rev code)) with Expr(cexpr,exprtype)->cexpr
      | _->failwith 
        ("Failed to get cexpr:"^(print_stmt (List.hd (List.rev code)))) 
        
  let get_id (code:code_t):data_id=
    match (List.hd (List.rev code)) with Expr(Ident(id),exprtype)->id
      | _->failwith "Failed to get id"       
 
  let get_exprtype (code:code_t):SR.type_t=
    match (List.hd (List.rev code)) with Expr(cexpr,exprtype)->exprtype
      | _->failwith 
        ("Failed to get exprtype:"^(print_stmt (List.hd (List.rev code))))
        
  let get_maptype (key:SR.type_t) (value:SR.type_t):SR.type_t=
    match key with SR.TTuple(a)->SR.Collection(SR.TTuple(a@[value]))
      | _->failwith "Failed to get keytype"
        
  let get_valuetype (map:SR.type_t):SR.type_t=
    match map with SR.Collection(SR.TTuple(a))->(List.hd (List.rev a))
      | SR.Collection _->failwith "Failed to get valuetype:Collection"
      | SR.TTuple _->failwith "Failed to get valuetype:TTuple"
      | SR.TFloat _->failwith "Failed to get valuetype:TFloat"        
      | _->failwith "Failed to get valuetype"        

  let rec get_flattype (colt:SR.type_t):SR.type_t=
    match colt with SR.Collection(SR.Collection(a))->(get_flattype (SR.Collection a))
      | _->colt       
        
  let get_stmt (code:code_t):code_t=
    List.rev (List.tl (List.rev code))
    
  let get_decl (code:code_t):code_t=
    List.fold_left (fun a b->match b with Decl _->(a@[b])|_->a) [] code
    
  let get_else (code:code_t):code_t=
    List.fold_left (fun a b->match b with Decl _ ->a|_->(a@[b])) [] code  
      
(* K3 Implementation *)
  
  let const ?(expr = None) (c:M3.const_t): code_t = 
    [Expr ((Ident (match c with CFloat(y) -> string_of_float y)),SR.TFloat)]
  
  let var ?(expr = None) (v:M3.var_t) (t:K3.SR.type_t): code_t = 
    [Expr (Ident("MaGiC"^v),t)]

  let tuple ?(expr = None) (c:code_t list): code_t = 
	(List.flatten (List.map get_stmt c))@
    [Expr ((Tuple (List.map get_cexpr c)),(SR.TTuple(List.map get_exprtype c)))]

(* Not implenmented *)    
    
  let project ?(expr = None) (tuple:code_t) (f:int list): code_t =[]

  let singleton ?(expr = None) (d:code_t) (t:K3.SR.type_t): code_t =[]

(* Not implenmented *)        
    
  let combine ?(expr = None) (a_code:code_t) (b_code:code_t): code_t =[]
  
  let op ?(expr = None) (mk_op:op_t) (lhs:code_t) (rhs:code_t): code_t = 
	(get_stmt lhs)@(get_stmt rhs)@
    [Expr ((if mk_op<>"?"
    then Math_op((get_cexpr lhs),mk_op,(get_cexpr rhs))
    else Cond_op((get_cexpr lhs),(get_cexpr rhs),Ident "0")),SR.TFloat)]
  
  let ifthenelse ?(expr = None) (p:code_t) (t:code_t) (e:code_t): code_t =
    if exist_cexpr t
    then (get_stmt p)@(get_decl t)@(get_decl e)@
      [If((get_cexpr p),(get_else (get_stmt t)),(get_else (get_stmt e)))]@
      [Expr(Cond_op((get_cexpr p),(get_cexpr t),(get_cexpr e)),(get_exprtype t))]
    else (get_stmt p)@[If((get_cexpr p),t,e)]
      
  let block ?(expr = None) (code:code_t list): code_t = 
	List.flatten code
     
  let iterate ?(expr = None) (fn:code_t) (colt:code_t): code_t =
    let id=get_id colt in
    let exprtype=get_exprtype colt in
	let args=match expr with Some(SR.Iterate(SR.Lambda(SR.ATuple(args_list),_),_))
   		->List.map fst args_list
   		|_-> failwith "Match Failure: Not a valid lambda :K3Cppgen@iterate"
 	in
		(get_stmt colt)@[Forlambda(exprtype,id,args,fn,"MaGiCaccv")]

  let lambda ?(expr =None) (args:K3.SR.arg_t) (body:code_t): code_t = body

  let assoc_lambda ?(expr = None) (arg1:K3.SR.arg_t) (arg2:K3.SR.arg_t) 
    				(body:code_t): code_t = body

  let rec apply_expr (src:pexpr_t) (arg:data_id) (tgt:pexpr_t): pexpr_t=
    match src with Empty->Empty
    | Ident (a)->if a<>arg then Ident (a) else tgt
    | Tuple(a)->Tuple(List.map (fun expr->apply_expr expr arg tgt) a)
    | Math_op(a,b,c)->Math_op((apply_expr a arg tgt),b,(apply_expr c arg tgt))
    | Cond_op(a,b,c)->Cond_op((apply_expr a arg tgt),(apply_expr b arg tgt),
      (apply_expr c arg tgt))
    
  let apply ?(expr = None) (fn:code_t) (value:code_t): code_t =
  	let arg=match expr with Some(SR.Apply(SR.Lambda(SR.AVar(a,b),_),_))
     	->a
		| _->failwith "Match Failure: Not a valid lambda :K3Cppgen@apply" 
	in
    	(get_stmt value)@[Lambda(("MaGiC"^arg),(get_cexpr value),(get_stmt fn))]
    @[Expr((apply_expr (get_cexpr fn) ("MaGiC"^arg) (get_cexpr value)),(get_exprtype fn))]
    
  let map ?(expr = None) (fn:code_t) (exprt:K3.SR.type_t) 
    		(colt:code_t): code_t =
    let name=name_map index in
    let id=get_id colt in
    let exprtype=get_exprtype colt in
	let args=match expr with Some(SR.Map(SR.Lambda(SR.ATuple(args_list),_),_))
   		->(List.map fst args_list) 
   		|_-> failwith "Match Failure: Not a valid lambda :K3Cppgen@iterate"
 	in
    	(get_stmt colt)@[Decl(SR.Collection(get_exprtype fn),name,Empty);
       Forlambda(exprtype,id,args,((get_stmt fn)@[Add(name,(get_exprtype fn),(get_cexpr fn))]
                            ),"MaGiCaccv");Expr((Ident name),SR.Collection(get_exprtype fn))]

  let aggregate ?(expr = None) (fn:code_t) (init:code_t) (colt:code_t): code_t =
    let name=name_accv index in
    let id=get_id colt in
    let exprtype=get_exprtype colt in
  	let args=match expr with 
    	Some(SR.Aggregate(SR.AssocLambda(SR.ATuple(args_list),_,_),_,_))
     	->List.map fst args_list
		|_-> failwith "Match Failure: Not a valid lambda :K3Cppgen@aggregate" in
		(get_stmt colt)@(get_stmt init)@[Decl(SR.TFloat,name,(get_cexpr init));
    		Forlambda(exprtype,id,args,((get_stmt fn)@
        [Assg(name,(get_cexpr fn))]),name);Expr(Ident(name),SR.TFloat)]

  let group_by_aggregate ?(expr = None) (fn:code_t)(init:code_t) (group:code_t) 
    					(colt:code_t): code_t =
    let name=name_accum index in
    let accv="accv" in
    let id=get_id colt in
    let exprtype=get_exprtype colt in
    let args=match expr with 
		Some(SR.GroupByAggregate(SR.AssocLambda(SR.ATuple(args_list),_,_),_,_,_))
    	->List.map fst args_list
    	|_-> failwith "Match Failure: Not a valid lambda :K3Cppgen@groupbyaggregate" in
		(get_stmt colt)@[Decl((get_maptype (get_exprtype group) SR.TFloat),name,Empty);
    		Forlambda(exprtype,id,args,(
        	(get_stmt init)@
        	[Decl(SR.TFloat,accv,(get_cexpr init));
        	Lookup(accv,name,(get_exprtype group),(get_cexpr group))]@
        	(get_stmt fn)@[Assg(accv,(get_cexpr fn));
            Assg_one(name,(get_exprtype group),(get_cexpr group),Ident(accv))]),accv);
        Expr(Ident(name),(get_maptype (get_exprtype group) SR.TFloat))]

  let flatten ?(expr = None) (colt:code_t): code_t =
    let name=name_flat index in
    let id=get_id colt in
    let exprtype=get_exprtype colt in
    	(get_stmt colt)@[Decl((get_flattype exprtype),name,Empty);
       Flatten(exprtype,id,name);Expr(Ident(name),(get_flattype exprtype))]

  let exists ?(expr = None) (colt:code_t) (key:code_t list): code_t =
    let name=name_exists index in
    	(get_stmt colt)@(List.flatten (List.map get_stmt key))@
    	[Decl(SR.TInt,name,Empty);
     	Exists(name,(get_id colt),SR.TTuple(List.map get_exprtype key),
       	Tuple (List.map get_cexpr key));Expr(Ident(name),SR.TInt)]

  let lookup ?(expr = None) (colt:code_t) (key:code_t list): code_t =
    let name=name_lookup index in
    let exprtype=get_exprtype colt in
    	(get_stmt colt)@(List.flatten (List.map get_stmt key))@
    	[Decl((get_valuetype exprtype),name,Empty);
       	Lookup(name,(get_id colt),SR.TTuple(List.map get_exprtype key),
       	Tuple (List.map get_cexpr key));Expr(Ident(name),(get_valuetype exprtype))]

  let slice ?(expr = None) (colt:code_t) (key:code_t list) (pattern:int list): code_t =
    if (List.length pattern)=0 
    then colt
    else let name=name_slice index in
    let id=get_id colt in
    let exprtype=get_exprtype colt in    
    let args=match expr with Some(SR.Slice(_,a,_))  
     	->List.map fst a
    	|_-> failwith "Match Failure: Not a valid slice :K3Cppgen@slice" in
		(get_stmt colt)@(List.flatten (List.map get_stmt key))@
    	[Decl(exprtype,name,Empty);
		Slice(exprtype,id,name,args,(List.map get_cexpr key),pattern);
       	Expr(Ident(name),exprtype)]
      

(* API? *)    
    
  let get_value ?(expr = None) (t:K3.SR.type_t) (map:K3.SR.coll_id_t): code_t = 
    [Expr(Ident(map),SR.TFloat)]
  
  let get_out_map ?(expr = None) (schema:K3.SR.schema) (t:K3.SR.type_t) 
    (map:K3.SR.coll_id_t): code_t = 
    [Expr(Ident(map),(get_maptype (SR.TTuple(List.map snd schema)) SR.TFloat))]
    
  let get_in_map ?(expr = None) (schema:K3.SR.schema) (t:K3.SR.type_t) 
    (map:K3.SR.coll_id_t): code_t = 
    [Expr(Ident(map),(get_maptype (SR.TTuple(List.map snd schema)) SR.TFloat))]
  
  let get_map ?(expr = None) ((ins,outs):(K3.SR.schema*K3.SR.schema)) 
    				(t:K3.SR.type_t) (map:K3.SR.coll_id_t): code_t = 
    [Expr(Ident(map),(get_maptype (SR.TTuple(List.map snd ins)) 
      (get_maptype (SR.TTuple(List.map snd outs)) SR.TFloat)))]

  let update_value ?(expr = None) (map:K3.SR.coll_id_t) 
    				(code:code_t): code_t = 
    (get_stmt code)@[Assg(map,(get_cexpr code))]

  let update_out_map_value ?(expr = None) (map:K3.SR.coll_id_t) (key:code_t list) 
    				(code:code_t): code_t = 
    (get_stmt code)@(List.flatten (List.map get_stmt key))@
    [Assg_one(map,SR.TTuple(List.map get_exprtype key),
      Tuple (List.map get_cexpr key),(get_cexpr code))]

  let update_in_map_value ?(expr = None) (map:K3.SR.coll_id_t) (key:code_t list) 
    				(code:code_t): code_t = 
    (get_stmt code)@(List.flatten (List.map get_stmt key))@
    [Assg_one(map,SR.TTuple(List.map get_exprtype key),
      Tuple (List.map get_cexpr key),(get_cexpr code))]

  let update_map_value ?(expr = None) (map:K3.SR.coll_id_t) (inkey:code_t list) 
    					(outkey:code_t list) (code:code_t): code_t = 
    (get_stmt code)@(List.flatten (List.map get_stmt inkey))@
    (List.flatten (List.map get_stmt outkey))@
    [Assg_two(map,SR.TTuple(List.map get_exprtype inkey),
      Tuple (List.map get_cexpr inkey),SR.TTuple(List.map get_exprtype outkey),
      Tuple (List.map get_cexpr outkey),(get_cexpr code))]

  let update_in_map ?(expr = None) (map:K3.SR.coll_id_t) (code:code_t): code_t = 
    (get_stmt code)@[Assg(map,(get_cexpr code))]

  let update_out_map ?(expr = None) (map:K3.SR.coll_id_t) (code:code_t): code_t =
    (get_stmt code)@[Assg(map,(get_cexpr code))]
   
  let update_map ?(expr = None) (map:K3.SR.coll_id_t) (inkey:code_t list) 
    (code:code_t): code_t = 
    (get_stmt code)@(List.flatten (List.map get_stmt inkey))@
    [Assg_one(map,SR.TTuple(List.map get_exprtype inkey),
      Tuple (List.map get_cexpr inkey),(get_cexpr code))]

(* Code Generation *)
   
  let rec typecast (t:SR.type_t):string= 
    match t with SR.Collection(SR.Collection(a))->"list<"^(typecast (SR.Collection(a)))^" >"
      |SR.Collection(SR.TTuple(a))->if (List.length a)<2
      then failwith "Type not implemented"
      else "map<"^(typecast (SR.TTuple(List.rev (List.tl (List.rev a)))))^","^
        (typecast (List.hd (List.rev a)))^" >"
      |SR.TTuple(a)->if (List.length a)=1 
      then typecast (List.hd a) 
      else "tuple<"^(String.concat "," (List.map typecast a))^" >"
      |SR.TInt->"int"
      |SR.TFloat->"double"
      |_->failwith "Type not implemented"
    
  let trigger (ins_del:M3.pm_t) (rel:M3.rel_id_t) (vars:M3.var_t list) 
    			(code:code_t list): code_t =
    [Trigger(("void On_"^(if ins_del = Insert then "Insert" else "Delete")^"_"^rel),
      vars,(List.flatten code))]
      
  let arg_maker (length:int):string =
  	let bf =Buffer.create 1024 in
  		if length=1
  		then "arg[0]"
  		else (for i=1 to (length-1) do 
      		Buffer.add_string bf ("arg["^(string_of_int (i-1))^"],")
    		done;
      		Buffer.add_string bf ("arg["^(string_of_int (length-1))^"]");Buffer.contents bf);;    


  let source (source:M3.source_t) (framing:M3.framing_t)
              (relations:(string * M3.adaptor_t) list):
              (source_impl_t * code_t option * code_t option) =
    let source_init = 
       match source with 
          | M3.FileSource(s) -> s
          | M3.SocketSource(_,_) -> failwith "Sockets not implemented"
          | M3.PipeSource(_) -> failwith "Pipes not implemented"
    in
    let name=name_stream index in
    let srctype=
    	match (List.hd relations) with |(rel,(atype,akeys))->atype
    in
    let srckeys=
    	match (List.hd relations) with |(rel,(atype,akeys))->akeys
    in
    let csvadpt delim arglist event=match (List.hd relations) with |(rel,(atype,akeys))->
      let arg=arg_maker(List.length (split (regexp ",") arglist)) in
        "ifstream "^name^";\n"^
        name^".open(\""^source_init^"\",ios::in);\n"^
        "while(int i=CSVAdpt("^name^",'"^delim^"',\""^arglist^"\",\""^event^"\",arg)){\n"^
        	"if(i==1)On_Insert_"^rel^"("^arg^");\n"^
        	"if(i==2)On_Delete_"^rel^"("^arg^");\n"^
        "}\n"^
        name^".close();\n",
        None,None
    in
    match srctype with
      | "csv" ->(match (List.hd relations) with |(rel,(atype,akeys))->
        csvadpt (snd (List.nth akeys 0)) (snd (List.nth akeys 1)) (snd (List.nth akeys 2)))
      | "lineitem" -> csvadpt "|" ("int,int,int,int,int,float,float,float,hash,"^
        "hash,date,date,date,hash,hash,hash") "insert"
      | "orders" -> csvadpt "|" "int,int,hash,float,date,hash,hash,int,hash" "insert"
      | "part" -> csvadpt "|" "int,hash,hash,hash,hash,int,hash,float,hash" "insert"
      | "partsupp" -> csvadpt "|" "int,int,int,float,hash" "insert"
      | "customer" -> csvadpt "|" "int,hash,hash,int,hash,float,hash,hash" "insert"
      | "supplier" -> csvadpt "|" "int,hash,hash,int,hash,float,hash" "insert"
      | "nation" -> csvadpt "|" "int,hash,int,hash" "insert"
      | "region" -> csvadpt "|" "int,hash,hash" "insert"
      | "orderbook" -> 
      let num_brokers = try
      	int_of_string (List.assoc "brokers" srckeys)
        with Not_found _ -> 0
      in
      	"ifstream "^name^";\n"^
      	name^".open(\""^source_init^"\",ios::in);\n"^
      	"while(int i=ORDAdpt("^name^",arg,"^(string_of_int num_brokers)^")){\n"^
      	(String.concat "" (List.map (fun x->match x with |(rel,(atype,akeys))->
       	let booktype= List.assoc "book" akeys in
        	"if(i=="^(match booktype with |"bids"->"1" |"asks"->"3" |_->"")^
         	")On_Insert_"^rel^
        		"("^(if num_brokers=0 then "" else "arg[2],")^"arg[0],arg[1]);\n"^
        	"if(i=="^(match booktype with |"bids"->"2" |"asks"->"4" |_->"")^
         	")On_Delete_"^rel^
        		"("^(if num_brokers=0 then "" else "arg[2],")^"arg[0],arg[1]);\n"
        ) relations))^
        "}\n"^
        name^".close();\n",None,None       
      | _ -> failwith ("Adapter not implemented: "^srctype)


  let tuplet_maker key: string=
  	let id=(List.map (fun (_,a,_,_,_)->a) key) in
  	let keyt=(List.map (fun (_,_,_,a,_)->a) key) in
    	if (List.length id)>1
    	then "tuple<"^(String.concat "," keyt)^">"
    	else (List.nth keyt 0)        
        
  let map_typecast a=
    match a with VT_String->"string"
      |VT_Int->"double"
      |VT_Float->"double"
  
  let map_type ((id,ta,tb):M3.map_type_t):string=
    if (List.length ta)<>0||(List.length tb)<>0
    then (if (List.length ta)<>0&&(List.length tb)<>0 
    	then ("map<"^(tuplet_maker (List.map (fun a->("","","",a,"")) 
      		(List.map map_typecast ta)))^",map<"^(tuplet_maker 
        	(List.map (fun a->("","","",a,"")) (List.map map_typecast tb)))^
         	",double> >")
    	else (if (List.length ta)<>0 then 
      		"map<"^(tuplet_maker (List.map (fun a->("","","",a,"")) 
			(List.map map_typecast ta)))^",double>" else 
			"map<"^(tuplet_maker (List.map (fun a->("","","",a,"")) 
   			(List.map map_typecast tb)))^",double>"))
    else "double"
      
  let map_declaration ((id,ta,tb):M3.map_type_t):string=
	(map_type (id,ta,tb))^" "^id^";\n"

  let list_maker (length:int) (itr:string) :string list =
    if length=1 then ["(*"^itr^")"]
    else if length=2 then [itr^"->first";itr^"->second"]
    else let rec get_itr num list= if num>1 then (get_itr (num-1) list)@
      ["get<"^(string_of_int (num))^">("^itr^"->first)"] else
        ["get<0>("^itr^"->first)";"get<1>("^itr^"->first)"] in 
      (get_itr (length-2) [])@[itr^"->second"]      

  let map_printer ((id,ta,tb):M3.map_type_t):string=
    if (List.length (max ta tb))=0 
    then "" 
    else "ostream& operator <<(ostream &os,"^(map_type (id,ta,tb))^" &obj){\n"^
      		"for("^(map_type (id,ta,tb))^"::iterator objitr=obj.begin();objitr!=obj.end();"^
      		"objitr++)os <<"^(String.concat "<<\"->\" <<" 
                          (list_maker ((List.length (max ta tb))+1) "objitr"))^
      					" <<endl;\n"^
      		"return os;\n"^
      	"}\n"
      
  let main (trigger_params:(string * Calculus.var_t list) list)
            (schema:M3.map_type_t list) (patterns:M3Common.Patterns.pattern_map)
            (sources:(source_impl_t * code_t option * code_t option) list)
            (triggers:code_t list) (tlqs:string list): code_t =
    let src=List.map (fun(a,_,_)->a) sources in
    let print_result="cout << setprecision(9) << "^(String.concat " << " tlqs)^
    	"<< endl;\n" in
    	[Code ("#include <string>\n"^
       	"#include <iostream>\n"^
       	"#include <fstream>\n"^
    	"#include <map>\n"^
       	"#include <list>\n"^
       	"#include <tr1/tuple>\n"^
       	"#include <iomanip>\n"^
    	"#include <Adaptor.h>\n"^
       	"\n"^
       	"using namespace std;\n"^
       	"using namespace std::tr1;\n"^
       	"\n"^
    	(String.concat "" (List.map map_declaration schema))^
    	(map_printer (List.hd schema)))]@(List.flatten triggers)@
    	[Code ("int main(){\n"^
       		"double arg[20];\n"^
       		"srand(12345);\n"^
       		(String.concat "" src)^
       		print_result^
           	"return 0;\n"^
		"}\n")]


  let rec pexpr_to_string (cexpr:pexpr_t):string=
    match cexpr with Empty ->""
    | Ident(a) ->a
    | Tuple(a) ->String.concat "," (List.map pexpr_to_string a)
    | Math_op(a,b,c) ->"("^(pexpr_to_string a)^b^(pexpr_to_string c)^")"
    | Cond_op(a,b,c)-> "("^(pexpr_to_string a)^"?"^
                    (pexpr_to_string b)^":"^(pexpr_to_string c)^")"
  
  let key_maker (t:SR.type_t) (expr:pexpr_t):string=
    match expr with Tuple(a)->if (List.length a=1) 
   	then (pexpr_to_string (List.hd a)) 
    else ((typecast t)^"("^(pexpr_to_string expr)^")")
      |_->failwith "Fail to make key set"

  let args_replace (fn:string) (arg:string list) (name:string) (accv:string):string = 
    let keys=list_maker (List.length arg) name in
    List.fold_left2 (fun f a b->(global_replace (regexp ("MaGiC"^a)) b f)) fn 
    (arg@["accv"]) (keys@[accv])
  
  let key_of_tuple (t:SR.type_t) (expr:pexpr_t):string=
    let elemt=
      match t with SR.TTuple(a)->SR.TTuple(List.rev(List.tl (List.rev a))) 
        	|_->failwith "Fail to get key from a tuple"
    in
    let elem=
      match expr with Tuple(a)->Tuple(List.rev(List.tl (List.rev a))) 
        	|_->failwith "Fail to get key from a tuple"    		
    in
    key_maker elemt elem

  let slice_pattern (elemlist:string list) (expr:pexpr_t list) (pattern:int list)
    :string=String.concat "&&" (List.map2 (fun a b->(pexpr_to_string a)^"=="^
                          (List.nth elemlist b)) expr pattern)
    
  let assg_map (maptype:SR.type_t) (newmap:string) (elemlist:string list):string=
    let t=match maptype with SR.Collection(a)->a
      |_->failwith "Assg_map failure"
    in
    let elem=Tuple(List.map (fun a->Ident a) elemlist)
    in
    newmap^"["^(key_of_tuple t elem)^"]="^(List.hd (List.rev elemlist))^";"
    
  let rec add_tuple (name:string) (t:SR.type_t) (elem:pexpr_t):string=
    match elem with Cond_op(a,b,c)->"if("^(pexpr_to_string a)^"){\n"^
      (add_tuple name t b)^"\n}else{\n"^(add_tuple name t c)^"\n}"
      |Tuple(a)->name^"["^(key_of_tuple t elem)^"]="^(pexpr_to_string 
        (List.hd (List.rev a)))^";"
      |_->failwith "Fail to add a tuple to collection"
        
  let rec add_colt (name:string) (t:SR.type_t) (elem:pexpr_t):string=      
    match elem with Cond_op(a,b,c)->"if("^(pexpr_to_string a)^"){\n"^
      (add_tuple name t b)^"\n}else{\n"^(add_tuple name t c)^"\n}"
      |Ident(a)->name^".push_back("^a^");"
      |_->failwith "Fail to add a collection to collection"        
  
  let pexpr (expr:pexpr_t) (t:SR.type_t):string=
    (pexpr_to_string expr)^";"
  
  let pdecl (t:SR.type_t) (id:data_id) (expr:pexpr_t):string=
    (typecast t)^" "^id^(if expr=Empty then "" else ("="^(pexpr_to_string expr)))^";"
   
  let passg (id:data_id) (expr:pexpr_t):string=
    id^"="^(pexpr_to_string expr)^";"
    
  let passg_one (id:data_id) (keyt:SR.type_t) (key:pexpr_t) (expr:pexpr_t)
    		:string=
    id^"["^(key_maker keyt key)^"]="^(pexpr_to_string expr)^";"
    
  let passg_two (id:data_id) (keyt1:SR.type_t) (key1:pexpr_t) 
    	(keyt2:SR.type_t) (key2:pexpr_t) (expr:pexpr_t):string=
    id^"["^(key_maker keyt1 key1)^"]["^(key_maker keyt2 key2)^"]="^
      	(pexpr_to_string expr)^";"
  
  let pif (p:pexpr_t) (t:string) (e:string):string=
    "if("^(pexpr_to_string p)^"){\n"^
    	t^
    "\n}else{\n"^
    	e^
    "\n}"
    
  let plambda (arg:data_id) (value:pexpr_t) (body:string):string=
    global_replace (regexp arg) (pexpr_to_string value) body
    
  let pforlambda (maptype:SR.type_t) (map:data_id) (args:data_id list)
    		(body:string) (accv:data_id):string=
    let name=name_itr index in
    "for("^(typecast maptype)^"::iterator "^name^"="^map^".begin();"^name^"!="^
    		map^".end();"^name^"++){\n"^
    	(args_replace body args name accv)^
    "\n}"
    
  let padd (colt:data_id) (t:SR.type_t) (expr:pexpr_t):string=
    match t with SR.TTuple _->add_tuple colt t expr
      	|SR.Collection _->add_colt colt t expr
       	|_->failwith "Add what to collection?"
         
  let rec pflatten (t:SR.type_t) (colt:data_id) (newmap:data_id):string=
      match t with SR.Collection(SR.Collection (x))->
        let name=name_itr index in
        "for("^(typecast t)^"::iterator "^name^"="^colt^".begin();"^name^"!="^colt^
      	".end();"^name^"++){\n"^
        	(pflatten (SR.Collection(x)) ("(*"^name^")") newmap)^
        "\n}"
        |SR.Collection(SR.TTuple (x))->
        let name=name_itr index in
        let elemlist=list_maker (List.length x) name in 
        "for("^(typecast t)^"::iterator "^name^"="^colt^".begin();"^name^"!="^colt^
      	".end();"^name^"++){\n"^
        	(assg_map t newmap elemlist)^
        "\n}"
        |_->failwith "Fail to flatten"
          
  let pslice (maptype:SR.type_t) (map:data_id) (newmap:data_id) 
    	(args:data_id list) (values:pexpr_t list) (pattern:int list):string=
    let name=name_itr index in
    let elemlist=list_maker ((List.length args)+1) name in
    "for("^(typecast maptype)^"::iterator "^name^"="^map^".begin();"^name^"!="^
    		map^".end();"^name^"++){\n"^
    	"if("^(slice_pattern elemlist values pattern)^")"^
    		(assg_map maptype newmap elemlist)^
    "\n}"
    
  let pexists (name:data_id) (map:data_id) (keyt:SR.type_t) (key:pexpr_t):string=
    name^"="^map^".find("^(key_maker keyt key)^")!="^map^".end();"
    
  let plookup (name:data_id) (map:data_id) (keyt:SR.type_t) (key:pexpr_t):string=
    name^"="^map^"["^(key_maker keyt key)^"];"
    
  let ptrigger (name:string) (args:data_id list) (body:string):string=
	name^"("^(String.concat "," (List.map (fun x->"double MaGiC"^x) args))^"){\n"^
    	body^
    "\n}"    
    
  let rec stmt_to_string (stmt:stmt_t):string=
    match stmt with Expr(a,b)->pexpr a b
    | Decl(a,b,c)->pdecl a b c 
    | Assg(a,b)->passg a b
    | Assg_one(a,b,c,d)->passg_one a b c d 
    | Assg_two(a,b,c,d,e,f)->passg_two a b c d e f 
    | If(a,b,c)->pif a (String.concat "\n" (List.map stmt_to_string b))
      	(String.concat "\n" (List.map stmt_to_string c))
    | Lambda(a,b,c)->plambda a b (String.concat "\n" (List.map stmt_to_string c))
    | Forlambda(a,b,c,d,e)->pforlambda a b c 
      (String.concat "\n" (List.map stmt_to_string d)) e
    | Add(a,b,c)->padd a b c
    | Flatten(a,b,c)->pflatten a b c
    | Slice(a,b,c,d,e,f)->pslice a b c d e f
    | Exists(a,b,c,d)->pexists a b c d
    | Lookup(a,b,c,d)->plookup a b c d
    | Trigger(a,b,c)->ptrigger a b (String.concat "\n" (List.map stmt_to_string c))
    | Code(a)->a
    
  let code_to_string (code:code_t):string=String.concat "\n" (List.map stmt_to_string
    code)
    
  let output (code:code_t) (out:out_channel): unit = 
    let lines=split (regexp "\n") (code_to_string code) in
    let counter=[|0|] in
    let coutitr counter num=Array.set counter 0 ((Array.get counter 0)+num);() in
    let bf =Buffer.create 262144 in
    	List.iter (fun str->
       		if String.contains str '}' then coutitr counter (-1) else ();
       		Buffer.add_string bf ((String.make (Array.get counter 0) '\t')^str^"\n");
       		if String.contains str '{' then coutitr counter 1 else ()) lines;
    	output_string out (Buffer.contents bf)
        
  let to_string (code:code_t): string = ""

  let debug_string (code:code_t): string = ""

end

module K3CG : K3Codegen.CG = K3C