(*
 * Terminology:
 * -- adaptor: a function to create structured tuples from raw input data
 * -- generator: a function to create an adaptor, in essence compiling
 *    adaptors since adaptors should be efficient as they see each packet
 *    coming off the network.
 *
 * Standard adaptors.
 * -- defines a set of parameters and transformations for use in all adaptors,
 *     i.e. a library of adaptor functions.
 * -- defines two generators to build adaptors:
 *   ++ csv generator
 *   ++ orderbook generator
 *
 *)


open M3
open Values
open Sources

exception AbortEventConstruction

(* Standard parameters/transformations for all adaptors *)

(* param_val: delimiter *)
let field_tokens param_val =
   let delim_re = Str.regexp (Str.quote param_val)
   in (fun tuple -> Str.split delim_re tuple)

(* param_val: comma-separated field offsets, i.e. "0,4,8,12" *)
let offset_tokens param_val =
   let offsets = List.map int_of_string (Str.split (Str.regexp ",") param_val) in
   if List.length offsets = 0 then
      failwith "Offset tokenizer: no offsets specified!"
   else 
      let aux tuple (prev_off, acc) off =
         try (off, acc@[String.sub tuple prev_off (off - prev_off)])
         with Invalid_argument(_) ->
            failwith ("Offset tokenizer: invalid offsets "^
               (string_of_int prev_off)^", "^(string_of_int off))
      in (fun tuple -> snd (List.fold_left (aux tuple)
         ((List.hd offsets), []) ((List.tl offsets)@[String.length tuple])))
 
(* Tokenizers: param key, tokenize fn list *)
let tokenizers : (string * (string -> string -> string list)) list =
   [("fields", field_tokens); ("offsets", offset_tokens)]

(* param_val: field indexes to project down to, i.e. "0,2,3" *)
let preproject param_val =
   let field_ids =
      List.map int_of_string (Str.split (Str.regexp ",") param_val)
   in (fun fields -> List.map (List.nth fields) field_ids)

(* param_val: "upper" | "lower" *)
let change_case param_val =
   match param_val with
    | "upper" -> (fun fields -> List.map String.uppercase fields)
    | "lower" -> (fun fields -> List.map String.lowercase fields)
    | _ -> (fun fields -> fields)

(* param_val: (field, offset, len) list
 * takes substrings of those fields specified in the param val, with the given
 * offsets and lengths. *)
let substring param_val =
   let split_re = Str.regexp "," in
   let spec_re = Str.regexp "[0-9]*,[0-9]*,[0-9]*" in
   let pos = ref 0 in
   let spec = ref [] in
   while Str.string_match spec_re param_val !pos do
      let triple = Str.matched_string param_val in
      let t_fields = List.map int_of_string (Str.split split_re triple) in
      match t_fields with
       | [fid;off;len] ->
         begin
            if List.mem_assoc fid !spec then
               failwith ("Duplicate substring found for field "^(string_of_int fid));
            spec := (fid,(off,len))::(!spec); 
            pos := !pos + (String.length triple)
         end
       | _ -> failwith "invalid specification construction"
   done;
   let aux (counter,acc) f =
      if List.mem_assoc counter !spec then
         let (off,len) = List.assoc counter !spec in
         (counter+1,acc@[String.sub f off len])
      else (counter+1,acc@[f])
   in (fun fields -> snd (List.fold_left aux (0,[]) fields))

let skiplines param_val = 
  let num = ref (int_of_string param_val) in
  (fun fields -> 
    if !num > 0 then (num := !num - 1; raise AbortEventConstruction)
                else fields
  )

let trimwhitespace param_val = 
  (fun fields -> List.map (Str.global_replace (Str.regexp "^ *\\(.*[^ ]\\) *$")
                                              "\\1") fields)

(* Preprocessing: param key, prep fn *)
let preprocessors : (string * (string -> string list -> string list)) list =
   [("project", preproject); ("case", change_case); ("substring", substring);
    ("skiplines", skiplines); ("trimwhitespace", trimwhitespace)]

(* param_val: (int|float|date|hash) list
 * Builds a tuple, i.e. a const_t list from a string list given the expected
 * types of fields.
 * -- if there are more fields than types, we use the last type to construct
 *    remaining fields.
 *) 
(* TODO: better hashing function control *)
let build_tuple param_val =
   let types = Str.split (Str.regexp ",") param_val in
   let build_fns = 
       List.map (fun t -> match t with
        | "int" -> (fun x -> try CFloat(float(int_of_string x)) with 
                    Failure(_) -> failwith ("Could not convert int: '"^x^"'"))
        | "float" -> (fun x -> try CFloat(float_of_string x) with
                    Failure(_) -> failwith ("Could not convert float: '"^x^"'"))
        | "date" -> (fun x -> 
            if (Str.string_match 
                  (Str.regexp "\\([0-9]+\\)-\\([0-9]+\\)-\\([0-9]+\\)") x 0)
            then (
               let y = (int_of_string (Str.matched_group 1 x)) in
               let m = (int_of_string (Str.matched_group 2 x)) in
               let d = (int_of_string (Str.matched_group 3 x)) in
                  if (m > 12) then failwith (
                        "Invalid month ("^(string_of_int m)^
                        ") in date: "^x
                     );
                  if (d > 31) then failwith (
                        "Invalid day ("^(string_of_int d)^
                        ") in date: "^x
                     );
                  CFloat(float((y * 10000) + (m * 100) + (d * 1)))
            ) else failwith ("Invalid date string: "^x)
         )
        | "hash" -> (fun x -> CFloat(float(Hashtbl.hash x)))
        | _ -> failwith ("invalid const_t type "^t)) types in
   let num_fns = List.length build_fns in
   let extend_fn = List.hd (List.rev build_fns) in
   let rec sub acc l off len =
      if len = 0 then acc
      else if off > 0 then sub acc (List.tl l) (off-1) len 
      else sub (acc@[List.hd l]) (List.tl l) off (len-1) in
   if num_fns = 1 then (fun fields -> List.map extend_fn fields)
   else 
   (fun fields ->
      let nf = List.length fields in
      let common =
         if nf = num_fns then List.map2 (fun bf f -> bf f) build_fns fields
         else if nf = 0 then
           raise AbortEventConstruction (* no fields => no event *)
         else if nf < num_fns then
           List.map2 (fun bf f -> bf f) (sub [] build_fns 0 nf) fields
         else List.map2 (fun bf f -> bf f) build_fns (sub [] fields 0 num_fns)
      in
      let extension = if nf > num_fns then
         List.map extend_fn (sub [] fields num_fns (nf - num_fns)) else []
      in common@extension)

(* Constructors: param key, const fn *)
let constructors : (string * (string -> string list -> const_t list)) list =
   [("schema", build_tuple)]

(* TODO: precision management transformations *)

(* param_val: field ids to project *)
let postproject param_val =
   let field_ids =
      List.map int_of_string (Str.split (Str.regexp ",") param_val)
   in (fun fields -> List.map (List.nth fields) field_ids)

(* Post processing: param key, postp fn *)
let postprocessors : (string * (string -> const_t list -> const_t list)) list =
   [("postproject", postproject)]

let constant_event param_val =
  match param_val with
    | "insert" -> (fun fields -> [(Insert, fields)])
    | "delete" -> (fun fields -> [(Delete, fields)])
    | _ -> failwith ("invalid event type "^param_val)

let parametrized_event param_val =
  let rules = List.map (fun rule ->
      match (Str.split (Str.regexp ":") rule) with 
        | [k;"insert"] -> (CFloat(float(Hashtbl.hash k)), Insert)
        | [k;"delete"] -> (CFloat(float(Hashtbl.hash k)), Delete)
        | _ -> failwith ("invalid event type field: "^rule)
    ) (Str.split (Str.regexp ",") param_val)
  in
    (fun fields -> 
      if (List.length fields) <= 0 then raise AbortEventConstruction
      else try
        [(List.assoc (List.hd fields) rules, List.tl fields)]
      with Not_found -> failwith ("No rule for identifying insert/delete")
    )

(* TODO: think of more complex event construction primitives?
 * -- determining event type from a field, equality, general comparison, etc *)

let event_constructors : (string * (string -> const_t list -> event list)) list =
   [("eventtype", constant_event); ("events", parametrized_event)]

(* Standard generator, applying above transformations *)
let standard_generator params =
   let match_keys l = List.map (fun (k,v) -> (List.assoc k l) v)
      (List.filter (fun (k,v) -> List.mem_assoc k l) params) in
   let match_unique_default l fn_class default =
      let m = match_keys l in match m with
       | [f] -> f
       | [] -> default
       | _ -> failwith ("Multiple "^fn_class^"s specified for CSV adaptor")
   in
   let match_unique l fn_class =
      let m = match_keys l in match m with
       | [f] -> f
       | [] -> failwith ("No "^fn_class^" specified for CSV adaptor")
       | _ -> failwith ("Multiple "^fn_class^"s specified for CSV adaptor")
   in
   let token_fn = 
     match_unique_default tokenizers "tokenizer" (field_tokens ",") in
   let prep_fns = match_keys preprocessors in
   let const_fn = match_unique constructors "constructor" in
   let post_fns = match_keys postprocessors in
   let event_const_fn = 
     match_unique_default event_constructors "event constructor" 
                          (constant_event "insert")
   in
   (fun tuple ->
      (*print_endline tuple;*)
      try
        ( 
          let pre_fields = List.fold_left
             (fun acc_fields prf -> prf acc_fields) (token_fn tuple) prep_fns in
          let post_fields = List.fold_left
             (fun acc_fields postf -> postf acc_fields) (const_fn pre_fields) post_fns
          in event_const_fn post_fields
        )
      with AbortEventConstruction -> []
    ) 

(* Generic CSV, w/ user-defined field delimiter *)
let csv_params delim schema event =
   let schema_val = if schema = "" then "float" else schema in
   let event_val = if event = "" then "insert" else event in
   ("csv", [("fields", delim); ("schema", schema_val); ("event", event_val) ])

let insert_csv delim = csv_params delim "" ""
let delete_csv delim = csv_params delim "" "delete"

let csv_generator = standard_generator
let csv_generator_wrapper default_params extra_params =
  let p = (List.filter (fun (k,v) ->
    not(List.mem_assoc k extra_params)) default_params)@extra_params
  in csv_generator p

(* Order books *)
(* TODO: generalize usage of fixed order book schema through field bindings *)
let bids_params = [("book", "bids")]
let asks_params = [("book", "asks")]

let orderbook_generator params =
   let get_float_const c =
      match c with | CFloat(x) -> x (*| _ -> failwith "invalid float const"*) in 
   let required = ["book"] in
   let optional = [("brokers", "0")] in
   
   (* Get required params *)
   let valid = List.for_all (fun k -> List.mem_assoc k params) required in
   if not(valid) then
      List.iter (fun k -> if not(List.mem_assoc k params) then
         failwith ("Orderbook generator missing required param "^k)) required;
         
   (* Get optional params *)
   let optionals = List.map (fun (k,default) -> 
      if List.mem_assoc k params
      then (k, List.assoc k params) else (k,default)) optional in
   
   (* Configuration *)
   let book_type = List.assoc "book" params in

   (* Only add broker ids if # brokers > 0 *)
   let num_brokers = try
      let r = int_of_string (List.assoc "brokers" optionals) in
      print_endline ("Orderbook # brokers: "^(string_of_int r)); r
      with Invalid_argument _ -> 0 in
   
   let book_orders = Hashtbl.create 1000 in
   
   (* returns action, order_id, price vol pair if # brokers <= 0
    * otherwise action, order_id, broker_id-price-volume triple *)
   let ff l i = float_of_string (List.nth l i) in
   let fi l i = int_of_string (List.nth l i) in
   let get_order_fields tup =
      let t = Str.split (Str.regexp ",") tup in
      let ts,i,a,v,p =
        (ff t 0, fi t 1, List.nth t 2, ff t 3, ff t 4)
      in if num_brokers > 0 then
           let broker_id = 
             if Util.Debug.active "DETERMINISTIC-BROKER" 
             then float_of_int (i mod num_brokers)
             else float_of_int(Random.int(num_brokers))
           in (a,i,[ts;float_of_int i;broker_id;v;p])
         else a,i,[ts;float_of_int i;v;p]
   in
   
   (* helpers for trigger input construction *)
   let v_idx = if num_brokers > 0 then 3 else 2 in
   let p_idx = if num_brokers > 0 then 4 else 3 in
   let getts tuple = List.hd tuple in
   let getoid tuple = List.nth tuple 1 in
   let getbid tuple =
      if num_brokers > 0 then List.nth tuple 2
      else failwith "no broker field found" in
   let getv tuple = List.nth tuple v_idx in
   let getp tuple = List.nth tuple p_idx in

   let insert tuple = (Insert, tuple) in
   let delete tuple = (Delete, tuple) in
   let const_t tuple = List.map (fun x -> CFloat(x)) tuple in
   
   (* Common actions across book types *)
   let adaptor_common action order_id tuple =
      let existing_tuple = if Hashtbl.mem book_orders order_id
         then Some(Hashtbl.find book_orders order_id) else None
      in match (existing_tuple, action) with
       | (None,_) -> []
       
       | (Some(old_tuple), "E") ->
         let subtract x y = c_sum x (CFloat(-.(get_float_const y))) in 
         let new_tuple =
            let nvp =
               [subtract (getv old_tuple) (getv tuple);
                subtract (getp old_tuple) (getp tuple)]
            in [getts old_tuple; getoid old_tuple]@
               (if num_brokers > 0 then [getbid old_tuple] else [])@nvp
         in
         (if (getv new_tuple) = CFloat(0.0)
          then (Hashtbl.remove book_orders order_id; [delete old_tuple])
          else (Hashtbl.replace book_orders order_id new_tuple;
                [delete old_tuple; insert new_tuple]))

       | (Some(old_tuple), "F") | (Some(old_tuple), "D") ->
          Hashtbl.remove book_orders order_id;
          [delete old_tuple]

       | (_,"X") | (_,"C") | (_,"T") -> []
       | (_,_) -> failwith ("invalid orderbook message type "^action)
   in
   let add_to_book order_id tuple =
      let ctuple = const_t tuple
      in Hashtbl.replace book_orders order_id ctuple; [insert ctuple] 
   in
   match book_type with
    | "bids" -> (fun tuple ->
      let (action, order_id, t) = get_order_fields tuple in
      begin match action with
       | "B" -> add_to_book order_id t
       | "S" -> []
       | _ -> adaptor_common action order_id (const_t t)
      end)
    
    | "asks" -> (fun tuple ->
      let (action, order_id, t) = get_order_fields tuple in
      begin match action with
       | "B" -> []
       | "S" -> add_to_book order_id t
       | _ -> adaptor_common action order_id (const_t t)
      end)
    
    | _ -> failwith ("invalid orderbook type: "^book_type)

(* TPC-H *)
let li_schema =
   "int,int,int,int,int,float,float,float,hash,hash,date,date,date,hash,hash,hash"

let lineitem_params = csv_params "|" li_schema "insert"
let order_params    = csv_params "|" "int,int,hash,float,date,hash,hash,int,hash" "insert"
let part_params     = csv_params "|" "int,hash,hash,hash,hash,int,hash,float,hash" "insert"
let partsupp_params = csv_params "|" "int,int,int,float,hash" "insert"
let customer_params = csv_params "|" "int,hash,hash,int,hash,float,hash,hash" "insert"
let supplier_params = csv_params "|" "int,hash,hash,int,hash,float,hash" "insert"
let nation_params   = csv_params "|" "int,hash,int,hash" "insert"
let region_params   = csv_params "|" "int,hash,hash" "insert"

let tpch_generators = List.map (fun (n,(_,p)) -> (n, csv_generator_wrapper p))
  ([("lineitem", lineitem_params); ("orders", order_params);
    ("part", part_params);         ("partsupp", partsupp_params);
    ("customer", customer_params); ("supplier", supplier_params);
    ("nation", nation_params);     ("region", region_params)])

let generators =
   [("csv", standard_generator);
    ("orderbook", orderbook_generator)]@
   tpch_generators

(* TODO: unit tests for generators *)

let initialize() =
   List.iter (fun (x,y) -> Adaptors.add x y) generators