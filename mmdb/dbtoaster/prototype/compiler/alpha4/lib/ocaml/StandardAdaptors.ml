open M3
open M3Common
open M3OCaml

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

(* TODO: preprocessing projection *)

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
   
(* Preprocessing: param key, prep fn *)
let preprocessors : (string * (string -> string list -> string list)) list =
   [("case", change_case); ("substring", substring)]

(* param_val: (int|float) list
 * Builds a tuple, i.e. a const_t list from a string list given the expected
 * types of fields.
 * -- if there are more fields than types, we use the last type to construct
 *    remaining fields.
 *) 
(* TODO: better hashing function control *)
let build_tuple param_val =
   let types = Str.split (Str.regexp ",") param_val in
   let build_fns = List.map (fun t -> match t with
      | "int" -> (fun x -> CFloat(float(int_of_string x)))
      | "float" -> (fun x -> CFloat(float_of_string x))
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
(* TODO: postprocessing projection *)
(* Post processing: param key, postp fn *)
let postprocessors : (string * (string -> const_t list -> const_t list)) list =
   []

let build_event param_val =
   match param_val with
    | "insert" -> (fun fields -> [(Insert, fields)])
    | "delete" -> (fun fields -> [(Delete, fields)])
    | _ -> failwith ("invalid event type "^param_val)

(* TODO: think of more complex event construction primitives?
 * -- determining event type from a field, equality, general comparison, etc *)

let event_constructors : (string * (string -> const_t list -> event list)) list =
   [("event", build_event)]

(* Standard generator, applying above transformations *)
let standard_generator params =
   let match_keys l = List.map (fun (k,v) -> (List.assoc k l) v)
      (List.filter (fun (k,v) -> List.mem_assoc k l) params) in
   let match_unique l fn_class =
      let m = match_keys l in match m with
       | [f] -> f
       | [] -> failwith ("No "^fn_class^" specified for adaptor")
       | _ -> failwith ("Multiple "^fn_class^"s specified for adaptor")
   in
   let token_fn = match_unique tokenizers "tokenizer" in 
   let prep_fns = match_keys preprocessors in
   let const_fn = match_unique constructors "constructor" in
   let post_fns = match_keys postprocessors in
   let event_const_fn = match_unique event_constructors "event constructor" in
   (fun tuple ->
      let pre_fields = List.fold_left
         (fun acc_fields prf -> prf acc_fields) (token_fn tuple) prep_fns in
      let post_fields = List.fold_left
         (fun acc_fields postf -> postf acc_fields) (const_fn pre_fields) post_fns
      in event_const_fn post_fields) 

(* Generic CSV, w/ user-defined field delimiter *)
let csv_params delim schema event =
   let schema_val = if schema = "" then "float" else schema in
   let event_val = if event = "" then "insert" else event in
   ("csv", [("fields", delim); ("schema", schema_val); ("event", event_val) ])

let insert_csv delim = csv_params delim "" ""
let delete_csv delim = csv_params delim "" "delete"

let csv_generator = standard_generator

(* Order books *)
(* TODO: generalize usage of fixed order book schema through field bindings *)
let bids_params = [("book", "bids")]
let asks_params = [("book", "asks")]

let orderbook_generator params =
   let get_float_const c =
      match c with | CFloat(x) -> x | _ -> failwith "invalid float const" in 
   let required = ["book"] in
   let valid = List.for_all (fun k -> List.mem_assoc k params) required in
   if not(valid) then
      List.iter (fun k -> if not(List.mem_assoc k params) then
         failwith ("Orderbook generator missing required param "^k)) required;
   let book_type = List.assoc "book" params in
   let book_orders = Hashtbl.create 1000 in
   (* returns action, order_id, price vol pair *)
   let ff l i = float_of_string (List.nth l i) in
   let get_order_fields tup =
      let t = Str.split (Str.regexp ",") tup in
      (List.nth t 2, int_of_string(List.nth t 1), (ff t 4, ff t 3)) in
   (* helpers for trigger input construction *) 
   let insert pv = (Insert, [fst pv; snd pv]) in
   let delete pv = (Delete, [fst pv; snd pv]) in
   let const_t pv = (CFloat (fst pv), CFloat(snd pv)) in
   let adaptor_common action order_id tup_pv =
      let existing_pv = if Hashtbl.mem book_orders order_id
         then Some(Hashtbl.find book_orders order_id) else None
      in match (existing_pv, action) with
       | (None,_) -> []
       
       | (Some(old_pv), "E") ->
         let subtract x y = c_sum x (CFloat(-.(get_float_const y))) in 
         let new_pv =
            (subtract (fst old_pv) (fst tup_pv),
             subtract (snd old_pv) (snd tup_pv))
         in
         if (fst new_pv) = CFloat(0.0) then Hashtbl.remove book_orders order_id;
         [delete old_pv; insert new_pv]

       | (Some(old_pv), "F") | (Some(old_pv), "D") ->
          Hashtbl.remove book_orders order_id;
          [delete old_pv]

       | (_,"X") | (_,"C") | (_,"T") -> []
       | (_,_) -> failwith ("invalid orderbook message type "^action)
   in
   match book_type with
    | "bids" -> (fun tuple ->
       let (action, order_id, pv) = get_order_fields tuple in
       match action with
        | "B" -> let cpv = const_t pv in
           Hashtbl.replace book_orders order_id cpv; [insert cpv]
        | "S" -> []
        | _ -> adaptor_common action order_id (const_t pv))
    
    | "asks" -> (fun tuple ->
      let (action, order_id, pv) = get_order_fields tuple in
      match action with
       | "B" -> []
       | "S" -> let cpv = const_t pv in
          Hashtbl.replace book_orders order_id cpv; [insert cpv]
       | _ -> adaptor_common action order_id (const_t pv))
    
    | _ -> failwith ("invalid orderbook type: "^book_type)

(* TPC-H *)
let lineitem_params = csv_params "|" "int,int,int,int,float,float,float,float,hash,hash,hash,hash,hash,hash,hash,hash" "insert"
let order_params    = csv_params "|" "int,int,hash,float,hash,hash,hash,int,hash" "insert"
let part_params     = csv_params "|" "int,hash,hash,hash,hash,int,hash,float,hash" "insert"
let partsupp_params = csv_params "|" "int,int,int,float,hash" "insert"
let customer_params = csv_params "|" "int,hash,hash,int,hash,float,hash,hash" "insert"
let supplier_params = csv_params "|" "int,hash,hash,int,hash,float,hash" "insert"

let generators =
   [("csv", standard_generator);
    ("orderbook", orderbook_generator)]

(* TODO: unit tests for standard adaptors *)

let initialize() =
   List.iter (fun (x,y) -> Adaptors.add x y) generators