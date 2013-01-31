open Type
open Constants
open Schema
open K3

type compiler_options = { desugar : bool; profile : bool }

module Common =
struct
  include SourceCode

  let sym_counter = ref 0
  let gensym () = incr sym_counter; "___y"^(string_of_int (!sym_counter))

  (* Source code stringification aliases *)
  let inl x = Inline x
  let inline_var_list sl = List.map (fun x -> inl x) sl
 
  let ssc = string_of_source_code
  let csc = concat_source_code
  let esc = empty_source_code
  let dsc = delim_source_code
  let cdsc = concat_and_delim_source_code
  let cscl = concat_and_delim_source_code_list
  let isc = indent_source_code
  let rec iscn ?(n=1) delim bc =
    if n = 0 then bc else iscn ~n:(n-1) delim (isc delim bc) 
  let ivl = inline_var_list
  let binopsc = binary_op_source_code
  let parensc = parenthesize_source_code

  let sequence ?(final=false) ?(delim=stmt_delimiter) cl =
    cscl ~final:final ~delim:delim (List.filter (fun x -> not(esc x)) cl)

end




(* A C++ compiler target implementation *)
module Target =
struct
  module K = K3

  open Imperative
  open Common
  

  type fields_t = (id_t * ext_type type_t) list
  and ext_type =
    | Type of id_t * (ext_type type_t) option
    | Ref of ext_type type_t
    | Iterator of ext_type type_t

      (* differentiate std::pair from tuple *)
    | Pair of ext_type type_t * ext_type type_t

      (* name, in tier, entry name, primary fields,
       *   list of indices, w/ tag, fields per index *)
    | MultiIndexDef of id_t * bool * ext_type type_t * 
                       fields_t * ((id_t * fields_t) list)

    | TypeDef of id_t * ext_type type_t
    | StructDef of id_t * fields_t
    | EntryStructDef of id_t * fields_t

  type ext_fn = 
    | Apply of string (* Function call *)
    | Constructor of ext_type type_t
    | MemberAccess of string
    | IteratorElement
    | PairFirst
    | PairSecond
    | BeginCollection
    | EndCollection
    | InsertCollection
    | EraseCollection
    | ModifyCollection
    | FindCollection
    | RangeCollection
    | ClearCollection
    | CopyCollection of ext_type decl_t
    | SecondaryIndex of ext_type type_t
    | ConstCast of ext_type type_t
    | BoostLambda of string
    | Inline of string


  type trigger_setup_t =
    (string * string * string * (string * string) list) list *
    (string * int list) list

  type compiler_trig_t = (ext_type type_t, ext_type, ext_fn) Program.trigger_t

  type imp_prog_t =
    (  K3.map_t list *              (* Schema *)
       Patterns.pattern_map *       (* Schema patterns *)
       (  (  source_code_t list *   (* ?? *)
             compiler_trig_t        (* Trigger code *)
          ) list *                  (* List of all triggers =^ *)
          trigger_setup_t           (* Trigger setup code *)
       )                            (* Imperative Program Triggers *)
    ) *                             (* Imperative Program =^ *)
    Schema.t *                      (* Database Schema *)
    (* Toplevel Queries *)
    ( K3.map_t list * 
      ((string * string) * source_code_t) list )

  let empty_prog ():imp_prog_t = 
    (([], Patterns.empty_pattern_map (), ([], ([], []))), 
     Schema.empty_db (), ([],[]))


  (* AST stringification *)
  let rec string_of_ext_type t =
    let quote s = "\""^s^"\"" in
    let sty = Imperative.string_of_type string_of_ext_type in
    let of_tlist tl = String.concat "," (List.map sty tl) in
    match t with 
    | Host _ -> failwith "unhandled host type"
    | Target(Type (id,_)) -> "Target(Type("^(quote id)^"))"
    | Target(Ref(t)) -> "Target(Ref("^(sty t)^"))"
    | Target(Iterator t) -> "Target(Iterator("^(sty t)^"))"
    | Target(Pair (l,r)) -> "Target(Pair("^(of_tlist [l;r])^"))"
    | Target(MultiIndexDef (id,in_tier,entry,primary,secondaries)) ->
        "Target(MultiIndexDef("^(quote id)^","^(sty entry)^"...))"
    | Target(TypeDef (dest,src)) -> 
        "Target(TypeDef("^(quote dest)^","^(sty src)^"))"
    | Target(StructDef (id, fields)) -> "Target(StructDef("^(quote id)^",...))"
    | Target(EntryStructDef (id, fields)) -> 
       "Target(EntryStructDef("^(quote id)^",...))"

  let string_of_ext_fn fn_id =
    let sty = Imperative.string_of_type string_of_ext_type in
    let quote s = "\""^s^"\"" in
    match fn_id with
    | Apply(s) -> "Apply("^(quote s)^")"
    | Constructor(ty) -> "Constructor("^(sty ty)^")"
    | MemberAccess(field) -> "MemberAccess("^(quote field)^")"
    | ConstCast(t) -> "ConstCast("^(sty t)^")" 
    | IteratorElement -> "IteratorElement"
    | PairFirst -> "PairFirst"
    | PairSecond -> "PairSecond"
    | BeginCollection -> "BeginCollection"
    | EndCollection -> "EndCollection"
    | FindCollection -> "FindCollection"
    | ModifyCollection -> "ModifyCollection"
    | InsertCollection -> "InsertCollection"
    | RangeCollection -> "RangeCollection"
    | EraseCollection -> "EraseCollection"
    | ClearCollection -> "ClearCollection"
    | CopyCollection (id,ty) -> "CopyCollection("^(quote id)^","^(sty ty)^")"
    | SecondaryIndex(pat) -> "SecondaryIndex("^(sty pat)^")"
    | BoostLambda(fn) -> "BoostLambda("^(quote fn)^")"
    | Inline(s) -> "Inline("^(quote s)^")"

  let string_of_ext_imp = 
    string_of_typed_imp string_of_ext_type string_of_ext_fn

  (* Misc helpers *)
  let back l = let x = List.rev l in List.rev (List.tl x), List.hd x
  let unique l = List.fold_left
    (fun acc el -> if List.mem el acc then acc else acc@[el]) [] l
  
  let unit = Host TUnit

  let tuple_type_of_list l =
    if List.length l = 1 then List.hd l else TTuple l

  let types_of_host_list l = List.map (fun t -> Host t) l  

  let mk_tuple l =
    if List.length l = 1 then List.hd l
    else "boost::fusion::make_tuple("^(String.concat "," l)^")"

  let mk_tuple_ty l =
    if List.length l = 1 then List.hd l
    else "boost::fusion::tuple<"^(String.concat "," l)^">"


  let imp_type_of_calc_type t = Host(K.TBase(t))

  let string_of_op op = match op with
    | Add -> "+"
    | Mult -> "*"
    | Eq -> "=="
    | Neq -> "!="
    | Lt -> "<"
    | Leq -> "<="
    | Assign -> " = "
    | _ -> failwith "unhandled op"


module Typing =
struct

  let rec string_of_type (t:ext_type Imperative.type_t) : string = 
     (ssc (source_code_of_type t))
  and 
  source_code_of_type (t:ext_type Imperative.type_t) : source_code_t =   
  begin
    let of_list l = String.concat "," (List.map string_of_type l) in
    let of_host_list l = of_list (types_of_host_list l) in
    
    let struct_fields fields = String.concat " "
      (List.map (fun (m,ty) -> (string_of_type ty)^" "^m^";") fields)
    in

    (* TODO: out tier entry constructor for map updates *)
    let source_code_of_entry_struct_def id fields =
      let str_fields = struct_fields fields in
      let ctor =
        let pair_id = "p" in
        let last = List.length fields - 1 in
        let tuple_member i p = if last <> 1 then "at_c<"^i^">("^p^")" else p in
        let field_types, field_names, direct_ctor, direct_assign, pair_assign =
          let a,b,c,d,e = snd (List.fold_left
            (fun (i, (ty_acc, m_acc, c1_acc, a1_acc, c2_acc)) (m,ty) ->
              let ty_s = string_of_type ty in
              let i_s = string_of_int i in
              let cf = "c"^i_s in (i+1,
                (ty_acc@[ty_s],
                 m_acc@[m],
                 c1_acc@[ty_s^" "^cf],
                 a1_acc@[m^" = "^cf],
                 c2_acc@[m^" = "^(if i <> last
                          then tuple_member i_s (pair_id^".first")
                          else pair_id^".second")])))
            (0,([],[],[],[],[])) fields)
          in a, b, String.concat "," c,
              ((String.concat "; " d)^";"), ((String.concat "; " e)^";")
        in
        let pair_ctor, pair_conversion =
          if field_types = [] then "","" else
          let k_t,v_t = back field_types in
          let k_m,v_m = back field_names 
          in 
          ("std::pair<const "^(mk_tuple_ty k_t)^", "^v_t^">& "^pair_id),
          ("operator const std::pair<const " ^ (mk_tuple_ty k_t) ^ 
           ", " ^ v_t ^ ">() const " ^
           "{ return std::make_pair("^(mk_tuple k_m)^", "^v_m^"); }")
          
        in
    
        let serialize = [
           "template<class Archive>";
           "void serialize(Archive& ar, const unsigned int version)";
           "{";]@
           (List.map 
              (fun (m,ty) -> tab^"ar & BOOST_SERIALIZATION_NVP("^m^");") 
              fields
           )@
           ["}";]
        in        
        Lines ([id^"("^direct_ctor^") { "^direct_assign^" }";
                id^"("^pair_ctor^") { "^pair_assign^" }";
                pair_conversion;] @ 
               serialize)
               
      in cscl [(inl ("struct "^id^" {")); isc tab (csc (inl str_fields)
               (if str_fields = "" then inl "" else ctor)); inl("};")]
    in

    let source_code_of_struct_def id fields = 
      let str_fields = struct_fields fields
      in cscl [(inl ("struct "^id^" {")); isc tab (inl str_fields); inl("};")]
    in

    (* Creates a C++ type declaration of a map. *)
    let source_code_of_multi_index_def id in_tier entry primary indices =
      let tuple_ty_of_fields fields =
        mk_tuple_ty (List.map (fun (_,ty) -> string_of_type ty) fields) in
      let key_of_fields fields = 
        let str_fields = String.concat "," (List.map (fun (id,ty) ->
          let members = String.concat ","
            [entry; string_of_type ty; ("&"^entry^"::"^id)]
          in "member<"^members^">") fields)
        in if str_fields = "" then ""
           else if List.length fields = 1 then str_fields
           else ("composite_key<"^entry^", "^str_fields^" >")
      in
      let extractor_of_fields idx_id fields =
        let extractor_id = id^"key"^idx_id^"_extractor" in
        let result_type_decl = Lines [
          "typedef "^(tuple_ty_of_fields fields)^" result_type;"] in
        let extractor_method = Lines [
          "result_type operator()(const "^entry^"& e) const {";
          "  return "^(mk_tuple (List.map (fun (id,_) -> "e."^id) fields))^";";
          "}"]
        in extractor_id,
           cscl [(inl ("struct "^extractor_id^" {"));
                 isc tab result_type_decl; isc tab extractor_method; inl("};")]
      in
      let hasher_of_fields idx_id fields = 
        let hasher_id = id^"key"^idx_id^"_hasher" in
        let entry_op = cscl
          [Lines(["size_t operator()(const "^entry^"& e) const {";]);
           isc tab (Lines (["size_t seed = 0;"]@
                           (List.map (fun (id,_) ->
                              "boost::hash_combine(seed, e."^id^");") fields)@
                           ["return seed;"]));
           Lines(["}"])]
        in
        let key_op = Lines ([
          "size_t operator()("^(tuple_ty_of_fields fields)^" k) const {";
          "  return boost::fusion::fold(k, 0, fold_hash());"; "}"])
        in hasher_id,
           cscl [inl ("struct "^hasher_id^" {");
                 isc tab entry_op; isc tab key_op; inl("};")]
      in
      let idx_cnt = ref 0 in
      let primary_def_sc, primary_idx =
        if List.length primary <= 1 then
          (inl ""), ("hashed_unique<"^(key_of_fields primary)^" >")
        else
        let exid,exsc = extractor_of_fields (string_of_int !idx_cnt) primary in
        let hid,hsc = hasher_of_fields (string_of_int !idx_cnt) primary in
          incr idx_cnt;
          (cscl [exsc;hsc]), ("hashed_unique<"^exid^","^hid^">") 
      in
      let idx_def_sc, idx_sc = 
        let x,y = List.fold_left
        (fun (defacc, idxacc) (tag, fields) ->
          if List.length fields <= 1 then
            let i =
              let key = key_of_fields fields in
              Lines(["hashed_non_unique<tag<"^tag^">, "^key^" >"])
            in defacc, idxacc@[i]
          else
          let exid,exsc = extractor_of_fields (string_of_int !idx_cnt) fields in
          let hid,hsc = hasher_of_fields (string_of_int !idx_cnt) fields in
            incr idx_cnt;
            defacc@[exsc;hsc], idxacc@[Lines(
              ["hashed_non_unique<tag<"^tag^">, "^exid^", "^hid^">"])])
        ([], []) indices
        in
        (* Added sequence index for caching. *)
        let seq_def_sc, seq_idx_sc =
          let seq_tag = id^"_seq" in
          if in_tier then
            [inl ("struct "^seq_tag^" {};")],
            [Lines ["sequenced<tag<"^seq_tag^"> >"]]
          else [], []
        in cscl (x@seq_def_sc), (cscl ~delim:"," (y@seq_idx_sc))
      in
      let indexes =
        cscl ([Lines ([primary_idx^(if esc idx_sc then "" else ",")])]@
              (if esc idx_sc then [] else [idx_sc]))
      in cscl [primary_def_sc; idx_def_sc;
               Lines(["typedef multi_index_container<"^entry^", indexed_by<"]);
               isc tab indexes; (inl (" > > "^id^";"))]
    in 

    begin match t with
      Host K.TUnit -> inl ("void")
    | Host(K.TBase(x)) -> inl (cpp_of_type x)
    | Host(K.TTuple(tl)) -> 
        if List.length tl = 1 then inl (of_host_list tl)
        else inl (mk_tuple_ty (List.map string_of_type (types_of_host_list tl)))
    | Host(K.Collection(_,et)) ->
        begin match et with
        | K.TTuple(fields) ->
            let rest, v =
              let x = List.rev fields in List.rev (List.tl x), List.hd x in
            let k = Host(K.TTuple(rest)) in
            let rangle_sep = match v with | K.TTuple _ -> " " | _ -> ""
            in inl ("map<"^(string_of_type k) ^ "," ^
                    (string_of_type (Host v)) ^ rangle_sep ^ ">")
 
        | _ -> let inner_t = string_of_type (Host et) in
               let sep = if inner_t.[String.length inner_t-1] = '>' 
                         then " " else ""
               in inl ("std::list<"^inner_t^sep^">")
        end
    | Host(K.Fn (args, rt)) ->
        inl ("(("^(string_of_type (Host rt))^")*)("^(of_host_list args)^")")

    | Target (ext_type) ->
      begin match ext_type with
        | Type(id,_) -> inl (id)
        | Ref(t) -> inl ((string_of_type t)^"&")
        | Iterator(t) -> inl ((string_of_type t)^"::iterator")
        | Pair(l,r) ->
          let p = of_list [l; r] in 
          let final_rangle = p.[String.length p-1] = '>' in
          inl ("std::pair<"^p^(if final_rangle then " " else "")^">")
        | TypeDef(dest,src) -> inl ("typedef " ^ (string_of_type src) ^
                                    " " ^ dest ^ ";")
        
        | StructDef(id, fields) ->
          source_code_of_struct_def id fields
        
        | EntryStructDef(id, fields) ->
          source_code_of_entry_struct_def id fields
          
        | MultiIndexDef(id, in_tier, 
            Target(Type(entry_id,Some(Target(EntryStructDef(_entry_id,_))))), 
            primary, indices) when entry_id = _entry_id ->
          source_code_of_multi_index_def id in_tier entry_id primary indices
        | MultiIndexDef _ -> failwith "Unsupported MultiIndexDef!"
      end
    end
  end (* source_code_of_type *)
  
  (* variable types *)
  type var_env_t = (id_t * ext_type type_t) list   

  (* Typing helpers *)

  let type_id_of_type t = match t with
    | Target(Type(x,_)) -> x
    | Target(TypeDef(x,_)) -> x
    | Target(MultiIndexDef (id,_,_,_,_)) -> id
    | Target(StructDef (id,_)) -> id
    | Target(EntryStructDef (id,_)) -> id
    | _ -> failwith ("invalid named type "^(string_of_type t))

  let type_decl_of_type t = match t with
    | Target(MultiIndexDef(id,_,entry,_,_)) -> [id,t]
    | Target(TypeDef (dest,src)) -> [dest,src]
    | Target(StructDef (id,_)) -> [id,t]
    | Target(EntryStructDef (id,_)) -> [id,t]
    | _ -> []

  let is_type_alias t = match t with
    | Target(Type(x,Some(_))) -> true
    | _ -> false 

  (* Retrieve a type definition *) 
  let rec type_decl_of t = match t with
    | Target(Type(x,Some(td))) ->
      (* Recursively chase all type aliases to some declaration *) 
      type_decl_of td
    | Target(Type(x,None)) -> failwith ("undeclared type "^(string_of_type t))
    | _ -> t

  (* Note: this is not recursive for now... *)
  (* TODO: check valid subfields of structs, and multi-indexes *)
  let is_type_defined t = match t with
    | Target(Type(_,None)) -> false 
    | _ -> true

  let rec is_ext_collection t = match (type_decl_of t) with
    | Target(MultiIndexDef(_)) -> true
    | _ -> false

  let rec ext_collection_of_type t = match (type_decl_of t) with
    | Target(MultiIndexDef(_)) as td -> Some(td)
    | _ -> None

  let rec field_types_of_type t = match t with
    | Host(TTuple l) -> List.map (fun x -> Host x) l
    | Target(Pair(Host(TTuple(l)),r)) -> 
         (field_types_of_type (Host(TTuple(l))))@[r]  
    | Target(Pair(Host(TBase(_) as l), r)) -> 
         (* functionally a tuple of size 1 *)
         [Host(l); r]
    | Target(StructDef(_, fields)) -> List.map snd fields
    | Target(EntryStructDef(_, fields)) -> List.map snd fields
    | _ -> failwith ("invalid composite type "^(string_of_type t))

  let entry_type_of_collection t =
    let error() = failwith ("invalid collection "^(string_of_type t)) in 
    let c_t = match ext_collection_of_type t with
      | None -> error() | Some(t) -> t
    in match c_t with
    | Target(MultiIndexDef(t_id,_,entry_t,_,_)) -> entry_t
    | _ -> error()

  let fields_of_entry_type entry_ty =
     begin match type_decl_of entry_ty with
      | Target(EntryStructDef(_, fields)) -> fields
      | _ -> failwith ("Unsupported entry type: "^(string_of_ext_type entry_ty))
     end
            
  let field_types_of_collection t = match t with
    | Target(MultiIndexDef(t_id,_,entry_t,_,_)) ->
        if is_type_defined entry_t then
          let def = type_decl_of entry_t in field_types_of_type def 
        else failwith ("invalid collection entry "^(string_of_type entry_t))
    | _ -> failwith ("invalid collection "^(string_of_type t))

  let index_of_collection t pat = match t with
    | Target(MultiIndexDef(t_id,_,_,_,indices)) ->
        let sfx = String.concat "" (List.map string_of_int pat) in
        let pat_sfx = "_pat"^sfx in
        let pat_sfx_len = String.length pat_sfx in
        let rp,ri = List.fold_left (fun (pacc,iacc) (id,_) ->
          let id_len = String.length id in
          if iacc <> "" then (pacc,iacc)
          else if id_len > pat_sfx_len
                && (Str.string_after id (id_len-pat_sfx_len)) = pat_sfx
          then id, ((Str.string_before id (id_len-pat_sfx_len))^"_index"^sfx)
          else (pacc,iacc)) ("","") indices
        in if rp <> "" && ri <> "" 
           then Target(Type(rp,None)), Target(Type(ri,None))
           else failwith ("no index with id \""^sfx^"\" found for type "^
                          (string_of_type t))
    | _ -> failwith ("collection index request on type "^(string_of_type t))

  let rec host_type_of_type t =
    begin match t with
      | Host _ -> t
      | _ when is_ext_collection t ->
        Host(Collection(Unknown, host_type
          (host_type_of_type (entry_type_of_collection t)))) 
      | _ ->
        let entry_fields = field_types_of_type (type_decl_of t)
        in Host(TTuple(List.map (fun f_t ->
            host_type (host_type_of_type f_t)) entry_fields))
    end

  (* Returns a multi_index_container type declaration for a global map, incl.
   * -- structs for map in tier and out tier entries.
   * -- multi_index_container types for in and out tiers.
   * -- pattern tag types and index typedefs.
   *)
  let type_of_map_schema patterns ((id, in_vl, out_vl, mapt):K3.map_t) =
    let k3vartype_of_m3vartype t = TBase(t) in
    let fields_of_var_types vl = snd (List.fold_left (fun (i,acc) (v,ty) ->
          (i+1, acc@[v, Host (k3vartype_of_m3vartype ty)]))
        (0,[]) vl)
    in
    let aux () =
      let mk_mindex t_id in_tier entry_t_decl idx_meta =
        let key_fields = match entry_t_decl with
          | Target(EntryStructDef(id,fields)) ->
              (List.rev (List.tl (List.rev fields)))
          | _ -> failwith "invalid multi-index entry"
        in 
        let pat_decl = List.map (fun (x,_,_) -> x) idx_meta in
        let idx_decl = List.map (fun (_,y,_) -> y) idx_meta in
        let idx_pat  = List.map (fun (_,_,z) -> z) idx_meta in
        let t_decl =
          Target(MultiIndexDef(t_id, in_tier, 
            Target(Type(type_id_of_type entry_t_decl,Some(entry_t_decl))),
            key_fields, idx_pat))
        in
        (* Type declaration for an insertion order index *)
        let seq_idx_decl =
          let seq_tag = t_id^"_seq" in
          let seq_t_id, seq_t =
            t_id^"_index_seq", 
            Target(Type(t_id^"::index<"^seq_tag^">::type",None)) in
          if in_tier then [Target(TypeDef(seq_t_id,seq_t))] else []
        in
        Target(Type(t_id,Some(t_decl))),
        ([entry_t_decl]@pat_decl@[t_decl]@idx_decl@seq_idx_decl)
      in
      
      let t_id           = id^"_map" in
      let entry_t_id     = id^"_entry" in
      
      let  in_fields = fields_of_var_types  in_vl in
      let out_fields = fields_of_var_types out_vl in
      let val_field  = "__av", Host(TBase(mapt)) in
        
      (* In/out pattern index and type lists *)
      let in_pat_its, out_pat_its = 
        let id_pats =
          if not(List.mem_assoc id patterns) then []
          else (List.assoc id patterns) in
        let it_of_idx l idxl = List.combine idxl (List.map (List.nth l) idxl) in
        let r = List.fold_left (fun (in_acc, out_acc) p -> match p with
            | Patterns.In(v,i) -> in_acc@[it_of_idx in_vl i], out_acc
            | Patterns.Out(v,i) -> in_acc, out_acc@[it_of_idx out_vl i])
          ([], []) id_pats
        in unique (List.filter (fun x -> x <> []) (fst r)),
           unique (List.filter (fun x -> x <> []) (snd r))
      in
      let mk_pat t_id pfx p =
          let id_of_idxl l = String.concat "" (List.map string_of_int l) in
          let mk_idx_field (i,(v,ty)) = v, Host(k3vartype_of_m3vartype ty) 
          in
     
          let pat_sfx = id_of_idxl (List.map fst p) in
          let pat_id = id^pfx^"_pat"^pat_sfx in
          let idx_fields = List.map mk_idx_field p in
          let idx_t_id, idx_t =
              (id^pfx^"_index"^pat_sfx), 
              Target(Type(t_id^"::index<"^pat_id^">::type",None))
          in
          Target(StructDef(pat_id, [])), 
          Target(TypeDef(idx_t_id, idx_t )),
          (pat_id, idx_fields)
      in
      
      (* Single tier maps only have entry_t defined *)
      let entry_t, entry_idx_meta, out_mindex_decls  =
         match in_fields, out_fields with
          | [],  _ -> 
            Target(EntryStructDef(entry_t_id, out_fields@[val_field])),
            List.map (mk_pat t_id "") out_pat_its,
            []
          |  _, [] -> 
            Target(EntryStructDef(entry_t_id,  in_fields@[val_field])),
            List.map (mk_pat t_id "") in_pat_its,
            []
          |  _,  _ ->
            let out_entry_t_id = id^"_out_entry" in
            let out_entry_t  = 
               Target(EntryStructDef(out_entry_t_id, out_fields@[val_field]))
            in
            let out_t_id       = id^"_out_map" in 
            let out_idx_meta =  
               List.map (mk_pat out_t_id "_out") out_pat_its 
            in
            let out_mindex_t, _out_mindex_decls = 
               mk_mindex out_t_id false out_entry_t out_idx_meta
            in
            Target(EntryStructDef(entry_t_id, in_fields@["__av",
                                  out_mindex_t])),
            List.map (mk_pat t_id "_in") in_pat_its,
            _out_mindex_decls
      in
      let mindex_t, mindex_decls =
        mk_mindex t_id (out_mindex_decls <> []) entry_t entry_idx_meta
      in 
      id, mindex_t, (out_mindex_decls@mindex_decls)
    in 
    if (in_vl@out_vl) = [] then (id, Host(TBase(mapt)), []) else aux()


  (* Returns a var env containing global variables *)
  let var_env_of_declarations arg_types (schema: K3.map_t list) patterns =
     List.fold_left (fun vacc (id,t,t_decls) -> vacc@[id,t]) 
      (arg_types) (List.map (type_of_map_schema patterns) schema)

  (* A typing function that can be invoked when building an imperative AST
   * to ensure leaf map variables have external/native types for this
   * target language. *)
  (*
  let ext_type_of_k3_collection e = match e with
    | SingletonPC (id,_) -> Host TFloat
    | InPC (id,_,_) | OutPC (id,_,_) | PC (id,_,_,_) -> Target(Type(id^"_map"))
    | _ -> failwith "invalid persistent k3 collection"
  *)


  (* Partial type inferencing. The partial refers to the fact this is only to
   * be applied to untyped imperative statements generated from annotated K3
   * expressions. Such statements do not contain any external function calls,
   * and as such we omit inferring types for such external functions (throwing
   * an exception).
   * This method can be used to construct a typed versions of imp_t and expr_t
   * from an untyped AST, which can then be desugared to rewrite K3 function
   * calls into external functions corresponding to this imperative target
   * language.
   *
   * Currently this method does not do rigorous type-checking and inference,
   * rather should be viewed as a typing constructor, decorating the AST with
   * type information. *)
  let rec infer_types init_env untyped_imp =
    let extract_imp opt_pair = match opt_pair with
      | Some(i), None -> i
      | _, _ -> failwith "invalid child imperative statement"
    in 
    let extract_expr opt_pair = match opt_pair with
      | None, Some(e) -> e
      | _,_ -> failwith "invalid child expression"
    in
    let sot = string_of_type in
    let rec promote_types t1 t2 =
      match t1,t2 with
      | Target(Type(_)), Host(Collection(_)) -> t1
      | Target(Type(_)), Host(TTuple(_)) -> t1
      | Host(TTuple(l1)), Host(TTuple(l2)) ->
        (*
        let f x = Host(x) in
        let l = List.map host_type
          (List.map2 promote_types (List.map f l1) (List.map f l2))
        in Host(TTuple(l))
        *)
        t1
      | Host(TBase(bt1)), Host(TBase(bt2)) -> 
         Host(TBase(Type.escalate_type bt1 bt2))
      | _, _ when t1 = t2 -> t1
      | _, _ -> failwith ("incompatible types "^(sot t1)^" "^(sot t2))
    in
    let promote_types_op op t1 t2 = match t1, t2 with
      | Host(TBase(bt1)), Host(TBase(bt2)) -> 
         Host(TBase(Type.escalate_type bt1 bt2))
      | _, _ when t1 = t2 -> t1
      | _, _ -> failwith ("incompatible types for op "^
                           (string_of_op op)^" "^(sot t1)^" "^(sot t2))
    in
    let add_env_var type_env id ty = 
      let vars = !type_env in
      let new_vars =
        (* Override existing type bindings with declarations *) 
        ((id,ty)::(if List.mem_assoc id vars
                   then List.remove_assoc id vars else vars))
       in type_env := new_vars;
          type_env
    in

    let infer_fn_type fn_id arg_types = match fn_id with
      | TupleElement j ->
        let t = List.hd arg_types in begin match t with
        | Host(TTuple l) -> Host (List.nth l j)
        | _ -> failwith ("invalid tuple type "^(string_of_type t))
        end

      | Projection idx ->
        Host (TTuple (List.map host_type (List.map (List.nth arg_types) idx)))
      | Singleton -> Host (Collection(Unknown, (host_type (List.hd arg_types))))
      | Combine ->
        List.fold_left 
           (fun l r -> if l = r 
                       then l 
                       else failwith "incompatible combine arguments")
           (List.hd arg_types)
           (List.tl arg_types)
        

        (* TODO: check schema compatability of maps and args for all the
         * following map operations.
         * Checking external types means we must be able to look up struct defs,
         * i.e. the type environment should contain type id=>type def mappings
         * as well as id=>type mappings. Although our language fragment does
         * not allow users to define their own types, this could easily be
         * supported via side-effecting external imperative statements. *)

      | Member ->
        (* TODO: change to a boolean type *)
        let t = List.hd arg_types in 
        let error() = 
          failwith ("invalid collection type for member "^(string_of_type t))
        in begin match t with
        | Host (Collection(_, (TTuple(_)))) -> Host(TBase(TInt))
        | Target(_) when is_ext_collection t -> Host(TBase(TInt))
        | _ -> error()
        end

      | Lookup -> 
        let t = List.hd arg_types in 
        let error () = failwith ("invalid collection type "^(string_of_type t))
        in begin match t with
        | Host (Collection(_, (TTuple l))) -> let _,t = back l in Host t
        | Target(_) when is_ext_collection t ->
          let t_fields = match ext_collection_of_type t with
            | Some(c_t) -> field_types_of_collection c_t
            | _ -> error()
          in snd (back t_fields)
        | _ -> error()
        end

      | Slice (idx) ->
        let t = List.hd arg_types in
        let error () =
          failwith ("invalid collection type for slicing "^(string_of_type t)) 
        in begin match t with
        | Host (Collection(_, (TTuple l))) ->
            if (List.length l) <= (List.fold_left max 0 idx) then
              failwith "invalid slice indices"
            else t
        | Target(_) when is_ext_collection t -> host_type_of_type t
        | _ -> error()
        end

      | ConcatElement -> unit
      | Concat -> unit
      | MapUpdate -> unit
      | MapValueUpdate -> unit
      | MapElementRemove -> unit
      | External(id,arg_t,r_t) ->
        let valid =
          match arg_t, arg_types with
          | AVar _, [t] ->
            (List.map host_type arg_types) = (K.types_of_arg arg_t)
          
          | ATuple f_t, [Host(TTuple(x))]
              when List.length f_t = List.length x ->
            x = (K.types_of_arg arg_t)
            
          | ATuple f_t, x when List.length f_t = List.length x ->
            (List.map host_type arg_types) = (K.types_of_arg arg_t)
          | _ -> false
        in 
        if valid then Host(r_t)
        else failwith ("invalid "^id^" external function in imp : "^
                       (ListExtras.ocaml_of_list K.string_of_type
                                                 (K.types_of_arg arg_t))^
                       " vs "^
                       (ListExtras.ocaml_of_list string_of_type arg_types))
      | Ext _ -> failwith "external function appeared in untyped imp"
    in
    let infer_expr env c_opts untyped_e =
      let cexpri i = extract_expr (List.nth c_opts i) in
      let ctypei i = type_of_expr_t (cexpri i) in
      let cexprs() = List.map extract_expr c_opts in
      let ctypes() = List.map type_of_expr_t (cexprs()) in
      match untyped_e with
      | Const (_,c) -> 
         let base_type = 
            begin match c with 
               | CString(_) when Debug.active "HASH-STRINGS" -> TInt
               | _ -> Constants.type_of_const c
            end
         in (None, Some(Const(Host(TBase(base_type)),c)))

      | Var (_,(v,t)) ->
        let defined_type = match t with
          | Target(Type(x,None)) -> false 
          | _ -> true
        in
        let declared_type =
          try List.assoc v !env
          with Not_found -> 
            print_endline ("no declaration found for "^v^" in: "^
                           (ListExtras.ocaml_of_list fst !env));
            failwith ("no declaration found for "^v)
        in
        begin match defined_type, declared_type with
        | false, _ -> failwith ("undefined type "^(sot t))

        | true, _ when t <> declared_type ->
          begin try 
            let override_t = promote_types declared_type t
            in None, Some(Var(override_t,(v,override_t)))
          with Failure _ ->
            print_endline ("type mismatch for var \""^v^"\": "^
                           (sot t)^" vs "^(sot declared_type)); 
            failwith ("type mismatch for var \""^v^"\": "^
                      (sot t)^" vs "^(sot declared_type))
          end

        | true, _ -> None, Some(Var(t,(v,t)))
        end

      | Tuple(_,l) ->
        let t = Host (TTuple(List.map host_type (ctypes())))
        in None, Some(Tuple(t, (cexprs()))) 

      | Op(_,op,ce) -> 
        let t = match op with
          | UMinus -> ctypei 0
          | _ -> failwith "invalid unary operator" 
        in None, Some(Op(t,op,cexpri 0))

      | BinOp(_,op,le,re) ->
        let t = match op with
          | Assign -> unit
          | If0 -> ctypei 1
          (* TODO: change to boolean type *)
          | Eq | Neq | Lt | Leq -> Host(TBase(TInt)) 
          | _ -> promote_types_op op (ctypei 0) (ctypei 1)    
        in None, Some(BinOp(t, op, cexpri 0, cexpri 1))

      | Fn(_,fn_id,args) ->
        let t = infer_fn_type fn_id (ctypes())
        in None, Some(Fn(t, fn_id, (cexprs())))
    in
    let infer_imp env c_opts untyped_i =
      let cexpri i = extract_expr (List.nth c_opts i) in
      let cimpi i = extract_imp (List.nth c_opts i) in
      match untyped_i with
      | Expr _ ->
        let t = type_of_expr_t (cexpri 0)
        in Some(Expr(t, (cexpri 0))), None

      | Block (_, []) -> Some(Block(unit, [])), None

      | Block _ ->
        let _,r = back c_opts in
        let t = type_of_imp_t (extract_imp r)
        in Some(Block(t, List.map extract_imp c_opts)), None

      | Decl (_,((id,ty) as d),init) ->
        if init = None then Some(Decl(unit, d, None)), None
        else let t = type_of_expr_t (cexpri 0) in
             if t <> ty then ignore(add_env_var env id t);
             Some(Decl(unit, (id,t), Some(cexpri 0))), None

      | For (_,((id,ty),f),_,l) ->
        let s_t = type_of_expr_t (cexpri 0) in
        let l_t = type_of_imp_t (cimpi 1) in
        let new_l, el_t =
          let el_t = match s_t with
            | Host(Collection(_, x)) -> Host(x)
            | Target(_) when is_ext_collection s_t ->
              entry_type_of_collection s_t
            | _ -> failwith "invalid collection in loop"
          in 
          if el_t = ty then cimpi 1, ty else
             begin
              ignore(add_env_var env id el_t);
              (*infer_types !type_env l*)
              cimpi 1, el_t
             end
        in Some(For(l_t,((id,el_t),f), (cexpri 0), new_l)), None

      | IfThenElse(_,_,_,_) ->
        let t =
          let lt = type_of_imp_t (cimpi 1) in
          let rt = type_of_imp_t (cimpi 2) in
          if lt = rt then lt else failwith "incompatible if-then-else types"
        in Some(IfThenElse(t, cexpri 0, cimpi 1, cimpi 2)), None 
    in
    let build_env_expr env untyped_e = env in
    let build_env_imp env untyped_i =
        match untyped_i with
        | Block (_,l) -> ref (!env)

        | Decl (_,(id,ty),_) -> add_env_var env id ty
            
        | For (_,elem,source,Decl _) ->
          failwith "invalid declaration as loop body"
        
        | For (_,((id,ty),_),_,_) -> add_env_var env id ty

        | IfThenElse(_,_,Decl _,_) | IfThenElse(_,_,_,Decl _) ->
          failwith "invalid declaration as condition branch"
        
        | _ -> env 
    in
    let r_opt_pair =
      fold_imp_traced infer_imp infer_expr build_env_imp build_env_expr
        (ref(init_env)) (None,None) untyped_imp
    in extract_imp r_opt_pair
    
end (* Typing *)

  open Typing

  let rec source_code_of_fn fn_id arg_exprs return_type =
    let args, arg_types = List.split (List.map (fun e ->
        ssc (source_code_of_expr e), type_of_expr_t e) arg_exprs)
    in
    let argi i = List.nth args i in
    let argti i = List.nth arg_types i in
    let of_list l = String.concat "," l in
    let a = of_list args in
    let string_of_int_list il = String.concat "," (List.map string_of_int il) in
    match fn_id with
    | TupleElement(idx) -> inl("at_c<"^(string_of_int idx)^">("^a^")")
    | Projection(idx) -> inl("project<"^(string_of_int_list idx)^">("^a^")")

    (* Singleton and combine operations should not exist after desugaring *)
    | Singleton -> inl("!!!error!!!singleton("^a^")")
    | Combine -> inl("!!!error!!!combine("^a^")")
    
    (* Map operations should not exist after desugaring.
     * For now we dump out erroneous code to debug locatinos rather than
     * throwing an exception. *)  
    | Member -> inl("!!!error!!!"^
      (argi 0)^".find("^
        (mk_tuple (List.tl args))^") != "^(argi 0)^".end()")
    
    | Lookup -> inl("!!!error!!!"^
        "(("^(argi 0)^".find("^(mk_tuple (List.tl args))^"))->__av)")

    | Slice(idx) -> inl("!!!error!!!"^
        (argi 0)^".slice<"^(of_list (List.map string_of_int idx))^">("^
            (of_list (List.tl args))^")")

      (* TODO: implement for unoptimized K3, except with C++ tuple lists rather
       * than maps *)
    | ConcatElement ->
        begin match argti 0 with
          | Host (Collection(_, TTuple(_))) ->
            let k,v = match List.nth arg_exprs 1 with
              | Tuple (_,fields) ->
                back (List.map (fun e -> ssc (source_code_of_expr e)) fields)
              | Var(_,(id,_)) ->
                let tuple_of_var x l = snd (List.fold_left (fun (i,acc) _ ->
                    (i+1, acc@["at_c<"^(string_of_int i)^">("^x^")"]))
                  (0,[]) l)
                in
                begin match argti 1 with
                | Host(TTuple(l)) -> back (tuple_of_var id l)
                | Target(Pair(Host(TTuple(l)),_)) ->
                  (tuple_of_var (id^".first") l), (id^".second") 
                | Target(Pair(Host(TBase(_)),_)) ->
                  [(id^".first")],(id^".second")
                | _ -> failwith "invalid tuple collection append"
                end 
              | _ -> failwith "invalid tuple collection append"
            in
            inl((argi 0)^".insert("^(argi 0)^".end(), "^
                              "make_pair("^(mk_tuple k)^","^v^"))")
          
          | Host (Collection _) ->
            inl((argi 0)^".push_back("^(mk_tuple (List.tl args))^")")
            
          | _ -> failwith ("invalid element concatenation to return type "^
                            (string_of_type return_type))
        end

    | Concat ->
        let lhs_arg  = argi  0 in
        let lhs_argt = type_decl_of (argti 0) in
        let lhs_argt_sc = string_of_type (argti 0) in
        Lines (List.fold_left 
            (fun acc (rhs_arg, rhs_argt) ->
             let rhs_argt_sc = string_of_type rhs_argt in
             let rhs_tuple_value = 
                 begin match type_decl_of rhs_argt with
                   | Host (Collection(_, TTuple(_))) -> "second"
                   | Target (MultiIndexDef _) -> "__av"
                   | _ -> 
                       failwith ("invalid concatenation of: " ^
                                 rhs_arg ^ " : " ^ rhs_argt_sc)
                 end
             in
             let modify_lhs = 
                begin match lhs_argt with
                  | Host (Collection(_, TTuple(_))) ->
                     "ret.first->second += combine_it->"^rhs_tuple_value
                  | Target (MultiIndexDef (_,_,entry_t,_,_)) ->
                    let new_value = 
                       "ret.first->__av + combine_it->" ^
                       rhs_tuple_value 
                    in
                       lhs_arg ^ ".modify( ret.first, boost::lambda::bind(&" ^
                       (type_id_of_type entry_t) ^
                       "::__av, boost::lambda::_1) = " ^ new_value ^ " )"
                  | _ -> 
                     failwith ("invalid concatenation of: " ^
                               lhs_arg ^ " : " ^ lhs_argt_sc)
                end
             in
             acc @
             ["for( " ^ rhs_argt_sc ^ 
              "::iterator combine_it = "^rhs_arg^".begin(); " ^
              "combine_it != "^rhs_arg^".end(); combine_it++ ) {";
              tab ^ "pair<" ^ lhs_argt_sc ^ 
              "::iterator,bool> ret = " ^ lhs_arg ^ ".insert(*combine_it);";
              tab ^ "if( !ret.second ) " ^ modify_lhs ^ ";";
              "}";]             
            )
           [] (List.tl (List.combine args arg_types)))
         
    | MapUpdate ->
        let k,v = back (List.tl args) in
        begin match argti 0 with
          | Host(Collection _) -> inl((argi 0)^"["^(mk_tuple k)^"] = "^v)
          | Host(_) -> failwith "invalid merge of non-collection"
          | Target(Type (id_t,_)) ->
            failwith "unhandled sugared persistent map update"
          | _ -> failwith "unsupported map update"  
        end

    | MapValueUpdate ->
        let k,v = back (List.tl args) in
        begin match argti 0 with
          | Host(Collection _) -> inl((argi 0)^"["^(mk_tuple k)^"] = "^v)
          | Host(_) -> inl((argi 0)^" = "^(argi 1))
          | Target(Type (x,_)) ->
            failwith "unhandled sugared persistent map value update"
          | _ -> failwith "unsupported map value update"  
        end
        
    | MapElementRemove ->
        let k = List.tl args in 
        begin match argti 0 with
          | Host(Collection _) -> inl((argi 0)^".erase("^(mk_tuple k)^")")
          | Host(_) -> failwith "invalid erase on non-collection"
          | Target(Type (x,_)) ->
            failwith "unhandled sugared persistent map value update"
          | _ -> failwith "unsupported map element remove"  
        end

    | External _ -> failwith "invalid sugared external function"

    | Ext(ext_fn) -> inl(
        begin match ext_fn with
          | Apply(fn) -> fn^"("^a^")"
          | Constructor(t) -> (string_of_type t)^"("^a^")"
          | MemberAccess(field) -> (argi 0)^"."^field 
          | IteratorElement -> "(*"^(argi 0)^")"
          | ConstCast(t) ->
            let ts = string_of_type t in
            let sep = if ts.[String.length ts - 1] = '>' then " " else ""
            in "const_cast<"^(ts^sep)^">("^(argi 0)^")"

          | CopyCollection((v,_)) -> v^".copy("^a^")" 
          | PairFirst -> (argi 0)^".first"
          | PairSecond -> (argi 0)^".second"
          | BeginCollection -> (argi 0)^".begin()"
          | EndCollection -> (argi 0)^".end()"
          | FindCollection -> (argi 0)^".find("^(mk_tuple (List.tl args))^")"
          | ModifyCollection -> (argi 0)^".modify("^
                                (of_list (List.tl args))^")"
          | InsertCollection -> (argi 0)^".insert("^
                                (of_list (List.tl args))^")"
          | RangeCollection -> (argi 0)^".equal_range("^
                               (mk_tuple (List.tl args))^")"
          | EraseCollection -> (argi 0)^".erase("^(of_list (List.tl args))^")"
          | ClearCollection -> (argi 0)^".clear()"  
          | SecondaryIndex(pat) -> (argi 0)^".get<"^(string_of_type pat)^">()" 
          | BoostLambda(fn) -> fn
          | Inline(s) -> s 
        end)

  and source_code_of_expr (expr : (ext_type, ext_fn) typed_expr_t) =
    let wrap_neg_with_parens r =
      if r.[0] = '-' then "("^r^")" else r
    in
    begin match expr with
    | Const (_,c) -> inl(
       begin match c with
         | CFloat x -> 
           let r = string_of_float x in
              wrap_neg_with_parens 
                 (if r.[(String.length r)-1] = '.' then r^"0" else r)
         
         | CInt x -> "static_cast<"^(cpp_of_type TInt)^">("^(string_of_int x)^")"
         
         | CString s ->
            if Debug.active "HASH-STRINGS"
            then "static_cast<"^(cpp_of_type TInt)^">(string_hash(\""^(String.escaped s)^"\"))"
            else "\""^(String.escaped s)^"\""
         
         | CBool(true) -> "true"
         | CBool(false) -> "false"
         | CDate(y,m,d) -> "static_cast<"^(cpp_of_type TInt)^">(" ^ 
                           (string_of_int (y*10000+m*100+d)) ^ ")" 
                           (* TODO must be change with appropriate 
                            * object in c++ *)
       end)
 
    | Var (_,(v,_)) -> inl(v)
    | Tuple (Host(TTuple(types)), fields) ->
      inl(mk_tuple (List.map (fun e -> ssc (source_code_of_expr e)) fields))

    | Op(_,op,e) -> inl ((string_of_op op)^"("^
                         (ssc (source_code_of_expr e))^")")
    | BinOp(_,op,l,r) ->
        let binop op =
          parensc "(" ")"
            (binopsc op (source_code_of_expr l) (source_code_of_expr r))
        in
        begin match op with
          | Assign ->
            binopsc (string_of_op op)
               (source_code_of_expr l) (source_code_of_expr r)

          | Eq -> binop "=="
          | Neq -> binop "!="
          | Lt -> binop "<"
          | Leq -> binop "<="
       
          | If0 ->
            parensc "(" ")" 
              (binopsc " ? " (source_code_of_expr l)
              (binopsc " : " (source_code_of_expr r) (inl "0")))

          | _ -> parensc "(" ")" (binopsc (string_of_op op)
                   (source_code_of_expr l) (source_code_of_expr r))
        end

    | Fn(rt,id,args) -> source_code_of_fn id args rt

    | _ -> failwith "invalid imperative expression"
  end
  
  let source_code_of_decl ?(gen_type = true) ?(gen_init = true)
                           (decl : (ext_type, ext_fn) typed_imp_t) =
    let t, s, d_init = begin match decl with
      | Decl(_,(_s,_t), _init) -> (string_of_type _t), _s, _init
      | _ -> failwith "Unexpected imp expression. Declaration required."
    end in
    let s_init = begin match d_init with
     (* Directly invoke constructor on non-heap allocated local definitions *)
      | Some(Fn(_, Ext(Constructor(_t)), args)) ->
        "("^(String.concat ","
               (List.map (fun a -> ssc (source_code_of_expr a)) args))^")"
      | Some(e) -> (string_of_op Assign)^(ssc (source_code_of_expr e))
      | None -> ""
    end in
      begin match gen_type, gen_init with
    | false, false -> (inl s)
    |  true, false -> (inl (t^" "^s))
    | false,  true -> (inl (if s_init = "" then "" else (s^s_init)))
    |  true,  true -> (inl (t^" "^s^s_init))
      end
    
  let source_code_of_decl_l ?(gen_type = true) ?(gen_init = true)
                            (decl_l : (ext_type, ext_fn) typed_imp_t list) =
  List.map (source_code_of_decl ~gen_type:gen_type ~gen_init:gen_init)
    (List.filter 
      (fun d -> begin match d, (not gen_type) && gen_init with
        | Decl(_,_,_),false 
        | Decl(_,_,Some(_)),true -> true
        | Decl(_,_,None),true    -> false
        | _,_ -> failwith "Unexpected imp expression. Declaration required." 
        end ) 
      decl_l)
                            
  let rec source_code_of_imp ?(gen_init = true) 
    (imp : (ext_type, ext_fn) typed_imp_t) =
    let mk_block i = match i with
        | Block _ -> i
        | _ -> Block(type_of_imp_t i, [i]) in
    begin match imp with
    | Expr (_,e) -> dsc stmt_delimiter (source_code_of_expr e)
    | Block (_,[Block (_,_) as b]) -> 
      source_code_of_imp ~gen_init:gen_init b
            
    | Block (_,il) ->
      let r = List.map (source_code_of_imp ~gen_init:gen_init) il in
      begin match r with
        | [Lines([l])] -> isc tab (Lines [l])
        | _ -> cscl ([inl "{"]@(List.map (isc tab) r)@[inl "}"])
      end
    | Decl(_,_,_) ->
        dsc stmt_delimiter (source_code_of_decl ~gen_init:gen_init imp)

    | For(_,((elem, elem_ty), elem_f),source,body) ->
        begin match type_of_expr_t source with
        | Host(TBase(TInt)) -> (* TODO: change to bool type *)
          let next_elem = "next_"^elem in
          let cond = ssc (source_code_of_expr source) in
          cscl (
            [Lines([
               (string_of_type elem_ty)^" "^next_elem^" = "^elem^stmt_delimiter;
               "for (; "^cond^"; "^elem^" = "^next_elem^") {";
               tab^"if( "^cond^" ) ++"^next_elem^stmt_delimiter;
               ])]@
            [source_code_of_imp body]@
            [Lines ["}"]])

        | _ -> 
          print_endline ("invalid desugared loop, expected iterators: "^
                            (string_of_ext_imp imp));
          failwith ("invalid desugared loop, expected iterators")
        end

    | IfThenElse(_,p,t,e) ->
        let tb, eb = mk_block t, mk_block e in
        let parenthesized_p =
          let sc = ssc (source_code_of_expr p) in
          try
            if (String.index sc '(') = 0
                && (String.rindex sc ')') = ((String.length sc)-1)
            then sc
            else "("^sc^")"
          with Not_found -> "("^sc^")"
        in 
        cscl ([Lines [("if "^parenthesized_p^" ")]]@
              [source_code_of_imp tb]@
              [Lines ["else "]]@
              [source_code_of_imp eb])
    end

  let source_code_of_trigger dbschema (event,stmts) =
    let trig_name = Schema.name_of_event event in
    let args = (Schema.event_vars event) in
    let trig_types = 
      List.map (fun v -> string_of_type (Host(TBase((snd v))))) args 
    in
    let trig_args = String.concat ", "
      (List.map (fun ((a,_),ty) -> ty^" "^a) (List.combine args trig_types))
    in  
    let trigger_fn =
      let trigger_body = source_code_of_imp stmts in
      [inl ("void on_"^trig_name^"("^trig_args^") {");
       inl ("BEGIN_TRIGGER(exec_stats,\""^trig_name^"\")");
       inl ("BEGIN_TRIGGER(ivc_stats,\""^trig_name^"\")");]@
      [trigger_body]@
      [inl ("END_TRIGGER(exec_stats,\""^trig_name^"\")");
       inl ("END_TRIGGER(ivc_stats,\""^trig_name^"\")");
       inl ("}")]
    in
    let unwrapper =
      if event = Schema.SystemInitializedEvent then []
      else
        let evt_arg = "e" in
        let evt_fields = snd (List.fold_left
          (fun (i,acc) ty -> (i+1,
            acc@["any_cast<"^ty^">("^evt_arg^"["^(string_of_int i)^"])"]))
          (0,[]) trig_types)
        in
          [Lines ["void unwrap_"^trig_name^"(const event_args_t& e) {";
                  tab^"on_"^trig_name^"("^(String.concat "," evt_fields)^");";
                  "}"; ""];]             
  in trigger_fn@unwrapper


  (* Desugaring: rewriting function calls to an alternate API, e.g. for
   * collection accessors. *)

  (* Desugaring expressions is non-recursive for now, we assume expressions
   * such as nested slice expressions do not occur. *)
  let flatten_desugaring ie_l = List.fold_left (fun (i_acc,e_acc) (io,eo) ->
      match io, eo with
      | Some(i), None -> (i_acc@i), e_acc
      | Some(i), Some(e) -> (i_acc@i), (e_acc@[e])
      | None, Some(e) -> i_acc, (e_acc@[e])
      | None, None -> i_acc, e_acc) ([],[]) ie_l

  let merge_desugaring i_l e_l = List.flatten (List.map2 (fun io eo ->
    match io,eo with
      | Some(i), None -> i
      | None, Some(e) -> [Expr(type_of_expr_t e, e)]
      | Some(i), Some(e) -> i@[Expr(type_of_expr_t e, e)]
      | None, None -> []) i_l e_l)


  (* TODO: cleanly handle direct vs indirect updates (i.e. whether type of
   * slice is persistent, or temporary). See profiling code.
   *
   * TODO: optimize out map construction vs. in-place modification.
   * if fields = [] then
   *   code(#map# = #slice#;)
   * else
   *   code(
   *     // We construct an out map since the slice may be a temporary type,
   *     // i.e. an STL map rather than a multiindex.
   *     it = #map#.find(#keys#);
   *     #out map type# om;  
   *     hint = om.end(); 
   *     slice_it = #slice#.begin();
   *     slice_end = #slice#.end();
   *     for ( ; slice_it != slice_end; ++slice_it) {
   *       #out map entry type# e( *slice_it); hint = om.insert(hint, e);
   *     }
   *     #map#.modify(it, bind(#map entry#::__av, _1) = om);
   *   )
   *)
  let desugar_map_update nargs c_t =
    let mk_it_t t = Target(Iterator(t)) in
    let mk_var id t = Var(t, (id, t)) in  
    let c_var = List.hd nargs in
    let entry_t = entry_type_of_collection c_t in 
    let entry_t_id = type_id_of_type entry_t in
    
    let c_it_id, c_it_t, c_it_var =
      let x,y = gensym(), mk_it_t c_t in x, y, Var(y, (x,y)) in
    let k,v = back (List.tl nargs) in
    if k = [] then [Expr(unit, BinOp(unit, Assign, c_var, v))]
    else
      (* Out map *)
      let it_decl = Decl(unit, (c_it_id, c_it_t),
                      Some(Fn(c_it_t, Ext(FindCollection), [c_var]@k))) in

      (* Update slice iterators *)
      let slice_id, slice_t = gensym(), type_of_expr_t v in
      let slice_elem_t = match slice_t with
        | Host(Collection(_, x)) -> Host x
        | Target(_) as x when is_ext_collection x ->
          entry_type_of_collection x
        | _ -> failwith ("invalid update collection in map update " ^ 
                         (string_of_type slice_t))
      in
      let slice_it_id, slice_end_id, slice_it_t =
        gensym(), gensym(), mk_it_t slice_t in
      let slice_it_var = mk_var slice_it_id slice_it_t in
      let slice_end_var = mk_var slice_end_id slice_it_t in
      
      let target_id, target_t, target_ref_t =
        let _,t = back (field_types_of_type (type_decl_of entry_t))
        in gensym(), t, Target(Ref(t)) in
      let target_var = mk_var target_id target_t in
      let target_decl = Decl(unit, (target_id, target_t), None) in
      let target_entry_t = entry_type_of_collection target_t in
      let target_entry_id, target_entry_decl, target_entry_var =
        let x = gensym() in x, (x,target_entry_t), mk_var x target_entry_t in
      let ctor =
        let init_args = [Fn(slice_elem_t, Ext(IteratorElement), [slice_it_var])]
        in Decl(unit, target_entry_decl,
                Some(Fn(target_entry_t, Ext(Constructor(target_entry_t)),
                     init_args)))
      in
      let target_it_t = mk_it_t target_t in
      let hint_id, hint_var =
        let x = gensym() in x, Var(target_it_t, (x, target_it_t)) in 
      let loop_decls =
        (* TODO: optimize double usage of v *)
        [Decl(unit, (hint_id, target_it_t),
              Some(Fn(target_it_t, Ext(EndCollection), [target_var])));
         Decl(unit, (slice_it_id, slice_it_t),
              Some(Fn(slice_it_t, Ext(BeginCollection), [v])));
         Decl(unit, (slice_end_id, slice_it_t),
              Some(Fn(slice_it_t, Ext(EndCollection), [v])))]
      in
      let update_loop =
        For(unit, ((slice_it_id, slice_it_t), false),
            BinOp((Host(TBase(TInt))), Neq, slice_it_var, slice_end_var),
            Block(unit,
              [ctor;
               Expr(unit, BinOp(unit, Assign, hint_var,
                 Fn(target_it_t, Ext(InsertCollection),
                   [target_var; hint_var; target_entry_var])))]))
      in
      let modify_lambda =
        "boost::lambda::bind(&"^entry_t_id^
        "::__av, boost::lambda::_1) = "^target_id
      in
      let map_update =
        Expr(unit, Fn(unit, Ext(ModifyCollection),
          [c_var; c_it_var; Fn(Host TUnit, 
                               Ext(BoostLambda(modify_lambda)), [])])) 
      in [it_decl; target_decl]@loop_decls@[update_loop; map_update]


  (* map value update rewrites:
   *    it = #map#.find(#key#);
   *    if ( it != #map#.end() ) {
   *      #value type# sym = #value#;
   *      if ( sym == 0 ) {
   *        #map#.erase(it);
   *      } else {
   *        #map#.modify(it, bind(#map entry#::__av, _1) = sym);
   *      }
   *    }
   *    else { #map entry# e(#key#, #value#); #map#.insert(e); }
   *)
  let desugar_map_value_update nargs c_t =
    let k,v = back (List.tl nargs) in
    let entry_id = gensym() in
    let entry_t = entry_type_of_collection c_t in 
    let entry_t_id = type_id_of_type entry_t in
    
    let c_it_t, it_id = Target(Iterator(c_t)), gensym() in
    let c_var, it_var = List.hd nargs, Var(c_it_t, (it_id, c_it_t)) in
    (* TODO: change op_t to bool when K3 supports boolean types. *)
    (* TODO: technically mod_t is a function type, but K3 types are not
     * extensible, thus cannot use external type arguments *)
    let op_t, mod_t = Host(TBase(TInt)), unit in
    let tmp_id, tmp_t = gensym(), type_of_expr_t v in 
    let delete_on_zero_expr =
      let zero, modify_value =
        if Debug.active "DELETE-ON-ZERO" then
           match tmp_t with
             | Host(TBase(TFloat)) ->
               Some(Const(tmp_t, CFloat(0.0))), tmp_id
             | Host(TBase(TInt)) ->
               Some(Const(tmp_t, CInt(0))), tmp_id
             | _ -> None, (ssc (source_code_of_expr v))
        else (None, (ssc (source_code_of_expr v)))
      in
      let modify_lambda =
        "boost::lambda::bind(&" ^ entry_t_id ^ 
        "::__av, boost::lambda::_1) = " ^ modify_value 
      in
      let modify_expr =
        Expr(unit, Fn(unit, Ext(ModifyCollection),
             [c_var; it_var; Fn(mod_t, Ext(BoostLambda(modify_lambda)), [])]))
      in
      match zero with 
        | Some(zero_i) ->
          Block(unit, [
            Decl(unit, (tmp_id, tmp_t), Some(v));
            IfThenElse(unit,
              BinOp(op_t, Eq, Var(tmp_t, (tmp_id, tmp_t)), zero_i),
              Expr(unit, Fn(unit, Ext(EraseCollection),
                   [c_var; Var(c_it_t, (it_id, c_it_t))])),
              modify_expr)])
        | _ -> modify_expr
    in
    [Decl(unit, (it_id, c_it_t), 
          Some(Fn(c_it_t, Ext(FindCollection), [c_var]@k)));
     IfThenElse(unit,
       BinOp(op_t, Neq, it_var, Fn(c_it_t, Ext(EndCollection), [c_var])),
       delete_on_zero_expr,
       Block(unit, [
         Decl(unit, (entry_id, entry_t),
           Some(Fn(entry_t, Ext(Constructor(entry_t)), (List.tl nargs))));
         Expr(unit, (* ignore iterator return *)
           Fn(c_it_t, Ext(InsertCollection),
               [c_var; Var(entry_t, (entry_id, entry_t))]))]))]


  (* Updates out tier if in tier exists using above single-level code,
   * otherwise builds new out tier entry and adds to the in tier *)
  let desugar_nested_map_value_update nargs c_t =
    let c_expr, c_t_id = List.hd nargs, (type_id_of_type c_t) in
    let c_entry_t, c_it_t =
      entry_type_of_collection c_t, Target(Iterator(c_t)) in
    let out_map_t, out_entry_t, in_exprs, out_exprs = 
      let fields_t = match ext_collection_of_type c_t with
        | Some(x) -> field_types_of_collection x
        | _ -> failwith ("invalid collection type "^(string_of_type c_t)) in
      let mt = snd (back fields_t) in 
      let num_in_fields = List.length fields_t - 1 in
      let ine,oute = snd (List.fold_left (fun (i,(ina,outa)) e ->
        (i+1, if i < num_in_fields then (ina@[e],outa) else (ina,outa@[e])))
        (0,([],[])) (List.tl nargs))
      in mt, (entry_type_of_collection mt), ine, oute
    in
    let in_args = c_expr::in_exprs in
    let c_it_expr = Fn(c_it_t, Ext(FindCollection), in_args) in
    let c_it_decl, c_it_var = 
      let x = gensym() in
      let y = x,c_it_t in Decl(unit, y, Some(c_it_expr)), Var(c_it_t, y)
    in
    let op_t = Host(TBase(TInt)) in
    let out_update_decl, out_update_map_var =
        let x,t = gensym(), Target(Ref(out_map_t)) in
        Decl(unit, (x,t), Some(
          Fn(t, Ext(ConstCast(t)),
            [Fn(out_map_t, Ext(MemberAccess("__av")),
               [Fn(c_entry_t, Ext(IteratorElement), [c_it_var])])]))),
        Var(t, (x,t))
    in
    let out_args = out_update_map_var::out_exprs in
    let out_update_imp = desugar_map_value_update out_args out_map_t in
    let out_map_var, out_entry_ctor_and_insert =
      let x,y = gensym(), gensym() in
      let out_entry_var = Var(out_entry_t, (x,out_entry_t)) in
      let out_map_var = Var(out_map_t, (y,out_map_t)) in
      out_map_var,
      [Decl(unit, (x,out_entry_t),
         Some(Fn(out_entry_t, Ext(Constructor(out_entry_t)), out_exprs)));
       Decl(unit, (y,out_map_t), None);
       Expr(unit, Fn(unit, Ext(InsertCollection), 
                     [out_map_var; out_entry_var]))]
    in
    let in_entry_ctor_and_insert =
      let x = gensym() in
      let c_entry_var = Var(c_entry_t, (x,c_entry_t)) in
      [Decl(unit, (x, c_entry_t),
            Some(Fn(c_entry_t, Ext(Constructor(c_entry_t)),
            (in_exprs@[out_map_var]))));
       Expr(unit, Fn(unit, Ext(InsertCollection), [c_expr; c_entry_var]))]
    in
    (* In-tier caching *)
    let seq_idx_tag = c_t_id^"_seq" in
    let seq_idx_id, seq_idx_t, seq_idx_it_t, seq_idx_var = 
      let x = gensym() in
      let y = Target(Type(c_t_id^"_index_seq",None))
      in x, Target(Ref(y)), Target(Iterator(y)), Var(y,(x,y))
    in
    let update_and_relocate =
      let push_id, push_t, push_var =
        let x = gensym() in
        let y = Target(Pair(seq_idx_it_t, Target(Type("bool",None))))
        in x, y, Var(y, (x,y))
      in
      let relocate_fn = seq_idx_id^".relocate" in
      let push_fn = seq_idx_id^".push_front" in
      let post_update = [
        Decl(seq_idx_t, (seq_idx_id, seq_idx_t),
          Some(Fn(seq_idx_t, 
                  Ext(SecondaryIndex(Target(Type(seq_idx_tag,None)))),
                  [c_expr])));
        Decl(push_t, (push_id, push_t),
          Some(Fn(unit, Ext(Apply(push_fn)),
                  [Fn(c_entry_t, Ext(IteratorElement), [c_it_var])])));
        Expr(unit, Fn(unit, Ext(Apply(relocate_fn)),
               [Fn(seq_idx_it_t, Ext(BeginCollection), [seq_idx_var]);
                Fn(seq_idx_it_t, Ext(PairFirst), [push_var])]))]
      in
        [out_update_decl]@out_update_imp@post_update
    in
    let insert_and_flush =
      let pre_insert =
        Decl(seq_idx_t, (seq_idx_id, seq_idx_t),
          Some(Fn(seq_idx_t,
                  Ext(SecondaryIndex(Target(Type(seq_idx_tag,None)))), 
                  [c_expr])))
      in
      let post_insert =
        let size_t = Host(TBase(TInt)) in
        let max_size = Var(size_t, (c_t_id^"_SIZE", size_t)) in
        let size_fn = seq_idx_id^".size" in
        let pop_last_fn = seq_idx_id^".pop_back" in
        IfThenElse(unit,
          BinOp(op_t, Lt, max_size, Fn(size_t, Ext(Apply(size_fn)), [])),
          Expr(unit, Fn(unit, Ext(Apply(pop_last_fn)), [])),
          Block(unit, []))
      in
        [pre_insert]@
        out_entry_ctor_and_insert@in_entry_ctor_and_insert@
        [post_insert]
    in
      [c_it_decl;
       IfThenElse(unit,
         BinOp(op_t, Neq, c_it_expr, Fn(c_it_t, Ext(EndCollection), [c_expr])),
         Block(unit, update_and_relocate),
         Block(unit, insert_and_flush))]

  (* Desugar map erase from a global collection *)
  let desugar_map_element_remove nargs c_t =
    let c_var = List.hd nargs in
    let entry_tuple =
      let tt = Host(TTuple(
        List.map (fun e -> host_type (type_of_expr_t e)) (List.tl nargs)))
      in Tuple(tt, List.tl nargs) 
    in
      Expr(unit, Fn(unit, Ext(EraseCollection), [c_var; entry_tuple]))

  (** Expression desugaring:
      Imp supports certain forms of syntactic sugar, allowing certain 
      expressions to be defined inline.  For example, we can have a For loop 
      that iterates over a collection rather by incrementing a constant.  When
      these are not supported in the target language, we need to remove them
      from the syntax tree.
      
      This function desugars an expression into two components: A set of
      initialization statements (that evaluate the inline expression) and 
      a single return value that now stores the value of the inlined 
      expression.  These values are returned inline in optional wrappers --
      the first if there is no initialization to be done, and I think the 
      second is None if it is not possible to desugar the expression?
      
    *)
  let rec desugar_expr opts e =
    let recur l = flatten_desugaring (List.map (desugar_expr opts) l) in
    let result x y = (if x = [] then None else Some(x)), Some(y) in
    let member_common args =
      let ci, nargs = recur args in
      if List.length nargs <> List.length args then
        failwith "invalid member desugaring"
      else
      let c_var = List.hd nargs in
      let c_it_t = Target(Iterator(type_of_expr_t c_var)) in
      (* TODO: replace with bool when K3 has bool type *)
      let op_t = Host(TBase(TInt)) in
      result ci (BinOp(op_t, Neq, Fn(c_it_t, Ext(FindCollection), nargs),
                                  Fn(c_it_t, Ext(EndCollection), [c_var])))
    in
    let lookup_common elem_t args =
      let ci, nargs = recur args in
      if List.length nargs <> List.length args then
        failwith "invalid member desugaring"
      else
      let c_var, c_t = List.hd nargs, type_of_expr_t (List.hd nargs) in
      let c_it_t = Target(Iterator(c_t)) in
      let c_elem_t, access_f = match c_t with
        | Host(Collection(_, x)) -> Host x, Ext(PairSecond)
        | Target(Type(x,Some(_))) -> 
            entry_type_of_collection c_t, Ext(MemberAccess("__av"))
        | _ -> failwith "invalid lookup on non-collection"
      in
      result ci (Fn(elem_t, access_f, [Fn(c_elem_t,
        Ext(IteratorElement), [Fn(c_it_t, Ext(FindCollection), nargs)])]))
    in
    match e with
    | Tuple (meta,el) ->
        let x,y = recur el in
        if List.length y = List.length el then result x (Tuple(meta,y))
        else failwith "invalid tuple desugaring"
 
    | Op (meta,op,ce) ->
        let x,y = recur [ce] in
        begin match y with
          | [nce] -> result x (Op(meta, op, nce))
          | _ -> failwith "invalid operator desugaring"  
        end

    | BinOp (meta,op,le,re) ->
        let x,y = recur [le; re] in
        begin match y with
          | [nle; nre] -> result x (BinOp(meta,op,nle,nre))
          | _ -> failwith "invalid binary operator desugaring"
        end

    | Fn(_, Member, args) -> member_common args
    
      (* Standard lookup case *)
    | Fn(elem_t, Lookup, args) -> lookup_common elem_t args

    | Fn(slice_ty, Slice(idx), mke) ->
        let ci,nmke = recur mke in
        if List.length nmke <> List.length mke then
            failwith "invalid slice function desugaring"
        else
        let it_ty = Target(Iterator(slice_ty)) in
        let slice_id = gensym() in
        let slice_decl = (slice_id, slice_ty) in
        let pair_id, pair_ty = gensym(), Target(Pair(it_ty, it_ty)) in
        let pair_var = Var(pair_ty, (pair_id, pair_ty)) in 
        Some(ci@
         [Decl(unit, (pair_id,pair_ty), Some(Fn(slice_ty, Slice(idx), nmke)));
          Decl(unit, slice_decl, None);
          Expr(unit, Fn(unit, Ext(CopyCollection(slice_decl)),
            [Fn(it_ty, Ext(PairFirst), [pair_var]);
             Fn(it_ty, Ext(PairSecond), [pair_var])]))]),
        Some(Var(slice_ty, slice_decl))

    (* Directly desugar updates to multi_index_container maps. The actual
     * work is performed by the desugar_map_*_update functions above. *)
    | Fn(ty, MapUpdate, args) ->
        let ci,nargs = recur args in
        if List.length nargs <> List.length args then
            failwith "invalid map update desugaring"
        else
        begin match type_of_expr_t (List.hd nargs) with
        | Target(Type(_)) as c_t ->
          if opts.profile then
            let id = ssc (source_code_of_expr (List.hd nargs)) in
            let _,v = back (List.tl nargs) in
            let update_f = match type_of_expr_t v with
              | Target(_) -> id^"_update_direct"
              | _ -> id^"_update"
            in result ci (Fn(ty, Ext(Apply(update_f)), nargs))
          else
            let insert_loop = desugar_map_update nargs c_t
            in Some(ci@insert_loop), None
        | _ ->
          if opts.profile then
            let id = ssc (source_code_of_expr (List.hd nargs))
            in result ci (Fn(ty, Ext(Apply(id^"_update")), nargs))
          else result ci (Fn(ty, MapUpdate, nargs))
        end

    | Fn(ty, MapValueUpdate, args) ->
        let ci,nargs = recur args in
        if List.length nargs <> List.length args then
            failwith "invalid map update desugaring"
        else
        let t = type_of_expr_t (List.hd nargs) in
        let error() = failwith
          ("invalid collection for value update "^(string_of_type t))
        in begin match t with
        | Target(_) as x when is_ext_collection x -> 
          let fields_t = match ext_collection_of_type x with
            | Some(c_t) -> field_types_of_collection c_t
            | _ -> error()
          in
          let desugar_f = match snd (back fields_t) with
            | Host(TBase(_)) -> desugar_map_value_update
            | Target(_) -> desugar_nested_map_value_update
            | _ -> error()
          in 
          (* Only profile external types, i.e. multi-indexes, not native types
           * such as STL maps, and scalars. *)
          if opts.profile then
            let id = ssc (source_code_of_expr (List.hd nargs))
            in result ci (Fn(ty, Ext(Apply(id^"_value_update")), nargs))
          else
            let r = desugar_f nargs x in Some(ci@r), None

        | _ -> result ci (Fn(ty, MapValueUpdate, nargs))
        end

    | Fn(ty, MapElementRemove, args) ->
        let ci,nargs = recur args in
        if List.length nargs <> List.length args then
            failwith "invalid map update desugaring"
        else
        let t = type_of_expr_t (List.hd nargs)
        in begin match t with
        | Target(_) as c_t when is_ext_collection c_t ->
          if opts.profile then
            let id = ssc (source_code_of_expr (List.hd nargs)) in
            let remove_f = id^"_remove"
            in result ci (Fn(ty, Ext(Apply(remove_f)), nargs))
          else
            Some(ci@[desugar_map_element_remove nargs c_t]), None
          
        | _ -> result ci (Fn(ty, MapElementRemove, nargs))
        end

    | Fn(ty, External(inline_id,arg_t,r_t), args) ->
        let ci,nargs = recur args in
        if List.length nargs <> List.length args then
            failwith "invalid external function application"
            
        else 
        let id = String.lowercase inline_id in
        begin match id with
            | "/" ->
              let arg = ssc (source_code_of_expr (List.hd nargs)) in 
                 result ci 
                        (Fn(ty, 
                            Ext(Inline("static_cast<double>(1) / (" ^
                            arg ^ ")")), []))
            | "listmax" | "listmin" ->
              let c_fn_id = String.sub id 4 3 in
              let base_type arg = begin match (type_of_expr_t arg) with
               | Host(TBase(base_t)) -> base_t
               | _ -> failwith ("Invalid call to "^id)
              end in
              let escalate f = Type.escalate_type_list ~opname:id
                                                        (List.map base_type f)
              in
              let arg_cast tgt_type arg = 
                  (Fn(ty, Ext(Apply("static_cast<"^
                                    (Type.cpp_of_type tgt_type)^
                                     ">")), [arg]))
              in
              begin match nargs with
                | [Tuple(ft_l, f)] ->
                  let tgt_type = escalate f in
                  result ci (Fn(ty, Ext(Apply(c_fn_id)), 
                     (List.map (arg_cast tgt_type) f)))
                | [Var (_,(v,Host(TTuple([TBase(_); TBase(_)]))))] ->
                  let arg = ssc (source_code_of_expr (List.hd nargs)) in
                  result ci (Fn(ty, Ext(Apply(c_fn_id)),
                     [ Fn(ty, Ext(Inline("at_c<0>(" ^ arg ^ ")")), []);
                       Fn(ty, Ext(Inline("at_c<1>(" ^ arg ^ ")")), []) ]))
                | _ -> result ci (Fn(ty, Ext(Apply(c_fn_id)), nargs))
              end
            | "date_part" ->
               begin match nargs with 
               | [Tuple(ft_l, [Const(_, CString(part)); d_arg])] ->
                  let lower_part = String.lowercase part in
                  if (lower_part <> "year") && (lower_part <> "month") && 
                     (lower_part <> "day")
                  then failwith ("invalid call to date_part on : "^part)
                  else
                     result ci (Fn(ty, Ext(Apply(lower_part^"_part")), [d_arg]))
               | _ -> failwith ("Invalid call to date_part")
               end
            | "cast_date" | "cast_int" | "cast_float" | "cast_string" ->
               begin match nargs with
               | [arg] ->
                  begin match type_of_expr_t arg with
                     | Host(TBase(source_type)) ->
                        let dest_type = 
                           begin match r_t with 
                              | TBase(dest_type) -> dest_type
                              | _ -> failwith "Invalid cast function"
                           end
                        in
                        if source_type = dest_type 
                        then result ci arg
                        else
                        let cast_fn_id = id^"_from_"^
                           (Type.string_of_type source_type) in
                        result ci (Fn(ty, Ext(Apply(cast_fn_id)), nargs))
                     | t -> failwith ("Cast of invalid type: "^
                              (Imperative.string_of_type (fun _ -> "[EXT]") t))
                  end
               | _ -> failwith ("Invalid call to cast '"^id^"'")
               end
            | "regexp_match" ->
              begin match nargs with
                | [Tuple(ft_l, f)] ->
                  result ci (Fn(ty, Ext(Apply(String.lowercase id)), f))
                | [Var (_,(v,Host(TTuple([TBase(_); TBase(_)]))))] ->                  
                  let arg = ssc (source_code_of_expr (List.hd nargs)) in
                  result ci (Fn(ty, Ext(Apply(String.lowercase id)),
                     [ Fn(ty, Ext(Inline("at_c<0>(" ^ arg ^ ").c_str()")), []);
                       Fn(ty, Ext(Inline("at_c<1>(" ^ arg ^ ")")), []) ]))
                | _ -> result ci (Fn(ty, Ext(Apply(String.lowercase id)), 
                                     nargs))
              end                                           
            | _ -> 
              begin match nargs with
                | [Tuple(ft_l, f)] ->
                  result ci (Fn(ty, Ext(Apply(String.lowercase id)), f))
                | _ -> result ci (Fn(ty, Ext(Apply(String.lowercase id)), 
                                     nargs))
              end
        end
      
    | Fn (meta,fn_id,args) ->
        let x,y = recur args in
        if List.length y = List.length args then result x (Fn(meta,fn_id,y))
        else failwith "invalid function desugaring"

    | _ -> None, Some(e)


  (* Declares iterators over an entire collection *)
  let iters_of_collection iter_id iter_end c c_ty =
    let it_t = Target(Iterator(c_ty)) in
    [Decl(unit, (iter_id, it_t), Some(Fn(it_t, Ext(BeginCollection), [c])));
     Decl(unit, (iter_end, it_t), Some(Fn(it_t, Ext(EndCollection), [c])))]

  (* Declares iterators for a subrange of a collection based 
   * on the given args*)
  let range_iters_of_collection iter_id iter_end c c_ty args =
    if args = [] then iters_of_collection iter_id iter_end c c_ty
    else
    let it_t = Target(Iterator(c_ty)) in
    let pair_t = Target(Pair(it_t, it_t)) in
    let pair_id = gensym() in
    let pair_v = Var(pair_t, (pair_id, pair_t)) in
      [Decl(unit, (pair_id,pair_t), 
            Some(Fn(c_ty, Ext(RangeCollection), [c]@args)));
       Decl(unit, (iter_id, it_t), Some(Fn(it_t, Ext(PairFirst), [pair_v])));
       Decl(unit, (iter_end, it_t), Some(Fn(it_t, Ext(PairSecond), [pair_v])))]

  (* Note this function does not currently propagate any changes in types
   * throughout the rest of the expression. This is left to the caller with
   * to do with further substitutions *)
  let rec sub_expr src_dest_pairs expr =
    let rcr = sub_expr src_dest_pairs in
    if List.mem_assoc expr src_dest_pairs then
        (*
        ((print_endline ("subbing "^
            (ssc (source_code_of_expr (snd (List.assoc expr src_dest_pairs))))^
            " for "^(ssc (source_code_of_expr expr))));
        *)
        List.assoc expr src_dest_pairs
    else begin match expr with
      | Const _ -> type_of_expr_t expr, expr
      | Var _ -> type_of_expr_t expr, expr
      | Tuple (meta, l) ->
        let r = Tuple(meta, List.map snd (List.map rcr l))
        in type_of_expr_t r, r
      | Op(meta,op,ce) -> 
        let r = Op(meta,op, snd (rcr ce)) in type_of_expr_t r, r
      | BinOp(meta, op, le, re) ->
        let nle, nre = rcr le, rcr re in
        let r = BinOp(meta, op, snd nle, snd nre)
        in type_of_expr_t r, r
      | Fn(meta, id, args) ->
        let r = Fn(meta, id, List.map snd (List.map rcr args))
        in type_of_expr_t r, r
      end

  (* See above note on type change propagation *) 
  let rec sub_imp_expr src_dest_pairs imp =
    let rcr = sub_imp_expr src_dest_pairs in
    let rcr_e = sub_expr src_dest_pairs in
    match imp with
    | Expr(meta,e) -> let n_e = rcr_e e in [], Expr(meta, snd n_e)

    | Block (meta,l) ->
        let subs, nl = List.split (List.map rcr l) in
        List.flatten subs, Block(meta, nl)  

    | Decl(_,_,None) as d -> [], d

    | Decl(meta,(id,ty),Some(e)) ->
      let n_ty,n_e = rcr_e e in 
        (if ty <> n_ty then [ty, (id,n_ty)] else []),
        Decl(meta,(id,n_ty),Some(n_e))

    | For(meta,((id,ty),f),source,body) ->
        let n_c_ty, n_source = rcr_e source in
        let subs, n_body = rcr body in
        subs, For(meta, ((id,ty),f), n_source, n_body)
        
    | IfThenElse(meta,p,t,e) ->
        let n_ty,n_p = rcr_e p in
        if (match n_ty with Host(TBase(_)) -> false | _ -> true) then
          failwith "invalid predicate after substitution"
        else
          let t_subs, n_t = rcr t in
          let e_subs, n_e = rcr e in
          (t_subs@e_subs), IfThenElse(meta, n_p, n_t, n_e)

  (* Replaces variables in expressions. Expressions are r-values, thus no
   * new variables can be declared in their scope. *)

  let sub_expr_vars src dest e =
    snd (sub_expr [src, (type_of_expr_t dest, dest)] e)

  let struct_member_sub src_expr tt_l smt_l =
    let num_fields = List.length smt_l in
      if num_fields <> List.length tt_l then
        failwith "invalid composite element substitution"
      else 
        snd (List.fold_left (fun (i,acc) (src_t, (attr,dest_t)) ->
          let src = Fn(Host src_t, TupleElement(i), [src_expr]) in
          let dest = dest_t, Fn(dest_t, Ext(MemberAccess(attr)), [src_expr])
          in i+1,acc@[src, dest])
        (0,[]) (List.combine tt_l smt_l))

  let rec fixpoint_sub_imp subs imp =
    let nsubs, nimp = sub_imp_expr subs imp in
    if nsubs = [] then nimp
    else
      let next_subs = List.map (fun (ty,(id,nty)) -> match ty, nty with
        | Host(Collection(_, (TTuple(l)))), Target(_) 
            when is_ext_collection nty ->
               let entry_ty = entry_type_of_collection (type_decl_of nty) in
               let fields_l = fields_of_entry_type entry_ty 
               in
               (struct_member_sub (Var(ty,(id,ty))) l fields_l)@
               [Var(ty, (id, ty)), (nty, Var(nty, (id, nty)))]
        
        | Host(TTuple l), Target(Pair(nk_t,nv_t)) ->
            let key_t,value_t = back l in 
            let flat_key = (List.length key_t = 1) in
            let ne_var = Var(nty, (id,nty)) in
            let nkey = Fn(nk_t, Ext(PairFirst), [ne_var]) in
            let nvalue = Fn(nv_t, Ext(PairSecond), [ne_var]) in
            (* replace tuple accessors of the old element, with accessors
             * based on the new element definitions *)
            let last_field = List.length l - 1 in
            (snd (List.fold_left (fun (i,acc) t ->
              let src = Fn(Host t, TupleElement(i),[Var(ty, (id, ty))]) in
              let dest =
                if i = last_field then nvalue 
                else if flat_key then nkey
                else Fn(Host t, TupleElement(i), [nkey])
            in 
            i+1,acc@[src, (Host t, dest)]) (0,[]) l))@
            [Var(ty, (id, ty)), (nty, Var(nty, (id, nty)))]
         
        | Host(TTuple l), Target(_) ->
          let fields_l = fields_of_entry_type nty 
          in (struct_member_sub (Var(ty,(id,ty))) l fields_l)@
             [Var(ty, (id, ty)), (nty, Var(nty, (id, nty)))]
        
        | _, _ -> []) nsubs
      in
      fixpoint_sub_imp (List.flatten next_subs) nimp 


  let rec sub_decl_init elem elem_ty subbed_elem l =
    let rcr = sub_decl_init elem elem_ty subbed_elem in
    List.fold_left (fun (found,acc) i ->
        if found then found,acc@[i] else
        let nf,ni = match i with
          | Decl(unit, (id,ty), Some(init)) ->
            let n_init =
              sub_expr_vars (Var(elem_ty,(elem, elem_ty))) subbed_elem init in
            let nd = Decl(unit, (id,ty), Some(n_init))
            in (id=elem), nd
          | Block(m,l2) -> let r, nl2 = rcr l2 in r, Block(m,nl2)
          | _ -> false, i
        in (nf,acc@[ni]))
      (false, []) l

  (* Given an expected loop element, and a replacement iterator, substitute
   * iterator dereferencing in place of the loop element in the body's
   * declarations. If the loop element's declaration should be forced, add 
   * it to the body's declarations. *)
  let sub_iters ((elem, elem_ty), elem_f)
                (iter_id, iter_end, iter_decls, source_ty) body =
    let iter_t = Target(Iterator(source_ty)) in
    let subbed_elem =
      Fn(elem_ty, Ext(IteratorElement), [Var (iter_t, (iter_id, iter_t))]) in
    let elem_decls =
      if not elem_f then []
      else [Decl(unit, (elem, elem_ty), Some(subbed_elem))] in
    let subbed_body =
      match body with
      | Block (m,l) ->
        (* substitute in declaration initializers, until element is redefined *)
        let r = snd (sub_decl_init elem elem_ty subbed_elem l)
        in Block(m,(elem_decls@r))
      | _ -> Block(type_of_imp_t body, elem_decls@[body]) 
    in
    (* TODO: change to bool type *)
    let loop_cond_t = Host(TBase(TInt)) in
    let loop_cond = BinOp(loop_cond_t, Neq,
        Var(iter_t, (iter_id, iter_t)), Var(iter_t, (iter_end, iter_t))) in
    let loop_t = type_of_imp_t subbed_body in 
      iter_decls@[For(loop_t, ((iter_id, iter_t), false), 
                      loop_cond, subbed_body)]

  let sub_entry_access_in_loop (id,id_t) elem_ty body = 
    let loop_elem_t, loop_elem_tl = match id_t with
      | Host(TTuple(id_tl)) -> id_t, id_tl
      | Target _ ->
        begin match host_type_of_type id_t with
        | (Host(TTuple(id_tl))) as x -> x, id_tl
        | _ -> failwith "invalid element for slice"
        end
      | _ -> failwith "invalid element for slice"
    in
    let subs =
      (* tuple element -> member access, and var subs. Ordering
       * is important here, larger expressions must occur before
       * enclosed ones. *)
      let src_var = Var(loop_elem_t,(id,loop_elem_t)) in
      let dest_var = Var(elem_ty,(id,elem_ty)) in
      let access_subs =
        let fields_l = fields_of_entry_type elem_ty
        in
        snd (List.fold_left (fun (i,acc) (src_t, (attr,dest_t)) ->
            let src = Fn(Host src_t, TupleElement(i), [src_var]) in
            let dest = dest_t, Fn(dest_t, Ext(MemberAccess(attr)), [dest_var])
            in i+1,acc@[src, dest]) (0,[])
          (List.combine loop_elem_tl fields_l))
      in let var_subs = [src_var, (elem_ty, dest_var)]
      in access_subs@var_subs
    in fixpoint_sub_imp subs body

  (* Imperative statement desugaring *)
  let rec desugar_imp_aux opts imp =
    let rcr = desugar_imp opts in
    let rcr_aux = desugar_imp_aux opts in
    let flatten_external e = match desugar_expr opts e with
        | Some(i), None -> i
        | None, Some(e) -> [Expr(type_of_expr_t e, e)] 
        | Some(i), Some(e) -> i@[Expr(type_of_expr_t e, e)]
        | _, _ -> failwith "invalid externalization"
    in
    match imp with
      Expr (_,e) -> flatten_external e 
    | Block (m,il) -> [Block(m, List.flatten (List.map rcr_aux il))]
    | Decl (unit,d,None) -> [imp]

    | Decl (unit, d, Some(init)) ->
        begin match desugar_expr opts init with
          | Some(i), Some(e) -> i@[Decl(unit, d, Some(e))]

          (* Avoid copying external types with equality operator, taking
           * a reference instead *)
          | None, Some(Var(Target(Type(x,td)),v) as e) ->
            let t, ref_t = let a = Target(Type(x,td)) in a, Target(Ref(a)) in
            [Decl(unit, (fst d, ref_t), Some(e))]

          (* TODO: revisit capturing reference/address for 
             nested external types.
          | None, Some(Fn(Target(Type(x)),Ext(MemberAccess _),args) as e) ->
            let t, ref_t = let a = Target(Type(x)) in a, Target(Ref(a)) in
            [Decl(unit, (fst d, ref_t), Some(Fn(t, Ext(ConstCast(t)), [e])))]
          *)

          | None, Some(e) -> [Decl(unit, d, Some(e))]
          | _, _ -> failwith "invalid declaration externalization" 
        end

    (* Two cases of for loops, declares iterators of either a materialized
     * collection, or as a subrange over a collection. Both apply the loop body
     * with the iterator substituted for declaration initializers (i.e. loop
     * body variables defined by the collection).
     * The resulting code uses an integer typed source (as opposed to a 
     * collection), with an iterator as the looping element. This method
     * purely performs loop rewriting to simplify the source code generator *)
    
    (* All loops over persistent collections should use slicing. 
     * Use slices inline, i.e. by iterating over members references, when
     * directly used in a loop *)
    | For(_,((id,id_t),f), Fn(c_ty, Slice(idx), args), body)
      ->
        let nprel, nargsl = List.split (List.map (fun a ->
            match desugar_expr opts a with
            | Some(i), Some(e) -> i, [e] 
            | None, Some(e) -> [], [e]
            | _, _ -> failwith "invalid collection externalization") args)
        in 
        let pre, nargs = List.flatten nprel, List.flatten nargsl in 
        
        let iter, iter_id, iter_end = let x = gensym() in 
           x, x^"_it", x^"_end" in
        let iter_decls, elem_ty, source_ty =
          let c_expr = List.hd nargs in
          (* assume types are correct in the input, and only modify on
           * realizing desugaring *)
          begin match type_of_expr_t c_expr with

          (* Pick the appropriate pattern index from a map type *)
          | Target(_) as c_t when is_ext_collection c_t ->
            let idx_t, elem_t, idx =
              if List.tl nargs = [] then
                c_t, (entry_type_of_collection c_t), c_expr
              else
              begin match ext_collection_of_type c_t with
              | Some(x) ->
                let pat_t, idx_t = index_of_collection x idx
                in idx_t, (entry_type_of_collection c_t),
                          Fn(idx_t, Ext(SecondaryIndex(pat_t)), [c_expr])
              | _ -> failwith ("invalid collection type "^(string_of_type c_t))
              end
            in
            let decls = range_iters_of_collection
                          iter_id iter_end idx idx_t (List.tl nargs) 
            in decls, elem_t, idx_t

          | _ -> failwith 
             "range iteration of tuples on non-multi-index container type"
          end
        in
        let iter_meta = (iter_id, iter_end, iter_decls, source_ty) in
        let new_body = sub_entry_access_in_loop (id, id_t) elem_ty body
        in pre@(sub_iters ((id,elem_ty),f) iter_meta (rcr new_body))

    (* Handle direct loops over data structures, which may be
     * temporaries or globals *)
    | For (_, elem, source, body) ->
        let pre, new_source = match desugar_expr opts source with
          | Some(i), Some(e) -> i, e 
          | None, Some(e) -> [], e
          | _, _ -> failwith "invalid collection externalization"
        in
        let iter, iter_id, iter_end = let x = gensym() in 
          x, x^"_it", x^"_end" in
        let iter_decls, new_source_ty = match new_source with
        | Var (_,(v,t)) ->
          (iters_of_collection iter_id iter_end new_source t), t

        | _ ->
          let c_id, c_ty = gensym(), type_of_expr_t new_source in
          let c_var = Var(c_ty, (c_id, c_ty)) in
          let decls =
            [Decl(unit, (c_id, c_ty), Some(new_source))]@
            (iters_of_collection iter_id iter_end c_var c_ty)
          in decls, c_ty
        in
        let iter_meta = (iter_id, iter_end, iter_decls, new_source_ty) in
        let new_elem, subbed_body =
          match elem, type_of_expr_t new_source with

          (* Rewrite loops over STL maps to use the std::pair format of map
           * entries rather than a single tuple or struct of keys and value. *)
          | ((id,(Host(TTuple id_tl) as id_t)), false), 
             Host(Collection(_, (TTuple _))) ->
            let key_t,value_t = back id_tl in 
            let flat_key, nk_t, nv_t = (List.length key_t = 1),
              Host (tuple_type_of_list key_t), Host value_t in
            let ne_t, nelem_d, ne_var =
              let x = Target(Pair(nk_t, nv_t)) in x, (id,x), Var(x, (id,x)) in
            let nkey = Fn(nk_t, Ext(PairFirst), [ne_var]) in
            let nvalue = Fn(nv_t, Ext(PairSecond), [ne_var]) in
            (* replace tuple accessors of the old element, with accessors
             * based on the new element definition in loop body declarations *)
            let subbed =
              let subs =
                let last_field = List.length id_tl - 1 in
                snd (List.fold_left (fun (i,acc) t ->
                  let src = Fn(Host t, TupleElement(i), 
                               [Var(id_t, (id, id_t))]) in
                  let dest = 
                    if i = last_field then nvalue 
                    else if flat_key then nkey
                    else Fn(Host t, TupleElement(i), [nkey])
                  in i+1,acc@[src, (Host t, dest)]) (0,[]) id_tl)
              in fixpoint_sub_imp (subs @ [Var(id_t, (id,id_t)), 
                                           (ne_t, ne_var)]) 
                                  body
            in (nelem_d,false), subbed 
          
          (* Non-tuple collections can simply bind the element directly from
           * an iterator, or substitute it with the iterator *)
          | _, Host(Collection _) -> elem, body

          (* Loops over global data structures *)
          | (((id,id_t),_), (Target(_) as c_t)) when is_ext_collection c_t ->
            elem, (sub_entry_access_in_loop (id,id_t) id_t body)

          (* Loop should be over a temporary or global collection. *)
          | _, t ->
            print_endline ("invalid for loop collection type " ^
                           (string_of_type t)^
                           ": "^(string_of_ext_imp imp));
            failwith ("invalid for loop collection type "^(string_of_type t)^
                      ": "^(ssc (source_code_of_expr new_source))) 
        in
        pre@(sub_iters new_elem iter_meta (rcr subbed_body))  

    | IfThenElse (condm,p,t,e) ->
        let r np = IfThenElse(condm, np, rcr t, rcr e) in
        begin match desugar_expr opts p with
          | Some(i), Some(e) -> i@[r e]
          | None, Some(e) -> [r e]
          | _, _ -> failwith "invalid condition externalization"
        end 
  and desugar_imp opts imp = match desugar_imp_aux opts imp with
    | [x] -> x
    | x -> let _,last = back x in Block(type_of_imp_t last, x)


  (* External typing interface *)
  type var_env_t = Typing.var_env_t
  let var_env_of_declarations = Typing.var_env_of_declarations

  let infer_types = Typing.infer_types

  (* Profiling code generation *)
  let profile_map_value_update c_id t_l map_t =
    let c_t, c_entry_t = (c_id^"_map"), (c_id^"_entry") in 
    let (k_decl,k) = snd (List.fold_left
        (fun (id,(dacc,vacc)) t ->
          (id+1,(dacc@[(cpp_of_type t)^" k"^(string_of_int id)],
                 vacc@["k"^(string_of_int id)])))
      (0,([],[])) t_l)
    in
    Lines(["void "^c_id^"_value_update("^c_t^"& m, "^
            (String.concat ", " k_decl)^", "^(cpp_of_type map_t)^" v)";
           "{";
           "  "^c_t^"::iterator it = m.find("^(mk_tuple k)^");";
           "  if (it != m.end()) {";
           "    m.modify(it, boost::lambda::bind(&"^
                         c_entry_t^"::__av, boost::lambda::_1) = v);";
           "  } else {";
           "    "^c_entry_t^" e("^(String.concat "," k)^",v);";
           "    m.insert(e);";
           "  }";
           "}"])

  let profile_nested_map_value_update c_id in_tl out_tl map_t =
    let c_out_id = c_id^"_out" in
    let c_t, c_entry_t, c_out_t, c_out_entry_t =
      c_id^"_map", c_id^"_entry", c_id^"_out_map", c_id^"_out_entry" in
    let get_decl_and_val i t_l = snd (List.fold_left
        (fun (id,(dacc,vacc)) t ->
          (id+1,(dacc@[(cpp_of_type t)^" k"^(string_of_int id)],
                 vacc@["k"^(string_of_int id)])))
      (i,([],[])) t_l)
    in
    let in_k_decl, in_k = get_decl_and_val 0 in_tl in
    let out_k_decl, out_k = get_decl_and_val (List.length in_tl) out_tl in
    Lines(["void "^c_id^"_value_update("^c_t^"& m, "^
             (String.concat ", " (in_k_decl@out_k_decl))^", "^(cpp_of_type map_t)^" v)";
           "{";
           "  "^c_t^"::iterator it = m.find("^(mk_tuple in_k)^");";
           "  if (it != m.end()) {";
           "    "^c_out_t^"& om = const_cast<"^c_out_t^"&>(it->__av);";
           "    "^c_out_id^"_value_update(om,"^(String.concat "," out_k)^",v);";
           "  } else {";
           "    "^c_out_t^" om;";
           "    "^c_out_entry_t^" oute("^(String.concat "," out_k)^",v);";
           "    om.insert(oute);";
           "    "^c_entry_t^" ine("^(String.concat "," in_k)^", om);";
           "    m.insert(ine);";
           "  }";
           "}"])

  let profile_map_update c_id in_tl out_tl map_t =
    let in_k_decl, in_k = snd (List.fold_left
        (fun (id,(dacc,vacc)) t ->
          (id+1,(dacc@[(cpp_of_type t)^" k"^(string_of_int id)],
                 vacc@["k"^(string_of_int id)])))
      (0,([],[])) in_tl)
    in
    let c_t, c_entry_t, c_out_t, c_out_entry_t =
      c_id^"_map", c_id^"_entry", c_id^"_out_map", c_id^"_out_entry"
    in
    let c_update_t =
      let t_l = if out_tl = [] then in_tl else out_tl in
      let update_k_t = mk_tuple_ty (List.map cpp_of_type t_l)
      in "map<"^update_k_t^","^(cpp_of_type map_t)^">"
    in
    let indirect_sc =
      let target_var, target_t, target_entry_t =
        if in_tl = [] || out_tl = [] then "m", c_t, c_entry_t
        else "om", c_out_t, c_out_entry_t in
      let update_loop =
        ["  "^target_t^"::iterator hint = "^target_var^".end();";
         "  "^c_update_t^"::iterator slice_it = u.begin();";
         "  "^c_update_t^"::iterator slice_end = u.end();";
         "  for (; slice_it != slice_end; ++slice_it) {";
         "    "^target_entry_t^" e(*slice_it);";
         "    hint = "^target_var^".insert(hint, e);";
         "  }"]
      in
      if in_tl = [] || out_tl = [] then
        ["void "^c_id^"_update("^c_t^"& m, "^c_update_t^"& u)";
         "{";"  m.clear();"]@update_loop@["}"]
      else
        ["void "^c_id^"_update("^c_t^"& m, "^
           (String.concat ", " in_k_decl)^", "^c_update_t^"& u)";
         "{";
         "  "^c_t^"::iterator it = m.find("^(mk_tuple in_k)^");";
         "  "^target_t^" om;"]@
            update_loop@
        ["  m.modify(it, boost::lambda::bind(&"^
                     c_entry_t^"::__av, boost::lambda::_1) = om);";
         "}"]
    in
    let direct_sc =
      let c_direct_t = if in_tl = [] || out_tl = [] then c_t else c_out_t in
      if in_tl = [] || out_tl = [] then
      ["void "^c_id^"_update_direct("^c_t^"& m, "^c_direct_t^"& u)";
       "{"; "  m = u;"; "}"]
      else
      ["void "^c_id^"_update_direct("^c_t^"& m, "^
         (String.concat ", " in_k_decl)^", "^c_direct_t^"& u)";
       "{";
       "  "^c_t^"::iterator it = m.find("^(mk_tuple in_k)^");";
       "  m.modify(it, boost::lambda::bind(&"^
                   c_entry_t^"::__av, boost::lambda::_1) = u);"; 
       "}"]
    in Lines(indirect_sc@direct_sc)

  let profile_map_element_remove c_id t_l =
    let c_t, c_entry_t = (c_id^"_map"), (c_id^"_entry") in 
    let (k_decl,k) = snd (List.fold_left
        (fun (id,(dacc,vacc)) t ->
          (id+1,(dacc@[(cpp_of_type t)^" k"^(string_of_int id)],
                 vacc@["k"^(string_of_int id)])))
      (0,([],[])) t_l)
    in
    Lines(["void "^c_id^"_remove("^c_t^"& m, "^
            (String.concat ", " k_decl)^")";
           "{";
           "  m.erase("^(mk_tuple k)^");";
           "}"])

  let declare_profiling (schema: K3.map_t list) =
    cscl ~delim:"\n" (List.flatten (List.map (fun (id, in_tl, out_tl, map_t) ->
      match List.map snd in_tl, List.map snd out_tl with
        | [],[] -> []
        | (x as i),([] as o) | ([] as i),(x as o) ->
          [profile_map_value_update id x map_t;
           profile_map_update id i o map_t] 
        | x,y ->
          [profile_map_value_update (id^"_out") y map_t;
           profile_nested_map_value_update id x y map_t;
           profile_map_update id x y map_t])
     schema))

  let profile_ivc (schema : K3.map_t list) map_ivc_ids stmt_name imp =
    let is_schema_map id = List.exists (fun (x,y,z,t) -> x=id) schema in
    let mii_ref = ref map_ivc_ids in 
    let bui_f _ _ i = match i with
      | IfThenElse(if_t,
          (BinOp(o_t, Neq,
                 Fn(fn_t, Ext(FindCollection), fn_args), op_r_i) as pred_i),
          then_i, else_i) ->
        if fn_args <> [] then
          begin match List.hd fn_args with
            | Var(m,(id,t)) when is_schema_map id ->
              let ivc_id = stmt_name^"_ivc"^id in
              let prof_id = 
                let (cnt, ivc_cnts) = !mii_ref in
                let nivc_cnts =
                  if List.mem_assoc ivc_id ivc_cnts then
                    (ivc_id, (List.assoc ivc_id ivc_cnts)@[cnt])::
                      (List.remove_assoc ivc_id ivc_cnts)
                  else ivc_cnts@[ivc_id, [cnt]]
                in mii_ref := cnt+1, nivc_cnts; (string_of_int cnt) 
              in
              let new_else_i =
                let prof_begin = Expr(unit, Fn(unit, Ext(Inline(
                  "ivc_stats->begin_probe("^prof_id^")")), []))
                in
                let prof_end = Expr(unit, Fn(unit, Ext(Inline(
                  "ivc_stats->end_probe("^prof_id^")")), []))
                in 
                Block(unit, [prof_begin; else_i; prof_end])
              in IfThenElse(if_t, pred_i, then_i, new_else_i)
            | _ -> i 
          end
        else i
      | _ -> i
    in
    let new_imp =
      fold_imp bui_f (fun _ _ e -> Block(unit,[])) (fun x _ -> x) (fun x _ -> x)
        None (Block(unit, [])) imp
    in !mii_ref, new_imp

  let profile_stmt (schema : K3.map_t list) sid_offset ivc_offsets stmt_id
                   trig_name iarg_decls iargs imp =
    let sid = string_of_int stmt_id in
    let prof_begin = Expr(unit, Fn(unit, Ext(Inline(
      "exec_stats->begin_probe("^
      (string_of_int (sid_offset+stmt_id))^")")), []))
    in
    let prof_end = Expr(unit, Fn(unit, Ext(Inline(
      "exec_stats->end_probe("^(string_of_int (sid_offset+stmt_id))^")")), []))
    in
    let stmt_name = trig_name^"_s"^sid in
    let nivc_offsets, new_decl =
      let nivco, prof_imp = profile_ivc schema ivc_offsets stmt_name imp in
      let bimp = match prof_imp with | Block _ -> prof_imp
                                     | _ -> Block(unit, [prof_imp])
      in nivco,
         [Lines (["void "^stmt_name^"("^(String.concat ", " iarg_decls)^"){"])]@
                 [source_code_of_imp bimp]@[Lines (["}"])]
    in nivc_offsets, new_decl,
       [prof_begin; 
        Expr(unit, Fn(unit, Ext(Apply(stmt_name)), iargs)); 
        prof_end]

  let profile_trigger sid_offset ivc_offsets dbschema 
                      (schema : K3.map_t list) (event,imp) =
    let trig_name = Schema.name_of_event event in
    let iarg_decls, iargs = List.split (List.map
        (fun (a,ty) ->
          let i_t = imp_type_of_calc_type ty in
          (Type.string_of_type ty)^" "^a, Var(i_t, (a, i_t)))
        (Schema.event_vars event))
    in
    let imp_stmts = match imp with | Block(_,s) -> s
                                   | _ -> failwith "invalid non-block trigger"
    in
    let nivc_offsets, decls, new_stmts = snd (
      List.fold_left (fun (stmt_id,(ivc_acc,dacc,sacc)) imp ->
          let nivc_acc, new_decl, new_imp =
            profile_stmt schema sid_offset ivc_acc stmt_id
                         trig_name iarg_decls iargs imp
          in
          stmt_id+1, (nivc_acc,dacc@new_decl,sacc@new_imp))
        (0,(ivc_offsets,[],[])) imp_stmts)
    in let profiled_trig =
      [Expr(unit, Fn(unit,
         Ext(Inline("exec_stats->begin_trigger(\""^trig_name^"\")")), []));
       Expr(unit, Fn(unit,
         Ext(Inline("ivc_stats->begin_trigger(\""^trig_name^"\")")), []))]@
      new_stmts@
      [Expr(unit, Fn(unit,
         Ext(Inline("exec_stats->end_trigger(\""^trig_name^"\")")), []));
       Expr(unit, Fn(unit,
         Ext(Inline("ivc_stats->end_trigger(\""^trig_name^"\")")), []))]
    in
    nivc_offsets, (decls, (event, Block(unit, profiled_trig))) 
  
  (* Top-level code generation *)
  let preamble opts = Lines
      ( (if Debug.active "CPP-TRACE" then ["#define DBT_TRACE   1"] else [])@
        (if opts.profile             then ["#define DBT_PROFILE 1"] else [])@
        ["#include \"program_base.hpp\"";
         "";
         "namespace dbtoaster {";])
         
  let ending = Lines ["}";]

  (* Relation identifier generation *)
  let declare_relations (dbschema:Schema.rel_t list) = Lines        
    (List.map (fun (rel,_,rel_type) -> 
                ("pb.add_relation(\""^(String.escaped rel)^"\""^
                    (if rel_type = TableRel then ", true" else "")^");")) 
              dbschema)
 
  (* Generates source code for map declarations, based on the
   * type_of_map_schema function above *)
  let declare_maps_and_triggers opts indent (imp_prog:imp_prog_t) =
    let (map_schema, patterns, (triggers,trig_reg_info)), 
         sources, (tlq_schema, tlqs) = imp_prog in
    
    let compile_types = List.fold_left 
      (fun acc (id,in_tl,out_tl,mapt) ->
        (* Declare map size constraint macros for garbage collection. *)
        let size_macro = 
          if in_tl = [] then []
          else [Lines ["#ifndef "^id^"_map_SIZE";
                           "#define "^id^"_map_SIZE DEFAULT_MAP_SIZE";
                           "#endif";"";]] in
        let (_,_,t_defs) = type_of_map_schema patterns 
                                              (id, in_tl, out_tl, mapt) 
        in        
        acc@size_macro@(List.map source_code_of_type t_defs))
      []
    in
    
    let compile_decls = List.fold_left 
      (fun (d_acc, i_acc) (id,in_tl,out_tl,mapt) ->
        let (_,t,_) = type_of_map_schema patterns (id, in_tl, out_tl, mapt) in
        let has_init = match t with 
          | Host(TBase(TInt)) | Host(TBase(TFloat)) -> true
          | _ -> false
        in 
        let imp_decl = match t with 
          | Host(TBase(TFloat)) -> 
            Decl(unit, (id,t), Some(Fn(t, Ext(Constructor(t)), 
                 [Const(unit, CFloat(0.0))]))) 
          | Host(TBase(TInt)) -> 
            Decl(unit, (id,t), Some(Fn(t, Ext(Constructor(t)), 
                 [Const(unit, CInt(0))])))
          | _ -> Decl(unit, (id,t), None)
        in 
        d_acc@[source_code_of_imp ~gen_init:false imp_decl],
        i_acc@( if has_init 
                then [source_code_of_decl ~gen_type:false imp_decl] 
                else [] ) )
      ([], [])
    in
    
    let compile_registration = List.fold_left 
      (fun acc (id,in_tl,out_tl,mapt) ->
        let e_id = String.escaped id in 
        let (_,t,_) = type_of_map_schema patterns (id, in_tl, out_tl, mapt) in
        acc@["pb.add_map<"^(string_of_type t)^">( \""^e_id^"\", "^e_id^" );"] )
      []
    in
    
    let compile_serialization = List.fold_left 
      (fun acc (id,t) ->
         acc@[t^" _"^id^" = get_"^id^"();";
              "ar & boost::serialization::make_nvp(BOOST_PP_STRINGIZE(" ^
              id ^ "), _" ^ id ^ ");"] )
      []
    in
    
    let tlq_d_l, tlq_i_l = compile_decls tlq_schema in
    let tlq_s_l = compile_serialization (List.map fst tlqs) in
    let flat_tlqs =
      [inl ("/* Functions returning / computing the results " ^ 
            "of top level queries */")]@ 
      List.map snd tlqs
    in
    
    let d_l, i_l = compile_decls (ListAsSet.diff map_schema tlq_schema) in
    let r_l = compile_registration map_schema in
    
    let dbschema = Schema.rels sources in    
    let register_relations = declare_relations dbschema in
    
    let table_rels = Schema.table_rels sources in
    let register_table_triggers = Lines (List.map
      (fun (id,_,_) -> "pb.add_trigger(\""^id^"\", insert_tuple,"^
        " boost::bind(&data_t::unwrap_insert_"^id^
        ", this, ::boost::lambda::_1));")
      table_rels)
    in  

    let declare_table_triggers =
       let table_trigger (r_id,r_vars,_) =
          let entry_t = r_id^"_entry" in
          let table_t = r_id^"_map" in
        
          let _, evt_unwrap, trig_args, entry_ctor = List.fold_left
            (fun (i,acc_e,acc_a,acc_c) (v,t) ->
               let ty = Typing.string_of_type (imp_type_of_calc_type t)
               in 
               (i+1,
                 acc_e@["any_cast<"^ty^">(ea["^(string_of_int i)^"])"],
                 acc_a@[ty^" "^v],
                 acc_c@[v])
            )
            (0,[],[],[]) r_vars
          in
          let modify_lambda = 
            "boost::lambda::bind(&" ^ entry_t ^ 
            "::__av, boost::lambda::_1) = ret.first->__av+1"
          in
          ["void on_insert_"^r_id^"("^(String.concat ", " trig_args)^") {";
           tab^entry_t^" e("^(String.concat ", " entry_ctor)^", 1);";
           tab^"pair<"^table_t^"::iterator,bool> ret = "^r_id^".insert(e);";
           tab^"if( !ret.second )       "^r_id^
               ".modify( ret.first, "^modify_lambda^" );";
           "}"; "";
           "void unwrap_insert_"^r_id^"(const event_args_t& ea) {";
           tab^"on_insert_"^r_id^"("^(String.concat ", " evt_unwrap)^");";
           "}"; "";]
       in
       Lines (
          ["/* Trigger functions for table relations */"]@ 
          (List.flatten (List.map table_trigger table_rels)))
    in

    let register_stream_triggers = Lines 
      (List.map (fun (rel, evt_type, unwrap_fn_id,_) ->
        let args = String.concat ", " [
            rel; 
            evt_type; 
            ("boost::bind(&"^unwrap_fn_id^", this, ::boost::lambda::_1)")
        ] in
          if evt_type = "insert_tuple" || evt_type = "delete_tuple" 
          then ("pb.add_trigger("^args^");") else ("")) 
        (fst trig_reg_info))
    in
    
    let declare_stream_triggers = cscl (
       Lines ["/* Trigger functions for stream relations */"] ::         
       (List.flatten 
          (List.map (fun (t_decls,t) -> 
             t_decls@(source_code_of_trigger dbschema t))
           triggers)))
    in
    let profiling = if opts.profile then (declare_profiling map_schema) 
                    else Lines([]) 
    in
    let init_stats =
      let stmt_ids = List.flatten (List.map (fun (_,_,_,sids) ->
         List.map (fun (i,n) ->
           "exec_stats->register_probe("^i^", \""^(String.escaped n)^"\");"
         ) sids) (fst trig_reg_info))
      in
      let ivc_ids =
        List.fold_left (fun acc (n,cl) ->
          acc@(List.map (fun i ->
            "ivc_stats->register_probe("^(string_of_int i)^", \""^
            (String.escaped n)^"\");") cl)
        ) [] (snd trig_reg_info)
      in
        Lines (["#ifdef DBT_PROFILE";
                "exec_stats = pb.exec_stats;";
                "ivc_stats = pb.ivc_stats;";
               ] @ stmt_ids @ ivc_ids @ ["#endif // DBT_PROFILE"])
    in
      
      
    isc indent (cscl ~delim:"\n" ([
      (cscl
         ((inl ("/* Definitions of auxiliary maps for storing " ^ 
                "materialized views. */")) ::
          (compile_types map_schema)));
      Lines [ 
      "/* Type definition providing a way to access " ^ 
      "the results of the sql program */";
      "struct tlq_t{";
      tab^"tlq_t()"^(if tlq_i_l <> [] 
                     then " : "^(ssc (cscl ~delim:"," tlq_i_l)) 
                     else "");
      tab^"{}";
      "";
      tab^"/* Serialization Code */";
      tab^"template<class Archive>";
      tab^"void serialize(Archive& ar, const unsigned int version) {";];
      isc (tab^tab) (Lines tlq_s_l);  
      Lines [
      tab^"}";];
      isc tab (cscl flat_tlqs);
      Lines ["protected:";];      
      isc tab (cscl ((inl 
        ("/* Data structures used for " ^ 
        "storing / computing top level queries */"))::tlq_d_l));
      Lines [ 
      "};";
      "";
      ("/* Type definition providing a way to incrementally maintain " ^
           "the results of the sql program */");
      "struct data_t : tlq_t{";
      tab^"data_t()"^
      (if i_l <> [] then " : "^(ssc (cscl ~delim:"," i_l)) else "");
      tab^"{}";
      "";
      "#ifdef DBT_PROFILE";
      ("boost::shared_ptr<dbtoaster::statistics::trigger_exec_stats> " ^ 
       "exec_stats;");
      ("boost::shared_ptr<dbtoaster::statistics::trigger_exec_stats> " ^ 
       "ivc_stats;");
      "#endif";
      "";
      tab^"/* Registering relations and trigger functions */";
      tab^"void register_data(ProgramBase& pb) {";];
      isc (tab^tab) (Lines r_l);
      isc (tab^tab) register_relations;
      isc (tab^tab) register_table_triggers;
      isc (tab^tab) register_stream_triggers;
      isc (tab^tab) init_stats;
      Lines [tab^"}";];
      isc tab declare_table_triggers;
      isc tab declare_stream_triggers;
      isc tab profiling;
      Lines ["private:"];
      isc tab (cscl ((inl ("/* Data structures used for storing " ^
                           "materialized views */"))::d_l)); 
      Lines ["};";];
      ])) 
    

  let declare_sources_and_adaptors (sources: Schema.source_info_t list ref) =
    let quote s = "\""^(String.escaped s)^"\"" in
    let valid_adaptors = ["csv"      , "csv_adaptor";
                          "orderbook", "order_books::order_book_adaptor"]
    in
    let array_of_id id = id^"[]" in
    let array_of_type t = match t with
        | Target(Type(x,None)) -> Target(Type(x^"[]",None))
        | _ -> t
    in
    let mk_array l = "{ "^(String.concat ", " l)^" }" in
    let decls_for = List.map (fun ((sf:Schema.source_t), ra) ->
      let adaptor_meta = 
      List.fold_left
      (fun acc ((a:Schema.adaptor_t),((r,fields,_):Schema.rel_t)) ->
        if List.mem_assoc (fst a) valid_adaptors then
          let a_t_id = List.assoc (fst a) valid_adaptors in
          let a_spt = "shared_ptr<"^a_t_id^" >" in
          let a_id, a_t = gensym(), Target(Type(a_spt,None)) in
          let a_params =
            let ud_params = snd a in
            if List.mem_assoc "schema" ud_params then ud_params
            else
              let a_schema =
                List.map (fun (_,t) ->
                  Typing.string_of_type (imp_type_of_calc_type t)) fields
              in ud_params@[("schema", String.concat "," a_schema)]
          in
          let params_arr = mk_array (List.map (fun (k,v) ->
            "make_pair("^(quote k)^","^(quote v)^")") a_params) in
          let param_id, param_t, param_arr_t =
            let t = Target(Type("pair<string,string>",None)) in
            a_id^"_params", t, array_of_type t in
          let param_d = Decl(unit, (array_of_id param_id, param_t),
            Some(Fn(param_arr_t, Ext(Inline(params_arr)), [])))
          in
          let d_ctor_args =
            let rel_id = ("get_relation_id(\""^(String.escaped r)^"\")")
            in
            String.concat "," ([rel_id]@
                               [string_of_int (List.length a_params); param_id])
          in
          let d = Decl(unit, (a_id, a_t), 
                    Some(Fn(a_t, Ext(Constructor(a_t)),
                      [Fn(Target(Type(a_t_id,None)),
                       Ext(Inline("new "^a_t_id^"("^d_ctor_args^")")), [])])))
          in acc@[a_id, [param_d;d]]
        else failwith ("unsupported adaptor of type "^(fst a)))
        [] ra
      in
      let source_var, source_decl = match sf with
        | FileSource(n, Delimited(d)) ->
          let f_id, f_t = gensym(), Target(Type("frame_descriptor",None)) in
          let f_decl = Decl(unit, (f_id, f_t),
            Some(Fn(f_t, Ext(Constructor(f_t)),
              [Fn(Target(Type("string",None)), Ext(Inline(quote d)), [])])))
          in
          let ad_arr_id, ad_ptr_t, ad_arr_t =
            let t = Target(Type("shared_ptr<stream_adaptor>",None)) in
            gensym(), t, array_of_type t in
          let ad_arr = mk_array (List.map fst adaptor_meta) in
          let ad_arr_decl = Decl(unit, (array_of_id ad_arr_id, ad_ptr_t),
            Some(Fn(ad_arr_t, Ext(Inline(ad_arr)), [])))
          in
          let al_id, al_t =
            gensym(), 
            Target(Type("std::list<shared_ptr<stream_adaptor> >",None))
          in
          let ad_arr_it_expr = 
            ad_arr_id ^ ", " ^ ad_arr_id ^ " + sizeof(" ^
            ad_arr_id ^ ") / sizeof(shared_ptr<stream_adaptor>)"
          in
          let al_decl = Decl(unit, (al_id, al_t),
            Some(Fn(al_t, Ext(Constructor(al_t)),
              [Fn(ad_arr_t, Ext(Inline(ad_arr_it_expr)), [])])))
          in
          let s_id, s_t, s_ptr_t =
            let x = "dbt_file_source" in 
            gensym(), Target(Type(x,None)),
            Target(Type("shared_ptr<"^x^">",None))
          in
          let s_ctor =
            let dbt_fs_ctor = ssc (source_code_of_expr
              (Fn(s_t, Ext(Constructor(s_t)),
                 [Fn(Target(Type("string",None)), Ext(Inline(quote n)), []);
                    Var(f_t, (f_id, f_t)); Var(al_t, (al_id, al_t))])))
            in Fn(s_ptr_t, Ext(Constructor(s_ptr_t)),
                    [Fn(s_t, Ext(Inline("new "^dbt_fs_ctor)), [])])
          in
          let s_decl = Decl(unit, (s_id, s_ptr_t), Some(s_ctor)) in
            Var(s_ptr_t, (s_id, s_ptr_t)), 
            [f_decl; ad_arr_decl; al_decl; s_decl]
          
        | _ -> failwith "unsupported data source and framing types"

      in source_var, (List.flatten (List.map snd adaptor_meta))@source_decl)
    in 
    
    let decls_code_for is_table_src srcs =
      let decls = decls_for (List.filter (fun (s,_) -> 
         (s <> NoSource) ) srcs) 
      in
      let register_src = function
         | Var(_,(id,_)) -> 
            Lines [ "add_source(" ^ id ^ ( if is_table_src 
                                           then ", true" 
                                           else "") ^ ");" ]
          | _ -> failwith "invalid source"
      in
      cscl ~delim:"\n" (List.map (fun (sv,sd) ->
        cscl ((List.map source_code_of_imp sd)@[register_src sv])) decls)
    in
    
    let table_sources, stream_sources = 
      Schema.partition_sources_by_type sources 
    in
    cscl ~delim:"\n"
        [inl "/* Specifying data sources */";
         decls_code_for true  table_sources;
         decls_code_for false stream_sources;]
            
end (* Target *)


(* An imperative compiler implementation *)
module Compiler =
struct
  open K3ToImperative.DirectIRBuilder
  open K3ToImperative.Imp
  open Imperative
  open Common
  open Target

  module IBK = K3ToImperative.AnnotatedK3
  module IBC = K3ToImperative.Common
  
  (* Internal type conversion helper function *)
  let imp_type_of_calc_type t = Host(K3.TBase(t))
  
  (* Compiles a single K3 expression with a return value, 
   * given a type environment *)
  let compile_k3_expr (opts:compiler_options) 
                      (arg_types:(string * ext_type type_t) list) 
                      (schema:K3.map_t list) 
                      (patterns:Patterns.pattern_map) 
                      (expr:K3.expr_t) =
    let var_env = var_env_of_declarations arg_types schema patterns in
    let ir = ir_of_expr expr in
    let expr_var = (IBC.sym_of_meta (IBK.meta_of_ir ir)) in
    
    let untyped_imp =
       let i = imp_of_ir var_env ir in 
       match i with 
          | [x] -> x 
          | _ -> Imperative.Block (None, i)
    in
    Debug.print "UNTYPED-IMP" (fun () -> string_of_imp_noext untyped_imp);  
    let _typed_imp = infer_types var_env untyped_imp in
    let typed_imp = if opts.desugar 
                    then desugar_imp opts _typed_imp 
                    else _typed_imp
    in
    
    let rec type_of_expr s = match s with
      | Imperative.Block (_, i_l) ->
        List.fold_left (fun t_l bs -> t_l@(type_of_expr bs)) [] i_l
      | Imperative.Decl (_,(d_var,ty), _) ->
         if d_var = expr_var then
            (*print_endline (
               "Typed_imp: " ^ 
               (string_of_typed_imp (Target.Typing.string_of_type) 
                                    (fun _ -> " hmm ") s));
            print_endline ("Found type for " ^ expr_var ^ 
                           ": " ^ (Target.Typing.string_of_type ty));*)
            [ty]
         else
            []
      | _ -> []
    in
    let expr_type = match (type_of_expr typed_imp) with
      | [ty] -> ty
      | _ -> failwith 
         "ImperativeCompiler: No type found for root variable of k3_expr."
    in
      (typed_imp),(expr_var,expr_type)
  
   (* Compiles a single K3 statement given a type environment *)
   let compile_k3_stmt (opts:compiler_options) 
                      (arg_types:(string * ext_type type_t) list) 
                      (schema:K3.map_t list) 
                      (patterns:Patterns.pattern_map) 
                      (stmt:K3.expr_t) =
      let var_env = var_env_of_declarations arg_types schema patterns in
      let untyped_imp =
         let i = imp_of_ir var_env (ir_of_expr stmt)
         in match i with 
         | [x] -> x 
         | _ -> Imperative.Block (None, i)
      in
          
      Debug.print "UNTYPED-IMP" (fun () -> string_of_imp_noext untyped_imp);  
      let typed_imp = infer_types var_env untyped_imp in
      if opts.desugar then desugar_imp opts typed_imp else typed_imp
    
  (* Compiles a K3 trigger, setting up a type environment based on the
   * given datastructure schemas and access patterns *)
  let compile_k3_trigger (opts:compiler_options) 
                         (dbschema:Schema.rel_t list)
                         (schema:K3.map_t list)
                         (patterns:Patterns.pattern_map)
                         (counter:int) 
                         (ivc_counters:(int * (string * int list) list))
                         (event:Schema.event_t) 
                         (k3stmts:K3.expr_t list) =
    let arg_types = 
      List.map 
         (fun (vn,vt) -> vn, imp_type_of_calc_type vt)
         (Schema.event_vars event)
    in
    let compile_f = fun stmt -> compile_k3_stmt opts arg_types 
                                                schema patterns stmt 
    in
    let i = Imperative.Block (Host TUnit, List.map compile_f k3stmts) in
    let t = (event,i) in
    if not opts.profile then (0,[]),([],t)
    else profile_trigger counter ivc_counters dbschema schema t

  (* Builds registration metadata for the imperative main function *)
  let build_trigger_setup_info (counter:int) 
                               (event:Schema.event_t) 
                               (stmts:'a list) =
    let ep = Schema.class_name_of_event event in
    let tnm = Schema.name_of_event event in
    let exec_ids = snd (List.fold_left (fun (i,acc) _ ->
      let x = string_of_int (counter+i)
      in i+1, acc@[x, tnm^"_s"^(string_of_int i)]) (0,[]) stmts)
    in ( ("\""^(String.escaped (Schema.rel_name_of_event event))^"\""), 
         ep,("data_t::unwrap_"^tnm),exec_ids)

  (* Top-level K3 trigger program compilation *)
  let compile_triggers opts dbschema ((schema,patterns),trigs,tlqs) :
    (source_code_t list * compiler_trig_t) list * trigger_setup_t 
  =
    let ivc_counters, compiler_trigs = snd (
      List.fold_left (fun (gcnt, (ivc_cnt, acc)) (event,k3stmts) ->
        let nivc_cnt, r = compile_k3_trigger opts dbschema schema patterns
                            gcnt ivc_cnt event k3stmts in
        gcnt+(List.length k3stmts), (nivc_cnt, acc@[r]))
      (0,((0,[]),[])) trigs)
    in
    let trigger_setup =
      snd (List.fold_left (fun (gcnt, acc) (event,stmts) ->
        gcnt+(List.length stmts),
        acc@[build_trigger_setup_info gcnt event stmts])
      (0, []) trigs)
    in compiler_trigs, (trigger_setup, snd ivc_counters)

  (* Top-level K3 queries compilation *)
  let compile_tlqs opts schema patterns tlqs
  =
    let tlq_get_schema tlq = get_expr_map_schema (snd tlq) in
    ListAsSet.multiunion (List.map tlq_get_schema tlqs)
    ,
    List.map ( fun (tlq_name,tlq_k3expr) ->
      let tlq_code,(tlq_var,_tlq_type) =
        (compile_k3_expr opts [] schema patterns tlq_k3expr)
      in
      let tlq_sc = match tlq_code with
         | Imperative.Block (_, i_l) -> cscl (List.map source_code_of_imp i_l)
         | _ ->  source_code_of_imp tlq_code
      in 
      let tlq_type = Target.Typing.string_of_type _tlq_type
      in      
      (tlq_name,tlq_type),
      cscl [
        Lines [tlq_type^" get_"^tlq_name^"(){"];
        isc tab tlq_sc;
        Lines [tab^"return "^tlq_var^";";"}";];
      ])
    tlqs

  let compile_imp opts (imp_prog:imp_prog_t) =
    let (map_schema, patterns, (triggers, trig_reg_info)), 
         sources, (tlq_schema, tlqs) = imp_prog in
    
    let maps_and_triggers = declare_maps_and_triggers opts tab imp_prog in
    let sources_and_adaptors = declare_sources_and_adaptors sources in
    
    let main_fn = (isc tab (cscl ~delim:"\n"
        ([Lines [
         "/* Type definition providing a way to execute the sql program */";
         "class Program : public ProgramBase";
         "{";
         "public:";
         tab^"Program(int argc = 0, char* argv[] = 0) : " ^
             "ProgramBase(argc,argv) {";
         tab^tab^"data.register_data(*this);";];
         isc (tab^tab) sources_and_adaptors;
         isc tab (inl "}");
         Lines [
          tab^"/* Imports data for static tables and performs " ^ 
              "view initialization based on it. */";
          tab^"void init() {";
          tab^tab^"process_tables();";
          tab^tab^"data.on_"^
            (Schema.name_of_event Schema.SystemInitializedEvent)^"();";
          tab^"}";
          "";
          tab^"/* Saves a snapshot of the data required to obtain " ^
              "the results of top level queries. */";
          tab^"snapshot_t take_snapshot(){";
          tab^tab^"return snapshot_t( new tlq_t((tlq_t&)data) );";
          tab^"}";
          "";
          "protected:";
          tab^"data_t data;";
          "};";];
        ] )))
    in
    
      strings_of_source_code (
         concat_and_delim_source_code_list ~delim:"\n"
            [preamble opts; 
             maps_and_triggers; 
             main_fn;
             ending;]
      )

  let imp_of_k3 opts (sources,(schema,patterns),trigs,tlqs):imp_prog_t =
      (  (  schema,patterns,
            compile_triggers opts (Schema.rels sources) 
                             ((schema,patterns),trigs,tlqs) ),
         sources,
         compile_tlqs opts schema patterns tlqs
      )

  let compile_query opts k3_prog =
    compile_imp 
      opts 
      (imp_of_k3 opts k3_prog)

  (* DS program compilation *)
  (*
  let compile_ds_triggers opts dbschema 
                          (((schema, patterns), dstrigs, tlqs)) :
    (source_code_t list * compiler_trig_t) list * trigger_setup_t 
  =
    let ivc_counters, compiler_trigs = snd (
      List.fold_left (fun (gcnt, (ivc_cnt, acc)) (event, ds_stmts) ->
        (* Retrieve statement-specific declarations during compilation *)
        let stmt_decls = List.fold_left (fun acc ((lsch, lpats), stmts) ->
          let ltd, ld = declare_maps_of_schema lsch lpats in
          acc@[stmts, (lsch, lpats, ltd, ld)]) [] ds_stmts
        in
        let local_decl_f schema patterns stmt =
          if not(List.mem_assoc stmt stmt_decls) then schema, patterns, []
          else
            let ls,lp,_,ld = List.assoc stmt stmt_decls
            in schema@ls, (Patterns.merge_pattern_maps patterns lp), [ld]
        in
        let ds_type_decls = List.map (fun (_,s) ->
          let _,_,td,_ = List.assoc s stmt_decls in td) ds_stmts in
        let k3stmts = (List.map snd ds_stmts) in
        let nivc_cnt, (sc_l, t) = compile_k3_trigger opts
          dbschema local_decl_f schema patterns gcnt ivc_cnt event k3stmts
        in gcnt+(List.length k3stmts),
           (nivc_cnt, acc@[(sc_l@ds_type_decls), t]))
      (0,((0,[]),[])) dstrigs)
    in
    let trigger_setup =
      snd (List.fold_left (fun (gcnt, acc) (event,stmts) ->
        gcnt+(List.length stmts),
        acc@[build_trigger_setup_info gcnt event stmts])
      (0, []) dstrigs)
    in compiler_trigs, (trigger_setup, snd ivc_counters)

  let compile_ds_query opts ((mapsch,pats),dstrigs,tlqs) sources =
    compile_imp 
      opts
      (  (  mapsch,pats,
            compile_ds_triggers opts (Schema.rels sources) 
                                ((mapsch,pats),dstrigs,tlqs) ),
         sources,
         (List.map fst tlqs))
        
   *)

end
