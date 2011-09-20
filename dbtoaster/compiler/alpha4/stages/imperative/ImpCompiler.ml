open M3
open K3.SR

type compiler_options = { desugar : bool; profile : bool }

module Common =
struct
  include SourceCode

  let sym_counter = ref 0
  let gensym () = incr sym_counter; "__y"^(string_of_int (!sym_counter))

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


(* Type signature for target language functor *)
module type ImpTarget =
sig
  open Imperative
  open Common

  type ext_type
  type ext_fn
  type type_env_t    

  val string_of_ext_type : ext_type type_t -> string
  val string_of_ext_fn : ext_fn -> string
  val string_of_ext_imp : (ext_type, ext_fn) typed_imp_t -> string

  (* Serialization code generation *)
  
  (* Generate source code that resides within an entry definition
   * for serialization *)
  val serialization_source_code_of_entry :
    K3.SR.id_t -> (K3.SR.id_t * ext_type type_t) list -> source_code_t
    
  (* Map serialization invocation *)
  val source_code_of_map_serialization :
    K3.SR.id_t -> K3.SR.id_t -> source_code_t

  (* Source code generation *)
  val source_code_of_imp : (ext_type, ext_fn) typed_imp_t -> source_code_t
  val source_code_of_trigger :
    (string * Calculus.var_t list) list ->
    (M3.pm_t * M3.rel_id_t * M3.var_t list * (ext_type, ext_fn) typed_imp_t) 
    -> source_code_t list
    
  val desugar_expr :
    compiler_options -> type_env_t
    -> (ext_type, ext_fn)  typed_expr_t 
    -> ((ext_type, ext_fn) typed_imp_t list) option * 
       ((ext_type, ext_fn) typed_expr_t) option

  val desugar_imp :
    compiler_options -> type_env_t
    -> (ext_type, ext_fn) typed_imp_t -> (ext_type, ext_fn) typed_imp_t

  (* Typing interface *)
  val var_type_env : type_env_t -> (K3.SR.id_t * ext_type type_t) list
  
  val type_env_of_declarations :
    (K3.SR.id_t * ext_type type_t) list ->
    M3.map_type_t list -> M3Common.Patterns.pattern_map -> type_env_t

  val infer_types :
    type_env_t -> ('a option, ext_type, ext_fn) imp_t
    -> (ext_type, ext_fn) typed_imp_t

  (* Profiler code generation *)
  val declare_profiling : M3.map_type_t list -> source_code_t
  val profile_trigger :
    int -> (string * Calculus.var_t list) list ->
    (ext_type type_t, ext_type, ext_fn) Program.trigger_t
    -> source_code_t list * (ext_type type_t, ext_type, ext_fn) Program.trigger_t 

  (* Toplevel code generation *)

  (* TODO: all of these should accept a compiler options record as input *)
  val preamble : compiler_options -> source_code_t

  val declare_maps_of_schema :
    M3.map_type_t list -> M3Common.Patterns.pattern_map -> source_code_t

  val declare_streams :
    (string * Calculus.var_t list) list -> (string * int) list * source_code_t

  val declare_sources_and_adaptors :
    M3.relation_input_t list -> (ext_type, ext_fn) typed_expr_t list * source_code_t

  val declare_main :
    compiler_options
    -> (string * int) list -> M3.map_type_t list
    -> (ext_type, ext_fn) typed_expr_t list
       (* Trigger registration metadata *)
    -> (string * string * string * (string * string) list) list
    -> string list -> source_code_t * source_code_t
end


(* Imperative stage: supports compilation from a K3 program *)

module type CompilerSig =
sig
  (* Similar interface to (M3|K3)Compiler.compile_query *)
  val compile_query_to_string :
    compiler_options ->
    (string * Calculus.var_t list) list (* dbschema *)
    -> K3.SR.program                    (* K3 program *)
    -> M3.relation_input_t list         (* sources *)
    -> string list -> string

  val compile_query :
    compiler_options ->
    (string * Calculus.var_t list) list (* dbschema *)
    -> K3.SR.program                    (* K3 program *)
    -> M3.relation_input_t list         (* sources *)
    -> string list -> Util.GenericIO.out_t -> unit
end


(* A C++ compiler target implementation *)
module CPPTarget : ImpTarget =
struct
  module K = K3.SR

  open Imperative
  open Common

  type fields_t = (id_t * ext_type type_t) list
  and ext_type =
    | Type of id_t
    | Ref of ext_type type_t
    | Iterator of ext_type type_t

      (* differentiate std::pair from tuple *)
    | Pair of ext_type type_t * ext_type type_t

      (* name, entry name, list of indices, w/ tag, fields per index *)
    | MultiIndexDef of id_t * id_t * fields_t * ((id_t * fields_t) list)

    | TypeDef of id_t * id_t
    | StructDef of id_t * fields_t
    | EntryStructDef of id_t * fields_t

  type ext_fn = 
    | Apply of string (* Function all *)
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


  (* AST stringification *)
  let rec string_of_ext_type t =
    let quote s = "\""^s^"\"" in
    let sty = Imperative.string_of_type string_of_ext_type in
    let of_tlist tl = String.concat "," (List.map sty tl) in
    match t with 
    | Host _ -> failwith "unhandled host type"
    | Target(Type id) -> "Target(Type("^(quote id)^"))"
    | Target(Ref(t)) -> "Target(Ref("^(sty t)^"))"
    | Target(Iterator t) -> "Target(Iterator("^(sty t)^"))"
    | Target(Pair (l,r)) -> "Target(Pair("^(of_tlist [l;r])^"))"
    | Target(MultiIndexDef (id,entry,primary,secondaries)) ->
        "Target(MultiIndexDef("^(quote id)^","^(quote entry)^"...))"

    | Target(TypeDef (src,dest)) -> "Target(TypeDef("^(quote src)^","^(quote dest)^"))"
    | Target(StructDef (id, fields)) -> "Target(StructDef("^(quote id)^",...))"
    | Target(EntryStructDef (id, fields)) -> "Target(EntryStructDef("^(quote id)^",...))"

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

  let string_of_ext_imp = string_of_typed_imp string_of_ext_type string_of_ext_fn

  (* Serialization code generation *)
  let serialization_source_code_of_entry id fields =
    let serialize_members =
      List.map (fun (m,ty) -> "  ar & BOOST_SERIALIZATION_NVP("^m^");") fields
    in Lines (["template<class Archive>";
               "void serialize(Archive& ar, const unsigned int version)";
               "{"]@serialize_members@["}"])

  let source_code_of_map_serialization archive_id map_id =
    inl(archive_id^"<<BOOST_SERIALIZATION_NVP("^map_id^")")

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


  let imp_type_of_calc_type t = match t with
    | Calculus.TInt -> Host K.TInt
    | Calculus.TLong -> failwith "Unsupport K3/Imp type: long"
    | Calculus.TDouble -> Host K.TFloat
    | Calculus.TString -> failwith "Unsupport K3/Imp type: string"

  (* Stringification *)
  let string_of_calc_type t = match t with
    | Calculus.TInt -> "int"
    | Calculus.TLong -> "long"
    | Calculus.TDouble -> "double"
    | Calculus.TString -> "string"

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

  let rec string_of_type t =
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
        let field_types, direct_ctor, direct_assign, pair_assign =
          let a,b,c,d = snd (List.fold_left
            (fun (i, (ty_acc, c1_acc, a1_acc, c2_acc)) (m,ty) ->
              let ty_s = string_of_type ty in
              let i_s = string_of_int i in
              let cf = "c"^i_s in (i+1,
                (ty_acc@[ty_s],
                 c1_acc@[ty_s^" "^cf],
                 a1_acc@[m^" = "^cf],
                 c2_acc@[m^" = "^(if i <> last
                          then tuple_member i_s (pair_id^".first")
                          else pair_id^".second")])))
            (0,([],[],[],[])) fields)
          in a, String.concat "," b,
              ((String.concat "; " c)^";"), ((String.concat "; " d)^";")
        in
        let pair_ctor =
          if field_types = [] then "" else
          let k_t,v_t = back field_types
          in "std::pair<"^(String.concat "," [mk_tuple_ty k_t; v_t])^"> "^pair_id  
        in
        let serialize = serialization_source_code_of_entry id fields in
        csc (Lines([id^"("^direct_ctor^") { "^direct_assign^" }";
                    id^"("^pair_ctor^") { "^pair_assign^" }"]))
            serialize
      in cscl [(inl ("struct "^id^" {")); isc tab (csc (inl str_fields)
               (if str_fields = "" then inl "" else ctor)); inl("};")]
    in

    let source_code_of_struct_def id fields = 
      let str_fields = struct_fields fields
      in cscl [(inl ("struct "^id^" {")); isc tab (inl str_fields); inl("};")]
    in

    let source_code_of_multi_index_def id entry primary indices =
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
        in cscl x, (cscl ~delim:"," y)
      in
      let indexes =
        cscl ([Lines ([primary_idx^(if esc idx_sc then "" else ",")])]@
              (if esc idx_sc then [] else [idx_sc]))
      in cscl [primary_def_sc; idx_def_sc;
               Lines(["typedef multi_index_container<"^entry^", indexed_by<"]);
               isc tab indexes; (inl (" > > "^id^";"))]
    in 

    begin match t with
      Host K.TUnit -> "void"
    | Host K.TFloat -> "double"
    | Host K.TInt -> "int"
    | Host(K.TTuple(tl)) -> 
        if List.length tl = 1 then of_host_list tl
        else mk_tuple_ty (List.map string_of_type (types_of_host_list tl))
    | Host(K.Collection et) ->
        begin match et with
        | K.TTuple(fields) ->
            let rest, v =
              let x = List.rev fields in List.rev (List.tl x), List.hd x in
            let k = Host(K.TTuple(rest)) in
            let rangle_sep = match v with | K.TTuple _ -> " " | _ -> ""
            in "map<"^(string_of_type k)^","^(string_of_type (Host v))^rangle_sep^">"
 
        | _ -> let inner_t = string_of_type (Host et) in
               let sep = if inner_t.[String.length inner_t-1] = '>' then " " else ""
               in "list<"^inner_t^sep^">"
        end
    | Host(K.Fn (args, rt)) ->
        "(("^(string_of_type (Host rt))^")*)("^(of_host_list args)^")"

    | Target (ext_type) ->
      begin match ext_type with
        | Type(id) -> id
        | Ref(t) -> (string_of_type t)^"&"
        | Iterator(t) -> (string_of_type t)^"::iterator"
        | Pair(l,r) ->
          let p = of_list [l; r] in 
          let final_rangle = p.[String.length p-1] = '>' in
          "std::pair<"^p^(if final_rangle then " " else "")^">"
        | TypeDef(a,b) -> "typedef "^a^" "^b^";"
        
        | StructDef(id, fields) ->
          ssc (source_code_of_struct_def id fields)
        
        | EntryStructDef(id, fields) ->
          ssc (source_code_of_entry_struct_def id fields)
          
        | MultiIndexDef(id, entry, primary, indices) ->
          ssc (source_code_of_multi_index_def id entry primary indices)
      end
    end
  end (* string_of_type *)


  (* variable types and typedefs *)
  type type_env_t =   (id_t * ext_type type_t) list
                    * (id_t * ext_type type_t) list    

  (* Typing helpers *)

  let type_id_of_type t = match t with
    | Target(Type(x)) -> x
    | Target(MultiIndexDef (id,_,_,_)) -> id
    | Target(StructDef (id,_)) -> id
    | Target(EntryStructDef (id,_)) -> id
    | _ -> failwith ("invalid named type "^(string_of_type t))

  let type_decl_of_type t = match t with
    | Target(MultiIndexDef(id,entry,_,_)) -> [id,t]
    | Target(TypeDef (src,dest)) -> [src, Target(Type(dest))]
    | Target(StructDef (id,_)) -> [id,t]
    | Target(EntryStructDef (id,_)) -> [id,t]
    | _ -> []

  let is_type_alias env t = match t with
    | Target(Type(x)) -> List.mem_assoc x (snd env)
    | _ -> false 

  (* Retrieve a type definition from the typing environment *) 
  let rec type_decl_of_env env t = match t with
    | Target(Type(x)) ->
        if List.mem_assoc x (snd env) then
          (* Recursively chase all type aliases to some declaration *) 
          type_decl_of_env env (List.assoc x (snd env))
        else (failwith ("undeclared type "^(string_of_type t)))
    | _ -> t

  (* Note: this is not recursive for now... *)
  (* TODO: check valid subfields of structs, and multi-indexes *)
  let is_type_defined env t = match t with
    | Target(Type(x)) -> List.mem_assoc x (snd env) 
    | _ -> true

  let rec is_ext_collection env t = match t with
    | Target(Type(x)) when is_type_defined env t ->
        is_ext_collection env (type_decl_of_env env t)
    | Target(MultiIndexDef(_)) -> true
    | _ -> false

  let rec ext_collection_of_type env t = match t with
    | Target(Type(x)) when is_type_defined env t ->
        ext_collection_of_type env (type_decl_of_env env t)
    | Target(MultiIndexDef(_)) -> Some(t)
    | _ -> None

  let rec field_types_of_type t = match t with
    | Host(TTuple l) -> List.map (fun x -> Host x) l
    | Target(Pair(Host(TTuple(l)),r)) -> (field_types_of_type (Host(TTuple(l))))@[r]  
    | Target(StructDef(_, fields)) -> List.map snd fields
    | Target(EntryStructDef(_, fields)) -> List.map snd fields
    | _ -> failwith ("invalid composite type "^(string_of_type t))

  let entry_type_of_collection env t =
    let error() = failwith ("invalid collection "^(string_of_type t)) in 
    let c_t = match ext_collection_of_type env t with
      | None -> error() | Some(t) -> t
    in match c_t with
    | Target(MultiIndexDef(t_id,entry_t_id,_,_)) -> Target(Type(entry_t_id))
    | _ -> error()

  let field_types_of_collection env t = match t with
    | Target(MultiIndexDef(t_id,entry_t_id,_,_)) ->
        let entry_t = Target(Type(entry_t_id)) in
        if is_type_defined env entry_t then
          let def = type_decl_of_env env entry_t in field_types_of_type def 
        else failwith ("invalid collection entry "^(string_of_type entry_t))
    | _ -> failwith ("invalid collection "^(string_of_type t))

  let index_of_collection env t pat = match t with
    | Target(MultiIndexDef(t_id,_,_,indices)) ->
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
        in if rp <> "" && ri <> "" then Target(Type(rp)), Target(Type(ri))
           else failwith ("no index with id \""^sfx^"\" found for type "^
                          (string_of_type t))
    | _ -> failwith ("collection index request on type "^(string_of_type t))

  let rec host_type_of_type env t =
    begin match t with
      (* TODO: fix int->float type all the way from K3 builder to here *)
      | Host TInt -> Host TFloat
      | Host _ -> t
      | _ when is_ext_collection env t ->
        Host(Collection(host_type
          (host_type_of_type env (entry_type_of_collection env t)))) 
      | _ ->
        let entry_fields = field_types_of_type (type_decl_of_env env t)
        in Host(TTuple(List.map (fun f_t ->
            host_type (host_type_of_type env f_t)) entry_fields))
    end

  (* Returns a multi_index_container type declaration for a global map, incl.
   * -- structs for map in tier and out tier entries.
   * -- multi_index_container types for in and out tiers.
   * -- pattern tag types and index typedefs.
   *)
  let type_of_map_schema patterns (id, in_tl, out_tl) =
    let k3vartype_of_m3vartype t = match t with
      | VT_String -> failwith "strings unsupported in K3"
      | VT_Int -> TInt
      | VT_Float -> TFloat
    in
    let attri i = "__a"^(string_of_int i) in
    let fields_of_var_types tl = List.fold_left (fun (i,acc) ty ->
          (i+1, acc@[attri i, Host (k3vartype_of_m3vartype ty)]))
        (0,[]) tl
    in
    let aux () =
      let t_id, t = let x = id^"_map" in x, Target(Type(x)) in
      let entry_t_id = id^"_entry" in
      let out_t, out_t_id = match in_tl, out_tl with
        | x,y when List.length x > 0 && List.length y > 0 -> 
          let z = id^"_out_map" in Target(Type(z)), z
        | _,_ -> Target(Type("!!!error!!!")), "!!!error!!!"
      in 
    
      (* Single tier maps only have entry_t defined *)
      let entry_t, extra_entry_t =
        let in_fields = snd (fields_of_var_types in_tl) in
        let out_fields = (snd (fields_of_var_types out_tl))@["__av", Host TFloat]
        in match in_fields, out_fields with
          | [], _ -> Target(EntryStructDef(entry_t_id, out_fields)), None
          | _, [x] -> Target(EntryStructDef(entry_t_id, in_fields@out_fields)), None
          | _, _ ->
            let out_entry_t_id = id^"_out_entry" in
            let out_entry_t = Target(EntryStructDef(out_entry_t_id, out_fields))
            in Target(EntryStructDef(entry_t_id, in_fields@["__av", out_t])),
               Some(out_entry_t)
      in
    
      (* In/out pattern index and type lists *)
      let in_pat_its, out_pat_its = 
        let id_pats =
          if not(List.mem_assoc id patterns) then []
          else (List.assoc id patterns) in
        let it_of_idx l idxl = List.combine idxl (List.map (List.nth l) idxl) in
        let r = List.fold_left (fun (in_acc, out_acc) p -> match p with
            | M3Common.Patterns.In(v,i) -> in_acc@[it_of_idx in_tl i], out_acc
            | M3Common.Patterns.Out(v,i) -> in_acc, out_acc@[it_of_idx out_tl i])
          ([], []) id_pats
        in unique (List.filter (fun x -> x <> []) (fst r)),
           unique (List.filter (fun x -> x <> []) (snd r))
      in
      let entry_idx_meta, extra_idx_meta = 
        let id_of_idxl l = String.concat "" (List.map string_of_int l) in
        let mk_idx_field (i,ty) = (attri i), Host(k3vartype_of_m3vartype ty) in 
        let mk_pat t_id pfx p =
          let pat_sfx = id_of_idxl (List.map fst p) in
          let pat_id = id^pfx^"_pat"^pat_sfx in
          let idx_fields = List.map mk_idx_field p in
          let idx_t_id, idx_t_str =
            (id^pfx^"_index"^pat_sfx), (t_id^"::index<"^pat_id^">::type")
          in
            Target(StructDef(pat_id, [])), 
            Target(TypeDef(idx_t_str, idx_t_id)),
            (pat_id, idx_fields)
        in match extra_entry_t with
          | None -> (List.map (mk_pat t_id "") (in_pat_its@out_pat_its)), []
          | Some(Target(EntryStructDef(_,_))) ->
            List.map (mk_pat t_id "_in") in_pat_its,
            List.map (mk_pat out_t_id "_out") out_pat_its
          | _ -> failwith "invalid out tier entry"
      in
      let mk_mindex t_id entry_t_decl idx_meta =
        let entry_t_id, key_fields = match entry_t_decl with
          | Target(EntryStructDef(id,fields)) ->
              id, (List.rev (List.tl (List.rev fields)))
          | _ -> failwith "invalid multi-index entry"
        in 
        let pat_decl = List.map (fun (x,_,_) -> x) idx_meta in
        let idx_decl = List.map (fun (_,y,_) -> y) idx_meta in
        let idx_pat = List.map (fun (_,_,z) -> z) idx_meta in
        let t_decl = Target(MultiIndexDef(t_id, entry_t_id, key_fields, idx_pat))
        in Target(Type(t_id)), ([entry_t_decl]@pat_decl@[t_decl]@idx_decl)
      in
      let out_mindex_decls = match extra_entry_t with
          | None -> []
          | Some(e_t) -> snd (mk_mindex out_t_id e_t extra_idx_meta)  
      in
      let mindex_t, mindex_decls = mk_mindex t_id entry_t entry_idx_meta
      in id, mindex_t, (out_mindex_decls@mindex_decls)
    in if (in_tl@out_tl) = [] then (id, Host TFloat, []) else aux()


  (* Returns a type env containing global variable, and type declarations *)
  let type_env_of_declarations arg_types schema patterns =
     List.fold_left (fun (vacc,tacc) (id,t,t_decls) ->
        vacc@[id,t], tacc@(List.flatten (List.map type_decl_of_type t_decls))) 
      (arg_types,[]) (List.map (type_of_map_schema patterns) schema)

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
      | Host(TFloat), Host(TInt) | Host(TInt), Host(TFloat) -> Host(TFloat)
      | _, _ when t1 = t2 -> t1
      | _, _ -> failwith ("incompatible types "^(sot t1)^" "^(sot t2))
    in
    let promote_types_op op t1 t2 = match t1, t2 with
      | Host(TFloat), Host(TInt) | Host(TInt), Host(TFloat) -> Host(TFloat)
      | _, _ when t1 = t2 -> t1
      | _, _ -> failwith ("incompatible types for op "^
                           (string_of_op op)^" "^(sot t1)^" "^(sot t2))
    in
    let add_env_var type_env id ty = 
      let (vars,tys) = !type_env in
      let new_vars =
        (* Override existing type bindings with declarations *) 
        ((id,ty)::(if List.mem_assoc id vars
                   then List.remove_assoc id vars else vars))
       in type_env := (new_vars,tys);
          type_env
    in

    let infer_fn_type env fn_id arg_types = match fn_id with
      | TupleElement j ->
        let t = List.hd arg_types in begin match t with
        | Host(TTuple l) -> Host (List.nth l j)
        | _ -> failwith ("invalid tuple type "^(string_of_type t))
        end

      | Projection idx ->
        Host (TTuple (List.map host_type (List.map (List.nth arg_types) idx)))
      | Singleton -> Host (Collection (host_type (List.hd arg_types))) 
      | Combine ->
        let l,r = List.hd arg_types, List.nth arg_types 1 in
        if l = r then l else failwith "incompatible combine arguments"

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
        | Host (Collection (TTuple(_))) -> Host TInt
        | Target(_) when is_ext_collection env t -> Host TInt
        | _ -> error()
        end

      | Lookup -> 
        let t = List.hd arg_types in 
        let error () = failwith ("invalid collection type "^(string_of_type t))
        in begin match t with
        | Host (Collection (TTuple l)) -> let _,t = back l in Host t
        | Target(_) when is_ext_collection env t ->
          let t_fields = match ext_collection_of_type env t with
            | Some(c_t) -> field_types_of_collection env c_t
            | _ -> error()
          in snd (back t_fields)
        | _ -> error()
        end

      | Slice (idx) ->
        let t = List.hd arg_types in
        let error () =
          failwith ("invalid collection type for slicing "^(string_of_type t)) 
        in begin match t with
        | Host (Collection (TTuple l)) ->
            if (List.length l) <= (List.fold_left max 0 idx) then
              failwith "invalid slice indices"
            else t
        | Target(_) when is_ext_collection env t -> host_type_of_type env t
        | _ -> error()
        end

      | ConcatElement -> unit
      | Concat -> unit
      | MapUpdate -> unit
      | MapValueUpdate -> unit
      | Ext _ -> failwith "external function appeared in untyped imp"
    in
    let infer_expr type_env c_opts untyped_e =
      let cexpri i = extract_expr (List.nth c_opts i) in
      let ctypei i = type_of_expr_t (cexpri i) in
      let cexprs() = List.map extract_expr c_opts in
      let ctypes() = List.map type_of_expr_t (cexprs()) in
      match untyped_e with
      | Const (_,c) ->
        let t = match c with 
          | CFloat _ -> Host TFloat
          | CString _ -> Host TInt
        in None, Some(Const(t,c))

      | Var (_,(v,t)) ->
        let defined_type = match t with
          | Target(Type(x)) -> List.mem_assoc x (snd !type_env)
          | _ -> true
        in
        let declared_type =
          try List.assoc v (fst !type_env)
          with Not_found -> 
            print_endline ("no declaration found for "^v^" in: "^
                           (Util.list_to_string fst (fst !type_env)));
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
          | _ -> promote_types_op op (ctypei 0) (ctypei 1)    
        in None, Some(BinOp(t, op, cexpri 0, cexpri 1))

      | Fn(_,fn_id,args) ->
        let t = infer_fn_type !type_env fn_id (ctypes())
        in None, Some(Fn(t, fn_id, (cexprs())))
    in
    let infer_imp type_env c_opts untyped_i =
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
             if t <> ty then ignore(add_env_var type_env id t);
             Some(Decl(unit, (id,t), Some(cexpri 0))), None

      | For (_,((id,ty),f),_,l) ->
        let s_t = type_of_expr_t (cexpri 0) in
        let l_t = type_of_imp_t (cimpi 1) in
        let new_l, el_t =
          let el_t = match s_t with
            | Host(Collection(x)) -> Host(x)
            | Target(_) when is_ext_collection !type_env s_t ->
              entry_type_of_collection !type_env s_t
            | _ -> failwith "invalid collection in loop"
          in 
          if el_t = ty then cimpi 1, ty else
             begin
              ignore(add_env_var type_env id el_t);
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
    let build_type_env_expr type_env untyped_e = type_env in
    let build_type_env_imp type_env untyped_i =
        match untyped_i with
        | Block (_,l) -> ref (!type_env)

        | Decl (_,(id,ty),_) -> add_env_var type_env id ty
            
        | For (_,elem,source,Decl _) ->
          failwith "invalid declaration as loop body"
        
        | For (_,((id,ty),_),_,_) -> add_env_var type_env id ty

        | IfThenElse(_,_,Decl _,_) | IfThenElse(_,_,_,Decl _) ->
          failwith "invalid declaration as condition branch"
        
        | _ -> type_env 
    in
    let r_opt_pair =
      fold_imp infer_imp infer_expr build_type_env_imp build_type_env_expr
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

      (* TODO *)
    | Singleton -> inl("singleton("^a^")")
    | Combine -> inl("combine("^a^")")
  
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
          | Host (Collection(TTuple(_))) ->
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
        begin match argti 0, argti 1 with
          | Host (Collection _), Host (Collection _) ->
            inl((argi 0)^".insert("^(argi 1)^".begin(), "^(argi 1)^".end())")

          | _ -> failwith ("invalid concatenation to return type "^
                            (string_of_type return_type))
        end

    | MapUpdate ->
        let k,v = back (List.tl args) in
        begin match argti 0 with
          | Host(Collection _) -> inl((argi 0)^"["^(mk_tuple k)^"] = "^v)
          | Host(_) -> failwith "invalid merge of non-collection"
          | Target(Type id_t) ->
            failwith "unhandled sugared persistent map update"
          | _ -> failwith "unsupported map update"  
        end

    | MapValueUpdate ->
        let k,v = back (List.tl args) in
        begin match argti 0 with
          | Host(Collection _) -> inl((argi 0)^"["^(mk_tuple k)^"] = "^v)
          | Host(_) -> inl((argi 0)^" = "^(argi 1))
          | Target(Type x) ->
            failwith "unhandled sugared persistent map value update"
          | _ -> failwith "unsupported map value update"  
        end


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
          | ModifyCollection -> (argi 0)^".modify("^(of_list (List.tl args))^")"
          | InsertCollection -> (argi 0)^".insert("^(of_list (List.tl args))^")"
          | RangeCollection -> (argi 0)^".equal_range("^(mk_tuple (List.tl args))^")"
          | EraseCollection -> (argi 0)^".erase("^(of_list (List.tl args))^")"
          | ClearCollection -> (argi 0)^".clear()"  
          | SecondaryIndex(pat) -> (argi 0)^".get<"^(string_of_type pat)^">()" 
          | BoostLambda(fn) -> fn
          | Inline(s) -> s 
        end)

  and source_code_of_expr (expr : (ext_type, ext_fn) typed_expr_t) =
    match expr with
    | Const (_,c) -> inl(
       begin match c with
         | CFloat x -> 
           let r = string_of_float x in
           let neg = r.[0] = '-' in
           (if neg then "(" else "")^
           (if r.[(String.length r)-1] = '.' then r^"0" else r)^
           (if neg then ")" else "")
         | CString s ->
           "static_cast<int>(string_hash(\""^(String.escaped s)^"\"))"
       end)
 
    | Var (_,(v,_)) -> inl(v)
    | Tuple (Host(TTuple(types)), fields) ->
      inl(mk_tuple (List.map (fun e -> ssc (source_code_of_expr e)) fields))

    | Op(_,op,e) -> inl ((string_of_op op)^"("^(ssc (source_code_of_expr e))^")")
    | BinOp(_,op,l,r) ->
        let binop op = parensc "(" ")"
          (parensc "(" ")"
            (binopsc op (source_code_of_expr l) (source_code_of_expr r)))
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

  let rec source_code_of_imp (imp : (ext_type, ext_fn) typed_imp_t) =
    let mk_block i = match i with
        | Block _ -> i
        | _ -> Block(type_of_imp_t i, [i]) in
    begin match imp with
    | Expr (_,e) -> dsc stmt_delimiter (source_code_of_expr e)
    | Block (_,il) ->
      let r = List.map source_code_of_imp il in
      begin match r with
        | [Lines([l])] -> inl ("{ "^l^" } ")
        | _ -> cscl ([inl "{"]@(List.map (isc tab) r)@[inl "}"])
      end

      (* Directly invoke constructor on non-heap allocated local definitions *)
    | Decl(_,(s,_), Some(Fn(_, Ext(Constructor(t)), args))) ->
        let cargs = String.concat ","
          (List.map (fun a -> ssc (source_code_of_expr a)) args) in
        let r = (string_of_type t)^" "^s^"("^cargs^")"
        in dsc stmt_delimiter (inl r)

    | Decl(_,(s,t),init) ->
        let sinit =
          match init with Some(e) -> ssc (source_code_of_expr e) | _ -> "" in
        let r = (string_of_type t)^" "^s^
          (if sinit = "" then "" else (string_of_op Assign)^(sinit))
        in dsc stmt_delimiter (inl r)

    | For(_,((elem, elem_ty), elem_f),source,body) ->
        begin match type_of_expr_t source with
        | Host(TInt) -> (* TODO: change to bool type *)
          let cond = ssc (source_code_of_expr source) in
          cscl (
            [Lines(["for (; "^cond^"; ++"^elem^")"])]@
            [source_code_of_imp body])

        | _ -> 
          print_endline ("invalid desugared loop, expected iterators: "^
                            (string_of_ext_imp imp));
          failwith ("invalid desugared loop, expected iterators")
        end

    | IfThenElse(_,p,t,e) ->
        let tb, eb = mk_block t, mk_block e in
        cscl ([inl ("if ("^(ssc (source_code_of_expr p))^") ")]@
              [source_code_of_imp tb]@
              [inl ("else ")]@[source_code_of_imp eb])
    end

  let source_code_of_trigger dbschema (event,rel,args,stmts) =
    let pm_name pm =
      match pm with M3.Insert -> "insert" | M3.Delete -> "delete" in
    let trig_name = (pm_name event)^"_"^rel in
    let trig_types = List.map (fun v -> string_of_calc_type (snd v))
        (List.assoc rel dbschema) in
    let trig_args = String.concat ", "
      (List.map (fun (a,ty) -> ty^" "^a) (List.combine args trig_types))
    in  
    let trigger_fn =
      [inl ("void on_"^trig_name^"("^trig_args^")")]@
      [source_code_of_imp stmts]@[Lines ([""])]
    in
    let unwrapper =
      let evt_arg = "e" in
      let evt_fields = snd (List.fold_left
        (fun (i,acc) ty -> (i+1,
          acc@["any_cast<"^ty^">("^evt_arg^"["^(string_of_int i)^"])"]))
        (0,[]) trig_types)
      in 
      [Lines ["void unwrap_"^trig_name^"(const event_data& e, "^
                "shared_ptr<dbt_trigger_log> logger) {";
              "  if ( logger && logger->sink_stream ) {";
              "    (*(logger->sink_stream)) << setprecision(15) << "^
                     (String.concat " << \",\" << " evt_fields)^" << endl;";
              "  }";
              "  on_"^trig_name^"("^(String.concat "," evt_fields)^");";
              "}"; ""]]
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
  let desugar_map_update env nargs c_t =
    let mk_it_t t = Target(Iterator(t)) in
    let mk_var id t = Var(t, (id, t)) in  
    let c_var = List.hd nargs in
    let id, entry_t_id, entry_t =
      let x = entry_type_of_collection env c_t in
      type_id_of_type c_t, type_id_of_type x, x in
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
        | Host(Collection(x)) -> Host x
        | Target(_) as x when is_ext_collection env x ->
          entry_type_of_collection env x
        | _ -> failwith ("invalid update collection in map update "^(string_of_type slice_t))
      in
      let slice_it_id, slice_end_id, slice_it_t =
        gensym(), gensym(), mk_it_t slice_t in
      let slice_it_var = mk_var slice_it_id slice_it_t in
      let slice_end_var = mk_var slice_end_id slice_it_t in
      
      let target_id, target_t, target_ref_t =
        let _,t = back (field_types_of_type (type_decl_of_env env entry_t))
        in gensym(), t, Target(Ref(t)) in
      let target_var = mk_var target_id target_t in
      let target_decl = Decl(unit, (target_id, target_t), None) in
      let target_entry_t = entry_type_of_collection env target_t in
      let target_entry_id, target_entry_decl, target_entry_var =
        let x = gensym() in x, (x,target_entry_t), mk_var x target_entry_t in
      let ctor =
        let init_args = [Fn(slice_elem_t, Ext(IteratorElement), [slice_it_var])]
        in Decl(unit, target_entry_decl,
             Some(Fn(target_entry_t, Ext(Constructor(target_entry_t)), init_args)))
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
            BinOp(Host TInt, Neq, slice_it_var, slice_end_var),
            Block(unit,
              [ctor;
               Expr(unit, BinOp(unit, Assign, hint_var,
                 Fn(target_it_t, Ext(InsertCollection),
                   [target_var; hint_var; target_entry_var])))]))
      in
      let modify_lambda = "bind(&"^entry_t_id^"::__av, _1) = "^target_id in
      let map_update =
        Expr(unit, Fn(unit, Ext(ModifyCollection),
          [c_var; c_it_var; Fn(Host TUnit, Ext(BoostLambda(modify_lambda)), [])])) 
      in [it_decl; target_decl]@loop_decls@[update_loop; map_update]


  (* map value update rewrites:
   *    it = #map#.find(#key#);
   *    if ( it != #map#.end() ) {
   *      #map#.modify(it, bind(#map entry#::__av, _1)=#value#);
   *    }
   *    else { #map entry# e(#key#, #value#); #map#.insert(e); }
   *)
  let desugar_map_value_update env nargs c_t =
    let k,v = back (List.tl nargs) in
    let entry_id, entry_t_id, entry_t =
      let x = entry_type_of_collection env c_t in gensym(), type_id_of_type x, x
    in
    let c_it_t = Target(Iterator(c_t)) in
    let it_id = gensym() in
    let c_var, it_var = List.hd nargs, Var(c_it_t, (it_id, c_it_t)) in
    (* TODO: change op_t to bool when K3 supports boolean types. *)
    (* TODO: technically mod_t is a function type, but K3 types are not
     * extensible, thus cannot use external type arguments *)
    let op_t, mod_t = Host TInt, unit in
    let modify_lambda =
      "bind(&"^entry_t_id^"::__av, _1) = "^(ssc (source_code_of_expr v))
    in
    [Decl(unit, (it_id, c_it_t), Some(Fn(c_it_t, Ext(FindCollection), [c_var]@k)));
     IfThenElse(unit,
       BinOp(op_t, Neq, it_var, Fn(c_it_t, Ext(EndCollection), [c_var])),
       Expr(unit, Fn(unit, Ext(ModifyCollection),
               [c_var; it_var; Fn(mod_t, Ext(BoostLambda(modify_lambda)), [])])),
       Block(unit, [
         Decl(unit, (entry_id, entry_t),
           Some(Fn(entry_t, Ext(Constructor(entry_t)), (List.tl nargs))));
         Expr(unit, (* ignore iterator return *)
           Fn(c_it_t, Ext(InsertCollection),
               [c_var; Var(entry_t, (entry_id, entry_t))]))]))]


  (* Updates out tier if in tier exists using above single-level code,
   * otherwise builds new out tier entry and adds to the in tier *)
  let desugar_nested_map_value_update env nargs c_t =
    let c_expr = List.hd nargs in
    let c_entry_t, c_it_t =
      entry_type_of_collection env c_t, Target(Iterator(c_t)) in
    let out_map_t, out_entry_t, in_exprs, out_exprs = 
      let fields_t = match ext_collection_of_type env c_t with
        | Some(x) -> field_types_of_collection env x
        | _ -> failwith ("invalid collection type "^(string_of_type c_t)) in
      let mt = snd (back fields_t) in 
      let num_in_fields = List.length fields_t - 1 in
      let ine,oute = snd (List.fold_left (fun (i,(ina,outa)) e ->
        (i+1, if i < num_in_fields then (ina@[e],outa) else (ina,outa@[e])))
        (0,([],[])) (List.tl nargs))
      in mt, (entry_type_of_collection env mt), ine, oute
    in
    let in_args = c_expr::in_exprs in
    let c_it_expr = Fn(c_it_t, Ext(FindCollection), in_args) in
    let c_it_decl, c_it_var = 
      let x = gensym() in
      let y = x,c_it_t in Decl(unit, y, Some(c_it_expr)), Var(c_it_t, y)
    in
    let op_t = Host TInt in
    let out_update_decl, out_update_map_var =
        let x,t = gensym(), Target(Ref(out_map_t)) in
        Decl(unit, (x,t), Some(
          Fn(t, Ext(ConstCast(t)),
            [Fn(out_map_t, Ext(MemberAccess("__av")),
               [Fn(c_entry_t, Ext(IteratorElement), [c_it_var])])]))),
        Var(t, (x,t))
    in
    let out_args = out_update_map_var::out_exprs in
    let out_update_imp = desugar_map_value_update env out_args out_map_t in
    let out_map_var, out_entry_ctor_and_insert =
      let x,y = gensym(), gensym() in
      let out_entry_var = Var(out_entry_t, (x,out_entry_t)) in
      let out_map_var = Var(out_map_t, (y,out_map_t)) in
      out_map_var,
      [Decl(unit, (x,out_entry_t),
         Some(Fn(out_entry_t, Ext(Constructor(out_entry_t)), out_exprs)));
       Decl(unit, (y,out_map_t), None);
       Expr(unit, Fn(unit, Ext(InsertCollection), [out_map_var; out_entry_var]))]
    in
    let in_entry_ctor_and_insert =
      let x = gensym() in
      let c_entry_var = Var(c_entry_t, (x,c_entry_t)) in
      [Decl(unit, (x, c_entry_t),
         Some(Fn(c_entry_t, Ext(Constructor(c_entry_t)), (in_exprs@[out_map_var]))));
       Expr(unit, Fn(unit, Ext(InsertCollection), [c_expr; c_entry_var]))]
    in
      [c_it_decl;
       IfThenElse(unit,
         BinOp(op_t, Neq, c_it_expr, Fn(c_it_t, Ext(EndCollection), [c_expr])),
         Block(unit, [out_update_decl]@out_update_imp),
         Block(unit, out_entry_ctor_and_insert@in_entry_ctor_and_insert))]


  (* Expression desugaring *)
  let rec desugar_expr opts env e =
    let recur l = flatten_desugaring (List.map (desugar_expr opts env) l) in
    let result x y = (if x = [] then None else Some(x)), Some(y) in
    let member_common args =
      let ci, nargs = recur args in
      if List.length nargs <> List.length args then
        failwith "invalid member desugaring"
      else
      let c_var = List.hd nargs in
      let c_it_t = Target(Iterator(type_of_expr_t c_var)) in
      (* TODO: replace with bool when K3 has bool type *)
      let op_t = Host TInt in
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
        | Host(Collection(x)) -> Host x, Ext(PairSecond)
        | Target(Type(x)) -> Target(Type(x^"_entry")), Ext(MemberAccess("__av"))
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
            let insert_loop = desugar_map_update env nargs c_t
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
        | Target(_) as x when is_ext_collection env x -> 
          let fields_t = match ext_collection_of_type env x with
            | Some(c_t) -> field_types_of_collection env c_t
            | _ -> error()
          in
          let desugar_f = match snd (back fields_t) with
            | Host TInt | Host TFloat -> desugar_map_value_update
            | Target(_) -> desugar_nested_map_value_update
            | _ -> error()
          in 
          (* Only profile external types, i.e. multi-indexes, not native types
           * such as STL maps, and scalars. *)
          if opts.profile then
            let id = ssc (source_code_of_expr (List.hd nargs))
            in result ci (Fn(ty, Ext(Apply(id^"_value_update")), nargs))
          else
            let r = desugar_f env nargs x in Some(ci@r), None

        | _ -> result ci (Fn(ty, MapValueUpdate, nargs))
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

  (* Declares iterators for a subrange of a collection based on the given args *)
  let range_iters_of_collection iter_id iter_end c c_ty args =
    if args = [] then iters_of_collection iter_id iter_end c c_ty
    else
    let it_t = Target(Iterator(c_ty)) in
    let pair_t = Target(Pair(it_t, it_t)) in
    let pair_id = gensym() in
    let pair_v = Var(pair_t, (pair_id, pair_t)) in
      [Decl(unit, (pair_id,pair_t), Some(Fn(c_ty, Ext(RangeCollection), [c]@args)));
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
  let rec sub_imp_expr env src_dest_pairs imp =
    let rcr = sub_imp_expr env src_dest_pairs in
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
        if not((n_ty = Host TInt) || (n_ty = Host TFloat)) then
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
        snd (List.fold_left (fun (i,acc) (src_t, dest_t) ->
          let attr = "__a"^(if i = num_fields then "v" else string_of_int i) in
          let src = Fn(Host src_t, TupleElement(i), [src_expr]) in
          let dest = dest_t, Fn(dest_t, Ext(MemberAccess(attr)), [src_expr])
          in i+1,acc@[src, dest])
        (0,[]) (List.combine tt_l smt_l))


  let rec fixpoint_sub_imp env subs imp =
    let nsubs, nimp = sub_imp_expr env subs imp in
    if nsubs = [] then nimp
    else
      let next_subs = List.map (fun (ty,(id,nty)) -> match ty, nty with
        | Host(Collection (TTuple(l))), Target(_) when is_ext_collection env nty ->
          let fields_t = field_types_of_collection env (type_decl_of_env env nty)
          in (struct_member_sub (Var(ty,(id,ty))) l fields_t)@
             [Var(ty, (id, ty)), (nty, Var(nty, (id, nty)))]
        
        | Host(TTuple l), Target(_) ->
          let fields_t = field_types_of_type (type_decl_of_env env nty)
          in (struct_member_sub (Var(ty,(id,ty))) l fields_t)@
             [Var(ty, (id, ty)), (nty, Var(nty, (id, nty)))]
        
        | _, _ -> []) nsubs
      in
      fixpoint_sub_imp env (List.flatten next_subs) nimp 


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
        let r = snd (List.fold_left (fun (found,acc) i ->
          if found then found,acc@[i] else
          let nf,ni = match i with
            | Decl(unit, (id,ty), Some(init)) ->
              let n_init =
                sub_expr_vars (Var(elem_ty,(elem, elem_ty))) subbed_elem init in
              let nd = Decl(unit, (id,ty), Some(n_init)) in
              (id=elem), nd
            | _ -> false, i
          in (nf,acc@[ni])) (false, []) l)
        in Block(m,(elem_decls@r))
      | _ -> Block(type_of_imp_t body, elem_decls@[body]) 
    in
    (* TODO: change to bool type *)
    let loop_cond_t = Host TInt in
    let loop_cond = BinOp(loop_cond_t, Neq,
        Var(iter_t, (iter_id, iter_t)), Var(iter_t, (iter_end, iter_t))) in
    let loop_t = type_of_imp_t subbed_body
    in iter_decls@[For(loop_t, ((iter_id, iter_t), false), loop_cond, subbed_body)]


  (* Imperative statement desugaring *)
  let rec desugar_imp_aux opts env imp =
    let rcr = desugar_imp opts env in
    let rcr_aux = desugar_imp_aux opts env in
    let flatten_external e = match desugar_expr opts env e with
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
        begin match desugar_expr opts env init with
          | Some(i), Some(e) -> i@[Decl(unit, d, Some(e))]

          (* Avoid copying external types with equality operator, taking
           * a reference instead *)
          | None, Some(Var(Target(Type(x)),v) as e) ->
            let t, ref_t = let a = Target(Type(x)) in a, Target(Ref(a)) in
            [Decl(unit, (fst d, ref_t), Some(e))]

          (* TODO: revisit capturing reference/address for nested external types.
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
            match desugar_expr opts env a with
            | Some(i), Some(e) -> i, [e] 
            | None, Some(e) -> [], [e]
            | _, _ -> failwith "invalid collection externalization") args)
        in 
        let pre, nargs = List.flatten nprel, List.flatten nargsl in 
        
        let iter, iter_id, iter_end = let x = gensym() in x, x^"_it", x^"_end" in
        let iter_decls, elem_ty, source_ty =
          let c_expr = List.hd nargs in
          (* assume types are correct in the input, and only modify on
           * realizing desugaring *)
          begin match type_of_expr_t c_expr with

          (* Pick the appropriate pattern index from a map type *)
          | Target(_) as c_t when is_ext_collection env c_t ->
            let idx_t, elem_t, idx =
              if List.tl nargs = [] then
                c_t, (entry_type_of_collection env c_t), c_expr
              else
              begin match ext_collection_of_type env c_t with
              | Some(x) ->
                let pat_t, idx_t = index_of_collection env x idx
                in idx_t, (entry_type_of_collection env c_t),
                          Fn(idx_t, Ext(SecondaryIndex(pat_t)), [c_expr])
              | _ -> failwith ("invalid collection type "^(string_of_type c_t))
              end
            in
            let decls = range_iters_of_collection
                          iter_id iter_end idx idx_t (List.tl nargs) 
            in decls, elem_t, idx_t

          | _ -> failwith "range iteration of tuples on non-multi-index container type"
          end
        in
        let iter_meta = (iter_id, iter_end, iter_decls, source_ty) in
        let new_body =
          let loop_elem_t, loop_elem_tl = match id_t with
            | Host(TTuple(id_tl)) -> id_t, id_tl
            | Target _ ->
              begin match host_type_of_type env id_t with
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
              let last_field = List.length loop_elem_tl - 1 in
              let fields_t = field_types_of_type (type_decl_of_env env elem_ty) in
              snd (List.fold_left (fun (i,acc) (src_t, dest_t) ->
                  let attr = "__a"^(if i = last_field then "v" else string_of_int i) in
                  let src = Fn(Host src_t, TupleElement(i), [src_var]) in
                  let dest = dest_t, Fn(dest_t, Ext(MemberAccess(attr)), [dest_var])
                  in i+1,acc@[src, dest]) (0,[])
                (List.combine loop_elem_tl fields_t))
            in let var_subs = [src_var, (elem_ty, dest_var)]
            in access_subs@var_subs
          in fixpoint_sub_imp env subs body 
        in
        pre@(sub_iters ((id,elem_ty),f) iter_meta (rcr new_body))

    (* Handle loops over temporaries *)
    | For (_, elem, source, body) ->
        let pre, new_source = match desugar_expr opts env source with
          | Some(i), Some(e) -> i, e 
          | None, Some(e) -> [], e
          | _, _ -> failwith "invalid collection externalization"
        in
        let iter, iter_id, iter_end = let x = gensym() in x, x^"_it", x^"_end" in
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
          | ((id,(Host(TTuple id_tl) as id_t)), false), Host(Collection (TTuple _)) ->
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
                  let src = Fn(Host t, TupleElement(i), [Var(id_t, (id, id_t))]) in
                  let dest = 
                    if i = last_field then nvalue 
                    else if flat_key then nkey
                    else Fn(Host t, TupleElement(i), [nkey])
                  in i+1,acc@[src, (Host t, dest)]) (0,[]) id_tl)
              in fixpoint_sub_imp env (subs@[Var(id_t, (id,id_t)), (ne_t, ne_var)]) body
            in (nelem_d,false), subbed 
          
          (* Non-tuple collections can simply bind the element directly from
           * an iterator, or substitute it with the iterator *)
          | _, Host(Collection _) -> elem, body

          (* Loop should be over a temporary collection. *)
          | _, t ->
            print_endline ("invalid for loop collection type "^(string_of_type t)^
                           ": "^(string_of_ext_imp imp));
            failwith ("invalid for loop collection type "^(string_of_type t)^
                      ": "^(ssc (source_code_of_expr new_source))) 
        in
        pre@(sub_iters new_elem iter_meta (rcr subbed_body))  

    | IfThenElse (condm,p,t,e) ->
        let r np = IfThenElse(condm, np, rcr t, rcr e) in
        begin match desugar_expr opts env p with
          | Some(i), Some(e) -> i@[r e]
          | None, Some(e) -> [r e]
          | _, _ -> failwith "invalid condition externalization"
        end 
  and desugar_imp opts env imp = match desugar_imp_aux opts env imp with
    | [x] -> x
    | x -> let _,last = back x in Block(type_of_imp_t last, x)


  (* External typing interface *)
  type type_env_t = Typing.type_env_t
  let var_type_env type_env = fst type_env
  let type_env_of_declarations = Typing.type_env_of_declarations

  let infer_types = Typing.infer_types

  (* Profiling code generation *)
  let string_of_m3_type t = match t with
    | VT_String -> "string" | VT_Int -> "int" | VT_Float -> "double"

  let profile_map_value_update c_id t_l =
    let c_t, c_entry_t = (c_id^"_map"), (c_id^"_entry") in 
    let (k_decl,k) = snd (List.fold_left
        (fun (id,(dacc,vacc)) t ->
          (id+1,(dacc@[(string_of_m3_type t)^" k"^(string_of_int id)],
                 vacc@["k"^(string_of_int id)])))
      (0,([],[])) t_l)
    in
    Lines(["void "^c_id^"_value_update("^c_t^"& m, "^
            (String.concat ", " k_decl)^", double v)"^
            " __attribute__((noinline));";
           "void "^c_id^"_value_update("^c_t^"& m, "^
            (String.concat ", " k_decl)^", double v)";
           "{";
           "  "^c_t^"::iterator it = m.find("^(mk_tuple k)^");";
           "  if (it != m.end()) {";
           "    m.modify(it, bind(&"^c_entry_t^"::__av, _1) = v);";
           "  } else {";
           "    "^c_entry_t^" e("^(String.concat "," k)^",v);";
           "    m.insert(e);";
           "  }";
           "}"])

  let profile_nested_map_value_update c_id in_tl out_tl =
    let c_out_id = c_id^"_out" in
    let c_t, c_entry_t, c_out_t, c_out_entry_t =
      c_id^"_map", c_id^"_entry", c_id^"_out_map", c_id^"_out_entry" in
    let get_decl_and_val i t_l = snd (List.fold_left
        (fun (id,(dacc,vacc)) t ->
          (id+1,(dacc@[(string_of_m3_type t)^" k"^(string_of_int id)],
                 vacc@["k"^(string_of_int id)])))
      (i,([],[])) t_l)
    in
    let in_k_decl, in_k = get_decl_and_val 0 in_tl in
    let out_k_decl, out_k = get_decl_and_val (List.length in_tl) out_tl in
    Lines(["void "^c_id^"_value_update("^c_t^"& m, "^
             (String.concat ", " (in_k_decl@out_k_decl))^", double v)"^
             " __attribute__((noinline));";
           "void "^c_id^"_value_update("^c_t^"& m, "^
             (String.concat ", " (in_k_decl@out_k_decl))^", double v)";
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

  let profile_map_update c_id in_tl out_tl =
    let in_k_decl, in_k = snd (List.fold_left
        (fun (id,(dacc,vacc)) t ->
          (id+1,(dacc@[(string_of_m3_type t)^" k"^(string_of_int id)],
                 vacc@["k"^(string_of_int id)])))
      (0,([],[])) in_tl)
    in
    let c_t, c_entry_t, c_out_t, c_out_entry_t =
      c_id^"_map", c_id^"_entry", c_id^"_out_map", c_id^"_out_entry"
    in
    let c_update_t =
      let t_l = if out_tl = [] then in_tl else out_tl in
      let update_k_t = mk_tuple_ty (List.map string_of_m3_type t_l)
      in "map<"^update_k_t^",double>"
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
        ["void "^c_id^"_update("^c_t^"& m, "^c_update_t^"& u)"^
           " __attribute__((noinline));";
         "void "^c_id^"_update("^c_t^"& m, "^c_update_t^"& u)";
         "{";"  m.clear();"]@update_loop@["}"]
      else
        ["void "^c_id^"_update("^c_t^"& m, "^
           (String.concat ", " in_k_decl)^", "^c_update_t^"& u)"^
           " __attribute__((noinline));";
         "void "^c_id^"_update("^c_t^"& m, "^
           (String.concat ", " in_k_decl)^", "^c_update_t^"& u)";
         "{";
         "  "^c_t^"::iterator it = m.find("^(mk_tuple in_k)^");";
         "  "^target_t^" om;"]@
            update_loop@
        ["  m.modify(it, bind(&"^c_entry_t^"::__av, _1) = om);";
         "}"]
    in
    let direct_sc =
      let c_direct_t = if in_tl = [] || out_tl = [] then c_t else c_out_t in
      if in_tl = [] || out_tl = [] then
      ["void "^c_id^"_update_direct("^c_t^"& m, "^c_direct_t^"& u)"^
         " __attribute__((noinline));";
       "void "^c_id^"_update_direct("^c_t^"& m, "^c_direct_t^"& u)";
       "{"; "  m = u;"; "}"]
      else
      ["void "^c_id^"_update_direct("^c_t^"& m, "^
         (String.concat ", " in_k_decl)^", "^c_direct_t^"& u)"^
         " __attribute__((noinline));";
       "void "^c_id^"_update_direct("^c_t^"& m, "^
         (String.concat ", " in_k_decl)^", "^c_direct_t^"& u)";
       "{";
       "  "^c_t^"::iterator it = m.find("^(mk_tuple in_k)^");";
       "  m.modify(it, bind(&"^c_entry_t^"::__av, _1) = u);"; 
       "}"]
    in Lines(indirect_sc@direct_sc)


  let declare_profiling schema =
    cscl ~delim:"\n" (List.flatten (List.map (fun (id, in_tl, out_tl) ->
      match in_tl, out_tl with
        | [],[] -> []
        | (x as i),([] as o) | ([] as i),(x as o) ->
          [profile_map_value_update id x;
           profile_map_update id i o] 
        | x,y ->
          [profile_map_value_update (id^"_out") y;
           profile_nested_map_value_update id x y;
           profile_map_update id x y])
     schema))
  
  let profile_trigger sid_offset dbschema (evt,rel,args,imp) =
    let pm_name pm =
      match pm with M3.Insert -> "insert" | M3.Delete -> "delete" in
    let trig_name = (pm_name evt)^"_"^rel in
    let arg_types = List.map snd (List.assoc rel dbschema) in
    let iarg_decls, iargs = List.split (List.map
        (fun (a,ty) ->
          let i_t = imp_type_of_calc_type ty in
          (string_of_calc_type ty)^" "^a, Var(i_t, (a, i_t)))
      (List.combine args arg_types))
    in
    let imp_stmts = match imp with
      | Block(_,s) -> s
      | _ -> failwith "invalid non-block trigger"
    in
    let decls, new_stmts = 
      snd (List.fold_left (fun (stmt_id,(dacc,sacc)) imp ->
          let sid = string_of_int stmt_id in
          let prof_begin = Expr(unit, Fn(unit, Ext(Inline(
            "exec_stats->begin_probe("^
              (string_of_int (sid_offset+stmt_id))^")")), []))
          in
          let prof_end =
            Expr(unit, Fn(unit, Ext(Inline(
              "exec_stats->end_probe("^
                (string_of_int (sid_offset+stmt_id))^")")), []))
          in
          let stmt_name = trig_name^"_s"^sid in
          let new_decl =
            let bimp = match imp with | Block _ -> imp | _ -> Block(unit, [imp]) in
            [Lines (["void "^stmt_name^"("^(String.concat ", " iarg_decls)^")"^
                     " __attribute__((noinline));";
                     "void "^stmt_name^"("^(String.concat ", " iarg_decls)^")"])]@
            [source_code_of_imp bimp]@[Lines ([""])]
          in
          let new_imp =
            [prof_begin; 
             Expr(unit, Fn(unit, Ext(Apply(stmt_name)), iargs)); 
             prof_end]
          in stmt_id+1,(dacc@new_decl,sacc@new_imp))
        (0,([],[])) imp_stmts)
    in let profiled_trig =
      [Expr(unit, Fn(unit,
         Ext(Inline("exec_stats->begin_trigger(\""^trig_name^"\")")), []))]@
      new_stmts@
      [Expr(unit, Fn(unit,
         Ext(Inline("exec_stats->end_trigger(\""^trig_name^"\")")), []))]
    in decls, (evt,rel,args, Block(unit, profiled_trig)) 
  
  (* Top-level code generation *)
  let preamble opts = Lines
      (["#include <fstream>";
        "#include <map>";
        "#include <utility>";
        "#include <boost/archive/xml_oarchive.hpp>";
        "#include <boost/array.hpp>";
        "#include <boost/chrono.hpp>";
        "#include <boost/filesystem.hpp>";
        "#include <boost/fusion/tuple.hpp>";
        "#include <boost/fusion/include/fold.hpp>";
        "#include <boost/lambda/lambda.hpp>";
        "#include <boost/lambda/bind.hpp>";
        "#include <boost/multi_index_container.hpp>";
        "#include <boost/multi_index/hashed_index.hpp>";
        "#include <boost/multi_index/composite_key.hpp>";
        "#include <boost/multi_index/member.hpp>";
        "#include <lib/c++/util.hpp>";
        "#include <lib/c++/streams.hpp>";
        "#include <lib/c++/runtime.hpp>";
        "#include <lib/c++/standard_adaptors.hpp>"]@
       (if not opts.profile then [] else
       ["#include <lib/c++/statistics.hpp>"])@
       ["using namespace ::std;";
        "using namespace ::boost;";
        "using namespace ::boost::chrono;";
        "using namespace ::boost::filesystem;";
        "using namespace ::boost::fusion;";
        "using namespace ::boost::lambda;";
        "using namespace ::boost::multi_index;";
        "using namespace ::dbtoaster;";
        "using namespace ::dbtoaster::adaptors;";
        "using namespace ::dbtoaster::datasets;";
        "using namespace ::dbtoaster::runtime;";
        "using namespace ::dbtoaster::streams;";
        "using namespace ::dbtoaster::util;"]@
       (if not opts.profile then [] else
       ["using namespace ::dbtoaster::statistics;"])) 

  (* Generates source code for map declarations, based on the
   * type_of_map_schema function above *)
  let declare_maps_of_schema schema patterns =
    cscl ~delim:"\n" (List.flatten (List.map (fun (id,in_tl,out_tl) ->
        let (_,t,t_decls) = type_of_map_schema patterns (id, in_tl, out_tl) in 
        let imp_decl = match t with 
          | Host TFloat -> Decl(unit, (id,t), Some(Const(unit, CFloat(0.0)))) 
          | _ -> Decl(unit, (id,t), None)
        in [Lines (List.map string_of_type t_decls)]@
           [source_code_of_imp imp_decl])
      schema))

  let declare_sources_and_adaptors sources =
    let quote s = "\""^(String.escaped s)^"\"" in
    let valid_adaptors = ["csv"      , "csv_adaptor";
                          "orderbook", "order_books::order_book_adaptor";
                          "lineitem" , "tpch::tpch_adaptor";
                          "orders"   , "tpch::tpch_adaptor";
                          "customer" , "tpch::tpch_adaptor";
                          "part"     , "tpch::tpch_adaptor";
                          "supplier" , "tpch::tpch_adaptor";
                          "partsupp" , "tpch::tpch_adaptor";
                          "nation"   , "tpch::tpch_adaptor";
                          "region"   , "tpch::tpch_adaptor"]
    in
    let adaptor_ctor_args = ["lineitem", quote "lineitem";
                             "orders"  , quote "orders";
                             "customer", quote "customer";
                             "part"    , quote "part";
                             "supplier", quote "supplier";
                             "partsupp", quote "partsupp";
                             "nation"  , quote "nation";
                             "region"  , quote "region"] in
    let array_of_id id = id^"[]" in
    let array_of_type t = match t with
        | Target(Type(x)) -> Target(Type(x^"[]"))
        | _ -> t
    in
    let mk_array l = "{ "^(String.concat ", " l)^" }" in
    let adaptors_per_source = List.fold_left (fun sf_acc (s,f,r,a) ->
      if List.mem_assoc (s,f) sf_acc then
        let current_ra = List.assoc (s,f) sf_acc in
        ((List.remove_assoc (s,f) sf_acc)@[(s,f), (current_ra@[r,a])])
      else sf_acc@[(s,f), [r,a]]) [] sources
    in
    let decls = List.map (fun (sf, ra) ->
      let adaptor_meta = List.fold_left (fun acc (r,a) ->
        if List.mem_assoc (fst a) valid_adaptors then
          let a_t_id = List.assoc (fst a) valid_adaptors in
          let a_spt = "shared_ptr<"^a_t_id^" >" in
          let a_id, a_t = gensym(), Target(Type(a_spt)) in
          let params_arr = mk_array (List.map (fun (k,v) ->
            "make_pair("^(quote k)^","^(quote v)^")") (snd a)) in
          let param_id, param_t, param_arr_t =
            let t = Target(Type("pair<string,string>")) in
            a_id^"_params", t, array_of_type t in
          let param_d = Decl(unit, (array_of_id param_id, param_t),
            Some(Fn(param_arr_t, Ext(Inline(params_arr)), [])))
          in
          let d_ctor_args =
            let extra_args =
              if List.mem_assoc (fst a) adaptor_ctor_args
              then [List.assoc (fst a) adaptor_ctor_args] else []
            in  
            String.concat "," ([r^"id"]@extra_args@
                               [string_of_int (List.length (snd a)); param_id])
          in
          let d = Decl(unit, (a_id, a_t), 
                    Some(Fn(a_t, Ext(Constructor(a_t)),
                      [Fn(Target(Type(a_t_id)),
                       Ext(Inline("new "^a_t_id^"("^d_ctor_args^")")), [])])))
          in acc@[a_id, [param_d;d]]
        else failwith ("unsupported adaptor of type "^(fst a)))
        [] ra
      in
      let source_var, source_decl = match sf with
        | FileSource n, Delimited d ->
          let f_id, f_t = gensym(), Target(Type("frame_descriptor")) in
          let f_decl = Decl(unit, (f_id, f_t),
            Some(Fn(f_t, Ext(Constructor(f_t)),
              [Fn(Target(Type("string")), Ext(Inline(quote d)), [])])))
          in
          let ad_arr_id, ad_ptr_t, ad_arr_t =
            let t = Target(Type("shared_ptr<stream_adaptor>")) in
            gensym(), t, array_of_type t in
          let ad_arr = mk_array (List.map fst adaptor_meta) in
          let ad_arr_decl = Decl(unit, (array_of_id ad_arr_id, ad_ptr_t),
            Some(Fn(ad_arr_t, Ext(Inline(ad_arr)), [])))
          in
          let al_id, al_t =
            gensym(), Target(Type("list<shared_ptr<stream_adaptor> >"))
          in
          let ad_arr_it_expr = ad_arr_id^", "^
            ad_arr_id^ " + sizeof("^ad_arr_id^") / sizeof(shared_ptr<stream_adaptor>)"
          in
          let al_decl = Decl(unit, (al_id, al_t),
            Some(Fn(al_t, Ext(Constructor(al_t)),
              [Fn(ad_arr_t, Ext(Inline(ad_arr_it_expr)), [])])))
          in
          let s_id, s_t, s_ptr_t =
            let x = "dbt_file_source" in 
            gensym(), Target(Type(x)), Target(Type("shared_ptr<"^x^">")) in
          let s_ctor =
            let dbt_fs_ctor = ssc (source_code_of_expr
              (Fn(s_t, Ext(Constructor(s_t)),
                 [Fn(Target(Type("string")), Ext(Inline(quote n)), []);
                    Var(f_t, (f_id, f_t)); Var(al_t, (al_id, al_t))])))
            in Fn(s_ptr_t, Ext(Constructor(s_ptr_t)),
                    [Fn(s_t, Ext(Inline("new "^dbt_fs_ctor)), [])])
          in
          let s_decl = Decl(unit, (s_id, s_ptr_t), Some(s_ctor)) in
            Var(s_ptr_t, (s_id, s_ptr_t)), [f_decl; ad_arr_decl; al_decl; s_decl]
          
        | _,_ -> failwith "unsupported data source and framing types" 
      in source_var, (List.flatten (List.map snd adaptor_meta))@source_decl)
      adaptors_per_source
    in 
    let rsc = cscl ~delim:"\n" (List.map (fun sd ->
        cscl (List.map source_code_of_imp sd)) (List.map snd decls))
    in (List.map fst decls), rsc

  (* Stream identifier generation *)
  let declare_streams dbschema = 
    let x,y = snd (List.fold_left
      (fun (i,(id_acc,sc_acc)) (rel,_) -> (i+1,(id_acc@[rel,i], sc_acc@
        [Lines(["int "^rel^"id = "^(string_of_int i)^";"])])))
      (0,([], [Lines ["map<string, int> stream_identifiers;"]])) dbschema)
    in x, cscl y
  
  (* Main function generation *)
  let declare_main opts stream_ids map_schema source_vars trig_reg_info tlqs =
    let mt = Target(Type("stream_multiplexer")) in
    let dt = Target(Type("stream_dispatcher")) in
    let rot = Target(Type("runtime_options")) in
    let exec_ts, exect =
      let x = "trigger_exec_stats" in x,Target(Type("shared_ptr<"^x^">")) in
    let delta_sz_ts, delta_sz_t =
      let x = "delta_size_stats" in x, Target(Type("shared_ptr<"^x^">")) in
    let globals =
      (* Multiplexer constructor: seed and stream batch size *)
      [Decl(unit, ("multiplexer", mt),
        Some(Fn(mt, Ext(Constructor(mt)),
          [Fn(Host TInt, Ext(Inline("12345")), []);
           Fn(Host TInt, Ext(Inline("10")), [])])));
       Decl(unit, ("dispatcher", dt), None);
       Decl(unit, ("run_opts", rot), None)]@
      (if opts.profile then 
        [Decl(unit, ("exec_stats", exect), None);
         Decl(unit, ("delta_stats", delta_sz_t), None)]
       else [])
    in
    let main_fn =
      let register_streams = List.map (fun (rel,id) ->
        "  stream_identifiers[\""^(String.escaped rel)^"\"] "^
              "= "^(string_of_int id)^";") stream_ids in
      let register_src = List.map (function
        | Var(_,(id,_)) -> "  multiplexer.add_source("^id^");"
        | _ -> failwith "invalid source") source_vars
      in
      let register_triggers = List.map (fun (sid, evt_type, unwrap_fn_id,_) ->
        let args = String.concat ", " [sid; evt_type; ("&"^unwrap_fn_id)] in
        "  dispatcher.add_trigger("^args^");") trig_reg_info
      in
      let map_outputs opts_id archive_id =
        List.flatten (List.map (fun (map_id,_,_) ->
          ["  if ( (!debug && "^opts_id^".is_output_map( \""^
                                            (String.escaped map_id)^"\" )) "^
                  " || (debug && "^opts_id^".is_traced_map( \""^
                                            (String.escaped map_id)^"\" )) )";
           "    "^(ssc (source_code_of_map_serialization archive_id map_id))^";"])
        map_schema)
      in
      let register_toplevel_queries = List.map (fun q -> 
         "  run_opts.add_toplevel_query(\""^q^"\");") tlqs
      in
      let stepper = ["";
         "void trace(std::ostream &ofs, bool debug) {";
         "  boost::archive::xml_oarchive oa(ofs, 0);"]
        @(map_outputs "run_opts" "oa")@
        ["}";
         "";
         "void trace(const path& trace_file, bool debug) {";
         "  if(strcmp(trace_file.c_str(), \"-\")){";
         "    std::ofstream ofs(trace_file.c_str());";
         "    trace(ofs, debug);";
         "  } else {";
         "    trace(cout, debug);";
         "  }";
         "}";
         "";
         "void process_event(stream_event& evt) {";
         "  if ( run_opts.is_traced() ) {";
         "    path trace_file = run_opts.get_trace_file();";
         "    trace(trace_file, true);";
         "  }";
         "  dispatcher.dispatch(evt);";
         "}";
         ""] 
      in
      let init_stats =
        let stmt_ids = List.flatten (List.map (fun (_,_,_,sids) ->
            List.map (fun (i,n) ->
              "  exec_stats->register_probe("^i^", \""^(String.escaped n)^"\");")
            sids)
          trig_reg_info)
        in
         ["  exec_stats = shared_ptr<"^exec_ts^" >("^
            "new "^exec_ts^"(\"exec\", "^
              "run_opts.get_stats_window_size(), run_opts.get_stats_period(), "^
              "run_opts.get_stats_file()));";
          "  delta_stats = shared_ptr<"^delta_sz_ts^">("^
            "new "^delta_sz_ts^"(\"delta_sz\", "^
              "run_opts.get_stats_window_size(), run_opts.get_stats_period(), "^
              "run_opts.get_stats_file()));"]
         @stmt_ids
      in
      Lines (stepper@
             ["int main(int argc, char* argv[]) {"]
             @register_streams@[
              "  run_opts.init(argc, argv);";
             ]@register_toplevel_queries@[
              "  if ( run_opts.help() ) { exit(1); };" ]
             @register_src        (* Add all sources to multiplexer *)
             @register_triggers   (* Add unwrappers to dispatcher *)
             @(if opts.profile then init_stats else [])@
             ["  run_opts.log_dispatcher(dispatcher,stream_identifiers);";
              "  while ( multiplexer.has_inputs() ) {";
              "    shared_ptr<list<stream_event> > events = multiplexer.next_inputs();";
              "    if ( events ) {";
              "      for_each(events->begin(), events->end(), ";
              "        bind(&process_event, _1));";
              "    }";
              "  }";
              "  trace(run_opts.get_output_file(), false);"]@
             (if not opts.profile then [] else
             ["  exec_stats->save_now();"])@
             ["}"])
    in cscl (List.map source_code_of_imp globals), main_fn
     
            
end (* CPPTarget *)


(* An imperative compiler implementation *)
module Compiler : CompilerSig =
struct
  open ImpBuilder.DirectIRBuilder
  open ImpBuilder.Imp
  open Imperative
  open Common
  open CPPTarget

  module IBK = ImpBuilder.AnnotatedK3
  module IBC = ImpBuilder.Common
  
  let compile_triggers opts dbschema (schema,patterns,trigs) :
    (source_code_t list * (ext_type type_t, ext_type, ext_fn) Program.trigger_t) list
  =
    let imp_type_of_calc_type t = match t with
      | Calculus.TInt -> Host K.TInt
      | Calculus.TLong -> failwith "Unsupport K3/Imp type: long"
      | Calculus.TDouble -> Host K.TFloat
      | Calculus.TString -> failwith "Unsupport K3/Imp type: string"
    in
    let stmt_cnt = ref 0 in
    List.map (fun (evt,rel,args,k3stmts) ->
        let arg_types = List.map2 (fun v1 v2 ->
            v1, imp_type_of_calc_type (snd v2)) args (List.assoc rel dbschema)
        in
        let type_env = type_env_of_declarations arg_types schema patterns in
        let dsi i = if opts.desugar then desugar_imp opts type_env i else i in
        let si_l = List.map (fun s ->
          let untyped_imp =
            let i = imp_of_ir (var_type_env type_env) (ir_of_expr (snd s))
            in match i with
              | [x] -> x | _ -> Imperative.Block (None, i)
          in infer_types type_env untyped_imp) k3stmts
        in 
        let dsimp = Imperative.Block (Host TUnit, List.map dsi si_l) in
        let t = (evt,rel,args, dsimp) in
        let r =  if not opts.profile then [],t
                 else profile_trigger !stmt_cnt dbschema t
        in stmt_cnt := !stmt_cnt + (List.length k3stmts); r)
      trigs

  let compile_query_to_string opts dbschema k3prog sources tlqs =
    String.concat "\n\n"
      (List.map (fun (_,(_,_,_,imp)) -> string_of_ext_imp imp)
        (compile_triggers opts dbschema k3prog))

  let compile_query opts dbschema (schema,patterns,trigs) sources tlqs chan =
    let pm_name pm = match pm with M3.Insert -> "insert" | M3.Delete -> "delete" in
    let map_decls = declare_maps_of_schema schema patterns in
    let profiling = if opts.profile then declare_profiling schema else Lines([]) in
    let triggers = cscl (List.flatten
      (List.map (fun (t_decls,t) -> t_decls@(source_code_of_trigger dbschema t))
        (compile_triggers opts dbschema (schema,patterns,trigs))))
    in
    let stream_ids, stream_id_decls = declare_streams dbschema in
    let source_vars, source_and_adaptor_decls =
      declare_sources_and_adaptors sources in 
    let global_decls, main_fn =
      let cnt = ref(-1) in
      let trig_reg_info = List.map (fun (evt,rel,_,stmts) ->
          let trig_name = (pm_name evt)^"_"^rel in
          let exec_ids = snd (List.fold_left (fun (i,acc) _ -> incr cnt;
            let x = string_of_int !cnt
            in i+1,acc@[x, trig_name^"_s"^(string_of_int i)]) (0,[]) stmts)
          in ((rel^"id"), ((pm_name evt)^"_tuple"), ("unwrap_"^trig_name),
              exec_ids))
        trigs
      in declare_main opts stream_ids schema source_vars trig_reg_info tlqs
    in
    let program = ssc (cscl ~delim:"\n"
      ([preamble opts; global_decls; map_decls; profiling; triggers;
        stream_id_decls; source_and_adaptor_decls; main_fn;]))
    in
      Util.GenericIO.write chan 
        (fun out_file -> 
           output_string out_file program; 
           output_string out_file "\n"; flush out_file)
end
