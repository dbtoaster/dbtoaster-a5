
open Util

let print_line x = print_string (x^"\n");;

let valid_langs = 
  List.fold_left (fun accum x -> StringSet.add x accum) StringSet.empty [
    "calculus"; "m3"; "ocaml"; "c++"
  ]

(********* PARSE ARGUMENTS *********)

type flag_type_t = NO_ARG | OPT_ARG | ARG | ARG_LIST

let flags = 
  let flags = 
    [ ("-l",     ("LANG",   ARG));
      ("--lang", ("LANG",   ARG));
      ("-o",     ("OUTPUT", ARG));
      ("-d",     ("DEBUG",  ARG_LIST));
    ]
  in 
    List.fold_left (fun m (k,v) -> StringMap.add k v m) 
                   StringMap.empty flags
;;

let arguments = 
  let (parse_state,(last_flag,last_type)) = 
    Array.fold_left (fun (parse_state,(last_flag,last_type)) arg ->
      if (String.length arg > 1) && (arg.[0] = '-') then 
        ((*print_line ("arg: "^arg^"; FLAG!");*)
          if (last_type <> NO_ARG) && (last_type <> OPT_ARG) then
            failwith ("Missing argument to flag: "^last_flag)
          else
            if not (StringMap.mem arg flags) then
              failwith ("Unknown flag: "^arg)
            else
              let (flag, flag_type) = StringMap.find arg flags in
                match flag_type with
                | NO_ARG -> 
                    (StringMap.add flag [""] parse_state, (flag, flag_type))
                | OPT_ARG ->
                    (StringMap.add flag [] parse_state, (flag, flag_type))
                | _ -> (parse_state, (flag, flag_type))
        )
      else
        ((*print_line ("arg: "^arg^"; NOT FLAG:"^last_flag);*)
          let append_to_entry k v m =
            if StringMap.mem k m then
              StringMap.add k ((StringMap.find k m)@[v]) m
            else
              StringMap.add k [v] m
          in
            match last_type with
            | NO_ARG -> 
                (append_to_entry "FILES" arg parse_state, ("", NO_ARG))
            | OPT_ARG -> 
                (StringMap.add last_flag [arg] parse_state, ("", NO_ARG))
            | ARG -> 
                (StringMap.add last_flag [arg] parse_state, ("", NO_ARG))
            | ARG_LIST -> 
                (append_to_entry last_flag arg parse_state, ("", NO_ARG))
        )
    ) (StringMap.empty, ("$0", ARG)) Sys.argv
  in
    parse_state;;

(********* VALIDATE/EXTRACT ARGUMENTS *********)

let flag_vals (flag:string): string list = 
  if StringMap.mem flag arguments then (StringMap.find flag arguments)
                                  else []

let flag_val (flag:string): string option = 
  match (flag_vals flag) with
  | []   -> None
  | [a]  -> Some(a)
  | _    -> failwith "Internal Error: single-value expression multivalued";;

let flag_set (flag:string): StringSet.t = 
  List.fold_right (fun f s -> StringSet.add (String.uppercase f) s)
                  (flag_vals flag) StringSet.empty

let flag_bool (flag:string): bool =
  match (flag_vals flag) with
  | [] -> false
  | _  -> true;;

let files = if (flag_bool "FILES") then flag_vals "FILES" 
            else failwith "No files provided";;

let debug_modes = flag_set "DEBUG";;

let debug (mode:string) (f:(unit->'a)): unit =
  if StringSet.mem mode debug_modes then let _ = f () in () else ()

let debug_line (mode:string) (f:(unit->string)): unit =
  debug mode (fun () -> print_line (f ()))
    
type language_t = L_OCAML | L_CPP | L_CALC | L_M3;;

let language = match flag_val "LANG" with
  | None -> L_OCAML
  | Some(a) -> match String.uppercase a with
    | "OCAML"    -> L_OCAML
    | "C++"      -> L_CPP
    | "CPP"      -> L_CPP
    | "CALCULUS" -> L_CALC
    | "CALC"     -> L_CALC
    | "M3"       -> L_M3
    | _          -> (failwith ("Unknown output language '"^a^"'"));;

let output_file = match flag_val "OUTPUT" with
  | None      -> stdout
  | Some("-") -> stdout
  | Some(f)   -> (open_out f);;

debug "ARGS" (fun () -> 
  StringMap.iter (fun k v ->
    print_line (k^":");
    List.iter (fun o -> 
      print_line ("   "^o)
    ) v
  ) arguments
);;

debug "PARSE" (fun () -> Parsing.set_trace true);;

(********* TRANSLATE SQL TO RELCALC *********)

let sql_file_to_calc f =
  let lexbuff = Lexing.from_channel (if f <> "-" then (open_in f) else stdin) in
  Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff;;

let (queries, sources) = 
  let (queries, sources) = 
    List.split (List.map sql_file_to_calc files)
  in (List.flatten queries, List.flatten sources);;

let query_list_to_calc_string qlist = 
  List.fold_left 
    (fun accum (query_exprs,_,_) ->
      accum^(List.fold_left (fun in_accum calc_term ->
        in_accum^(Calculus.term_as_string (Calculus.make_term calc_term))^"\n"
      ) "" query_exprs)
    ) "" qlist;;

debug_line "CALCULUS" (fun () -> (query_list_to_calc_string queries));;

if language == L_CALC then
  (
    output_string output_file (query_list_to_calc_string queries);
    exit 0
  )
else ()