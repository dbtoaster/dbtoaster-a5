
open Util

exception GenericCompileError of string;;

let give_up err = raise (GenericCompileError(err));;

let print_line x = print_string (x^"\n");;

let valid_langs = 
  List.fold_left (fun accum x -> StringSet.add x accum) StringSet.empty [
    "calculus"; "m3"; "ocaml"; "c++"
  ]

(********* PARSE ARGUMENTS *********)

type flag_type_t = NO_ARG | OPT_ARG | ARG | ARG_LIST

let (flags,helptext,flagtext) = 
  let (flags,helptext_left,helptext_right,help_width) =
    let flags = 
      [ (["-l";"--lang"], 
            ("LANG",   ARG), "ocaml|cpp|calc|m3",
            "specify the output language (default: ocaml)");
        (["-o"],
            ("OUTPUT", ARG), "<outfile>",
            "output to <outputfile> (default: stdout)" );
        (["-d"],
            ("DEBUG",  ARG_LIST), "<flag> [-d <flag> [...]]",
            "enable a debug flag" );
        (["-?"], 
            ("HELP",   NO_ARG),  "", 
            "display this help text" );
      ]
    in 
      List.fold_left (fun (m,doc_left,doc_right,doc_width) (klist,v,pdoc,doc) -> 
        let left_text = (List.fold_left (fun accum k ->
            if accum = "" then k else accum^"|"^k
          ) "" klist)^(if pdoc = "" then "" else " "^pdoc)
        in
          (
            List.fold_left (fun m2 k ->
              StringMap.add k v m2
            ) m klist,
            doc_left@[left_text],
            doc_right@[doc],
            if (String.length left_text)+5 > doc_width 
              then (String.length left_text)+5
              else doc_width
          ) 
      )(StringMap.empty,[],[],0) flags
  in
    ( flags, 
      List.fold_left2 (fun accum left right -> 
        accum^left^
        (String.make (help_width-(String.length left)) ' ')^
        right^"\n"
      ) "" helptext_left helptext_right,
      List.fold_left (fun accum left -> 
        accum^"["^left^"] "
      ) "" helptext_left
    )
;;

let arguments = 
  let (parse_state,(last_flag,last_type)) = 
    Array.fold_left (fun (parse_state,(last_flag,last_type)) arg ->
      if (String.length arg > 1) && (arg.[0] = '-') then 
        ((*print_line ("arg: "^arg^"; FLAG!");*)
          if (last_type <> NO_ARG) && (last_type <> OPT_ARG) then
            give_up ("Missing argument to flag: "^last_flag)
          else
            if not (StringMap.mem arg flags) then
              give_up ("Unknown flag: "^arg)
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

(********* UTILITIES FOR VALIDATING/EXTRACTING ARGUMENTS *********)

let flag_vals (flag:string): string list = 
  if StringMap.mem flag arguments then (StringMap.find flag arguments)
                                  else []

let flag_val (flag:string): string option = 
  match (flag_vals flag) with
  | []   -> None
  | [a]  -> Some(a)
  | _    -> failwith "Internal Error: single-value flag argument with multiple values";;

let flag_val_force (flag:string): string =
  match flag_val flag with
  | None    -> give_up ("Missing flag: "^flag)
  | Some(a) -> a

let flag_set (flag:string): StringSet.t = 
  List.fold_right (fun f s -> StringSet.add (String.uppercase f) s)
                  (flag_vals flag) StringSet.empty

let flag_bool (flag:string): bool =
  match (flag_vals flag) with
  | [] -> false
  | _  -> true;;

(********* UTILITIES FOR ACCESSING DEBUGFLAGS *********)

let debug_modes = flag_set "DEBUG";;

let debug (mode:string) (f:(unit->'a)): unit =
  if StringSet.mem mode debug_modes then let _ = f () in () else ()

let debug_line (mode:string) (f:(unit->string)): unit =
  debug mode (fun () -> print_line (f ()));;

if flag_bool "HELP" then
  (
    print_string (
      (flag_val_force "$0")^" "^flagtext^"file1 [file2 [...]]\n\n"^helptext
    );
    exit 0
  )
else ();;

(********* EXTRACT/VALIDATE COMMAND LINE ARGUMENTS *********)

let input_files = if (flag_bool "FILES") then flag_vals "FILES" 
                  else give_up "No files provided";;
    
type language_t = L_OCAML | L_CPP | L_CALC | L_M3 | L_NONE;;

let language = match flag_val "LANG" with
  | None -> L_OCAML
  | Some(a) -> match String.uppercase a with
    | "OCAML"    -> L_OCAML
    | "C++"      -> L_CPP
    | "CPP"      -> L_CPP
    | "CALCULUS" -> L_CALC
    | "CALC"     -> L_CALC
    | "M3"       -> L_M3
    | "NONE"     -> L_NONE
    | "DEBUG"    -> L_NONE
    | _          -> (give_up ("Unknown output language '"^a^"'"));;

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

(********* TRANSLATE SQL TO RELCALC *********)

let sql_file_to_calc f =
  let lexbuff = Lexing.from_channel (if f <> "-" then (open_in f) else stdin) in
  Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff;;

debug "PARSE" (fun () -> Parsing.set_trace true);;

let (queries, sources) = 
  let (queries, sources) = 
    List.split (List.map sql_file_to_calc input_files)
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
else ();;

(********* TRANSLATE RELCALC TO M3 *********)

let calc_into_m3_inprogress qname (qlist,dbschema,qvars) m3ip = 
  fst
    (List.fold_left (fun (accum,sub_id) q ->
      (try 
        Compiler.compile Calculus.ModeOpenDomain
                         dbschema
                         (Calculus.make_term q,
                          Calculus.map_term 
                            (qname^"_"^(string_of_int sub_id))
                            qvars
                         )
                         CalcToM3.M3InProgress.generate_m3
                         accum
      with 
        |Util.Function.NonFunctionalMappingException -> 
            Printexc.print_backtrace stdout; give_up "Error!"
      ), sub_id+1 ) (m3ip,1) qlist
    )

let m3_prog = 
  let (m3_prog_in_prog,_) = 
    List.fold_left (fun (accum,id) q ->
      (calc_into_m3_inprogress ("QUERY_"^(string_of_int id)) q accum, id + 1)
    ) (CalcToM3.M3InProgress.init, 1) queries
  in
    CalcToM3.M3InProgress.finalize m3_prog_in_prog;;


if language == L_M3 then
  (
    output_string output_file (M3Common.pretty_print_prog m3_prog);
    exit 0
  )
else ();;

(********* TRANSLATE M3 TO [language of your choosing] *********)
