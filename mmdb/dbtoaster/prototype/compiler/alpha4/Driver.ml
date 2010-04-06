
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
            "Specify the output language (default: ocaml).");
        (["-o"],
            ("OUTPUT", ARG), "<outfile>",
            "Output to <outputfile> (default: stdout)." );
        (["-c"],
            ("COMPILE", ARG), "<obj_file>",
            "Invoke secondary compiler to compile to <obj_file>." );
        (["-d"],
            ("DEBUG",  ARG_LIST), "<flag> [-d <flag> [...]]",
            "Enable a debug flag." );
        (["-?"], 
            ("HELP",   NO_ARG),  "", 
            "Display this help text." );
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
        accum @ ["["^left^"]"]
      ) [] helptext_left
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

let debug_flag df = StringSet.mem df debug_modes;;

if flag_bool "HELP" then
  (
    print_string (
      let max_width = 80 in
      let app_name = (flag_val_force "$0") in
      let (indent_width,skip_line) = 
        if (String.length app_name) * 4 > max_width then
          (4, "\n    ") else (String.length app_name, "")
      in
      let (indented_app_invocation,_) = 
        (List.fold_left (fun (accum,width) field ->
          if width + (String.length field) + 1 > max_width then
            ( accum^"\n"^(String.make (indent_width+1) ' ')^field,
              (String.length field)+1 )
          else
            ( accum^" "^field, width+(String.length field)+1 )
        ) ("", indent_width) (flagtext@["file1 [file2 [...]]"]))
      in
        app_name^skip_line^indented_app_invocation^"\n\n"^helptext
    );
    exit 0
  )
else ();;

(********* EXTRACT/VALIDATE COMMAND LINE ARGUMENTS *********)

let input_files = if (flag_bool "FILES") then flag_vals "FILES" 
                  else give_up "No files provided";;
    
type language_t = L_OCAML | L_CPP | L_SQL | L_CALC | L_M3 | L_NONE;;

let language = match flag_val "LANG" with
  | None -> L_OCAML
  | Some(a) -> match String.uppercase a with
    | "OCAML"    -> L_OCAML
    | "C++"      -> L_CPP
    | "CPP"      -> L_CPP
    | "CALCULUS" -> L_CALC
    | "CALC"     -> L_CALC
    | "M3"       -> L_M3
    | "SQL"      -> L_SQL  (* Translates DBT-SQL + sources -> SQL w/ Inserts *)
    | "NONE"     -> L_NONE (* Used for getting just debug output *)
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

(********* PRODUCE SQL FOR COMPARISON *********)

if language == L_SQL then
  (
    (* failwith "Translation to normal SQL unsupported at the moment"; *)
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

debug_line "M3" (fun () -> (M3Common.pretty_print_prog m3_prog));;

(********* TRANSLATE M3 TO [language of your choosing] *********)

module M3OCamlCompiler = M3Compiler.Make(M3OCamlgen.CG);;

let compile_function: (M3.prog_t * M3.relation_input_t list -> 
                       Util.GenericIO.out_t -> unit) = 
  match language with
  | L_OCAML -> M3OCamlCompiler.compile_query
  | L_CPP   -> give_up "Compilation to C++ not implemented yet"
  | L_M3    -> (fun (p, s) f -> 
      GenericIO.write f (fun fd -> 
          output_string fd (M3Common.pretty_print_prog p)))
  | L_NONE  -> (fun q f -> ())
  | _       -> failwith "Error: Asked to output unknown language"
    (* Calc should have been outputted before M3 generation *)
;;

let compile = compile_function (m3_prog, sources);;

if (not (flag_bool "COMPILE")) || (flag_bool "OUTPUT") then
  (* If we've gotten a -c but no -o, don't output the intermediaries *)
  compile (GenericIO.O_FileDescriptor(output_file))
else ();;

(********* COMPILE [language of your choosing] *********)

let compile_ocaml in_file_name =
  let ocaml_cc = "ocamlc" in
  let ocaml_lib_ext = ".cma" in
  let dbt_lib_ext = ".ml" in
  let ocaml_libs = [ "unix"; "str" ] in
  let dbt_lib_path = Filename.dirname (flag_val_force "$0") in
  let dbt_includes = [ "lib/ocaml" ] in
  let dbt_libs = [ "Util";
                   "M3";
                   "M3Common";
                   "lib/ocaml/SliceableMap";
                   "lib/ocaml/M3OCaml";
                   "lib/ocaml/StandardAdaptors" ] in
    (* would nice to generate args dynamically off the makefile *)
    Unix.execvp ocaml_cc 
      ( Array.of_list (
        [ ocaml_cc; "-ccopt"; "-O3" ] @
        (if debug_flag "COMPILE-WITH-GDB" then [ "-g" ] else []) @
        (List.flatten (List.map (fun x -> [ "-I" ; x ]) dbt_includes)) @
        (List.map (fun x -> x^ocaml_lib_ext) ocaml_libs) @
        (List.map (fun x -> dbt_lib_path^"/"^x^dbt_lib_ext) dbt_libs) @
        ["-" ; in_file_name ; "-o" ; (flag_val_force "COMPILE") ]
      ));;
  
let compile_ocaml_via_tmp () =
  compile (GenericIO.O_TempFile("dbtoaster_", ".ml", compile_ocaml));;

if flag_bool "COMPILE" then
  match language with
  | L_OCAML ->  
    (
      match (flag_val "OUTPUT") with
        | None      -> compile_ocaml_via_tmp ()
        | Some("-") -> compile_ocaml_via_tmp ()
        | Some(a)   -> compile_ocaml a
    )
  | L_CPP   -> give_up "Compilation of C++ not implemented yet"
  | _       -> give_up "No external compiler available for this language"
else ()
