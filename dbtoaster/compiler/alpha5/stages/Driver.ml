(*
  Driver.ml
  
  The primary interface to the dbtoaster compiler; Compiles into dbtoaster
  
  Most of Driver is just glue code and parameter parsing options; The process
  can be broken down into roughly 5 stages:
  
  1 - Parse user options.  Self explanatory.  For a full rundown, compile
      and run './dbtoaster -?'  Among other things, this is where we learn
      what language the user wants to compile into.  This is also where we
      figure out where to output.  The global 'output_file' variable is set to
      the appropriate output stream.
      
  2 - Translate SQL to Calculus.  Most of the heavy lifting here is done by
      ocaml's builtin Parsing module, and the lexing/parsing modules in the
      parser subdirectory (in particular Sqlparser).  
      
      After this phase we are left with a list of querysets (one per input
      file).  Each queryset contains a list of queries, a schema, and a list of
      relation input sources (M3.relation_input_t).  
      
      If the user indicates that they want their output in relational calculus
      form (ie, "-l calc" is passed in on the command line), convert the 
      generated formula to a string, dump it into the output file, and exit.
  
  3 - Translate Calculus to M3.  This is where most of the exciting stuff takes
      place.  Most of the heavy lifting is done by the Compiler and CalcToM3 
      modules: Compiler.compile and CalcToM3.M3InProgress.generate_m3.  
      compile, when invoked with generate_m3 will output an M3InProgress.t that
      we can repeatedly invoke Compiler.compile on.  
      
      Compiler.compile is invoked once per query (recall, queries are stored 
      under 2 levels of list nesting), and an M3.prog_t is extracted from the
      result.
      
  4 - Translate M3 to the target language.  If the target language is M3, then
      we just need to turn it into a string before outputting it.  Otherwise,
      we need to do some code generation.  The heavy lifting here is done by
      the M3Compiler module parametrized by an M3Codegen.
      
      Each subdirectory of the codegen directory contains a subclass of 
      M3Codegen.  Parametrized appropriately, M3Compiler translates an M3.prog_t
      and a set of relation input sources (obtained in step 2) into the target
      language.  Currently, support exists for compiling into:
        - OCAML
      
      Unless "-l none" is specified, the compiler will compile the M3 program 
      and output to the designated output_file (with one caveat, see step 5).
      
  5 - Invoke the secondary compiler.  If the target language is one with its own
      (external) compiler (eg, Ocaml / C++), dbtoaster can be invoked with the
      "-c" flag.  As a convenience, the "-c" flag will invoke the external 
      compiler on the generated source file.  
      
      If no output file is specified for the target language (or if outputting 
      to stdout), the M3->Target compiler will be invoked to produce output to 
      a temporary file.  Consequently, unless explicitly directed to output to
      stdout "-o -", only the temporary file will be produced (ie, the target
      language output will not be dumped to stdout if "-c" is used).
*) 

open Util

(********* PARSE ARGUMENTS *********)

type flag_type_t = NO_ARG | OPT_ARG | ARG | ARG_LIST

let flag_descriptors = 
  ParseArgs.compile
    [ (["-l";"--lang"], 
          ("LANG",    ParseArgs.ARG), "<language>",
          "Specify the output language (default: ocaml).");
      (["-o";"--output-source"],
          ("OUTPUT",  ParseArgs.ARG), "<outfile>",
          "Output source file to <outputfile> (default: stdout)." );
      (["-c";"--compile-binary"],
          ("COMPILE", ParseArgs.ARG), "<binary_file>",
          "Invoke secondary compiler to compile to a runnable binary." );
      (["-r";"--run";"--interpret"],
          ("INTERPRETER", ParseArgs.NO_ARG), "",
          "Run the query in interpreter mode (equivalent to -l run)." );
      (["-a";"--adaptors"],
          ("RUN ADAPTORS", ParseArgs.ARG), "<output dir>",
          "Run the adaptors to prepare input data files.");
      (["--depth"],
          ("COMPILE_DEPTH", ParseArgs.ARG), "<depth> | -",
          "Limit the compile depth; 1 = standard view maint, - = no limit");
      (["-d"],
          ("DEBUG",   ParseArgs.ARG_LIST), "<flag> [-d <flag> [...]]",
          "Enable a debug flag." );
      (["-?";"--help"], 
          ("HELP",    ParseArgs.NO_ARG),  "", 
          "Display this help text." );
    ];;

let arguments = ParseArgs.parse flag_descriptors;;

(********* UTILITIES FOR ACCESSING FLAGS *********)

(* Initialize Util.ParseArgs *)
let (flag_vals, flag_val, flag_val_force, flag_set, flag_bool) = 
  ParseArgs.curry arguments;;

(* Initialize Util.Debug *)
Debug.set_modes (flag_set "DEBUG");

if flag_bool "HELP" then
  (
    print_string (ParseArgs.helptext arguments flag_descriptors);
    print_endline "\n------------\n";
    print_endline 
      ( "Language Support\n\n"^
        "sql           Echo the parsed SQL\n"^
        "calc          DBToaster-style Relational Calculus\n"
      );
    exit 0
  )
else ();;

(********* EXTRACT/VALIDATE COMMAND LINE ARGUMENTS *********)

let input_files = if (flag_bool "FILES") then flag_vals "FILES" 
                  else give_up "No files provided";;

type language_t = 
   | L_SQL
   | L_CALC
   | L_INTERPRETER
   | L_NONE

let language = 
  if flag_bool "INTERPRETER" then L_INTERPRETER
  else
    match flag_val "LANG" with
    | None -> L_CALC
    | Some(a) -> match String.uppercase a with
      | "SQL"      -> L_SQL
      | "CALCULUS" -> L_CALC
      | "CALC"     -> L_CALC
      | "RUN"      -> L_INTERPRETER
      | _          -> (give_up ("Unknown output language '"^a^"'"));;

let output_file = match flag_val "OUTPUT" with
  | None      -> stdout
  | Some("-") -> stdout
  | Some(f)   -> (open_out f);;

let adaptor_dir = flag_val "RUN ADAPTORS";;

Debug.exec "ARGS" (fun () -> 
  StringMap.iter (fun k v ->
    print_endline (k^":");
    List.iter (fun o -> 
      print_endline ("   "^o)
    ) v
  ) arguments
);;

(********* PARSE THE SQL *********)

let parse_file (f:string): Sql.file_t =
  let lexbuff = Lexing.from_channel (if f <> "-" then (open_in f) else stdin) in
  Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff;;

Debug.exec "PARSE" (fun () -> Parsing.set_trace true);;

let ((sources, sql_queries):Sql.file_t) = 
   List.fold_left Sql.merge_files Sql.empty_file (
      List.map parse_file input_files
   )
;;
if language = L_SQL then
   (
      List.iter (fun q ->
         print_endline (Sql.string_of_select q)
      ) sql_queries;
      exit 0
   )
;;

(********* TRANSLATE SQL TO CALC *********)
let calc_queries = 
   let temp_var = ref (-1) in
   (* TODO: We should provide a different name for each select statement *)
   List.flatten (
      List.map 
         (Calculus.calc_of_sql ~tmp_var_id:temp_var sources)
         sql_queries
   )
;;
if language = L_CALC then
   (
      List.iter (fun (qn,q) ->
         print_endline ("Calculus for "^qn^":\n   "^(Calculus.string_of_calc q))
      ) calc_queries;
      exit 0
   )
;;

(********* COMPILE CALC TO AN INCREMENTAL PLAN *********)
let incremental_plans = 
   List.map (fun (qn,q) -> 
      IncrementalPlan.compile_map qn (IncrementalPlan.convert_calc q) 
   ) calc_queries

(********* OPTIMIZE INCREMENTAL PLAN *********)


(********* TRANSLATE INCREMENTAL PLAN TO INCREMENTAL PROGRAM *********)


(********* TRANSLATE INCREMENTAL PROGRAM TO K3 *********)


(********* TRANSLATE K3 TO TARGET LANGUAGE *********)
