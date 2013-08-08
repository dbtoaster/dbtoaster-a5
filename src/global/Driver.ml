(**
   Command and control functionality; The main() function of the dbtoaster 
   binary
*)
let (inform, warn, error, bug) = Debug.Logger.functions_for_module ""

;;

(************ Language Names ************)
type language_t =
   | Auto | SQL | Calc | MPlan | DistM3 | M3 | M3DM | K3 | IMP | CPP | Scala 
   | Ocaml | Interpreter

let languages =
   (* string     token         human-readable string         show in help?  *)
   [  "AUTO"   , (Auto       , "automatic"                    , false); 
      "SQL"    , (SQL        , "DBToaster SQL"                , false);
      "CALC"   , (Calc       , "DBToaster Relational Calculus", true);
      "PLAN"   , (MPlan      , "Materialization Plan"         , false);
      "M3"     , (M3         , "M3 Program"                   , true);
      "DISTM3" , (DistM3     , "Distributable M3 Program"     , false);
      "M3DM"   , (M3DM       , "M3 Domain Maintenance Program", false);
      "K3"     , (K3         , "K3 Program"                   , false);
      "IMP"    , (IMP        , "Abstract Imperative Program"  , false);
      "SCALA"  , (Scala      , "Scala Code"                   , true);
    (*"OCAML"  , (Ocaml      , "Ocaml Code"                   , false); *)
      "RUN"    , (Interpreter, "Ocaml Interpreter"            , false);
      "CPP"    , (CPP        , "C++ Code"                     , true);
   ]

let input_language  = ref Auto
let output_language = ref Auto

let parse_language lang = 
   if List.mem_assoc (String.uppercase lang) languages then
      let (l,_,_) = 
         (List.assoc (String.uppercase lang) languages)
      in l
   else 
      raise (Arg.Bad("Unknown language "^lang))
;;

(************ Output Formatting ************)
let (files:string list ref) = ref [];;
let output_file = ref "-";;
let output_filehandle = ref None;;
let binary_file = ref "";;
let compiler = ref ExternalCompiler.null_compiler;;

(* Creates the directories of path 'p', where the last element is a file *)
let rec mk_path p =
   if String.contains p '/' then (
      let dir_sep_idx = String.rindex p '/' in
      let dir = String.sub p 0 dir_sep_idx in
      if not (Sys.file_exists dir) then (
         mk_path dir;
         Unix.mkdir dir 0o750 )
      else if not (Sys.is_directory dir) then
            raise (Arg.Bad(dir^" already exists and is not a directory."))
   )
      
let output s = 
   let fh = 
      match !output_filehandle with 
      | None -> 
         let fh = 
            if !output_file = "-" then stdout
            else (
               mk_path !output_file;
               open_out !output_file )
         in
            output_filehandle := Some(fh); fh
      | Some(fh) -> fh
   in
      output_string fh s
;;

let output_endline s = output (s^"\n");;

let flush_output () = 
   match !output_filehandle with None -> ()
   | Some(fh) -> flush fh;;

(************ Optimizations ************)
let optimizations_by_level = 
   [  (** -O1 **) [
         "WEAK-EXPR-EQUIV";
         "K3-NO-OPTIMIZE";
         "COMPILE-WITHOUT-OPT";
         "DUMB-LIFT-DELTAS";
      ]; 
      (** -O2 **) [
      ];
      (** -O3 **) [
         "UNIFY-EXPRESSIONS";
         "AGGRESSIVE-FACTORIZE";
         "AGGRESSIVE-UNIFICATION";
         "DELETE-ON-ZERO";
         "OPTIMIZE-PATTERNS";
      ];
   ]
let optimizations = 
   ListAsSet.union (ListAsSet.multiunion optimizations_by_level) [
      "IGNORE-DELETES"; "HEURISTICS-ALWAYS-UPDATE";
      "HASH-STRINGS"; "EXPRESSIVE-TLQS"; "COMPILE-WITH-STATIC";
      "CALC-DONT-CREATE-ZEROES"; "HEURISTICS-ENABLE-INPUTVARS";
      
      (* This is generally more efficient, but doesn't respect side-effect-
         producing statements, and is unsafe *)
      "K3-OPTIMIZE-LIFT-UPDATES";
   ]
let opt_level = ref 2;;
let max_compile_depth = ref None;;

(************ Command Line Parsing ************)
let specs:(Arg.key * Arg.spec * Arg.doc) list  = Arg.align [ 
   (  "-l", 
      (Arg.String(fun x -> output_language := parse_language x)), 
      "lang   Set the compiler's output language to lang");
(*     Disabled in the release.  Use .suffix instead  
   (  "-i", 
      (Arg.String(fun x -> input_language := parse_language x)),
      "lang   Set the compiler's input language to lang"); *)
   (  "-d",
      (Arg.String(fun x -> Debug.activate (String.uppercase x))),
      "mode   Activate indicated debugging output mode");
   (  "-o",
      (Arg.Set_string(output_file)),
      "file   Direct the output of dbtoaster (default: stdout)" );
   (  "-c",
      (Arg.Set_string(binary_file)),
      "file   Invoke a second stage compiler on the source file");
   (  "-r",
      (Arg.Unit(fun () -> output_language := Interpreter)),
      "       Run the query in the internal interpreter");
   (  "--custom-prefix",
      (Arg.String(FreshVariable.set_prefix)),
      "pfx    Specify a prefix for generated symbols");
   (  "--debug",
      (Arg.Unit(fun () -> 
               output_language := Interpreter;
               Debug.activate "STEP-INTERPRETER";
               Debug.activate "LOG-INTERPRETER-UPDATES")),
      "       Run the interpreter in debugging mode");
   (  "--depth",
      (Arg.Int(fun d -> max_compile_depth := Some(d))),
      "       Set the compiler's maximum recursive depth");
   (  "--batch",
      (Arg.Unit(fun () -> 
         max_compile_depth := Some(0);
         Debug.activate "EXPRESSIVE-TLQS"
      )),
      "       Generate a non-incremental (batch) query engine.");
   (  "-I", 
      (Arg.String(ExternalCompiler.add_env "INCLUDE_HDR")),
      "dir    Add a directory to the second-stage compiler's include path");
   (  "-L", 
      (Arg.String(ExternalCompiler.add_env "INCLUDE_LIB")),
      "dir    Add a directory to the second-stage compiler's library path");
   (  "-g",
      (Arg.String(ExternalCompiler.add_flag)),
      "arg    Pass through an argument to the second-stage compiler");
   (  "-O1",
      (Arg.Unit(fun () -> opt_level := 1)),
      "       Produce less efficient code faster");
   (  "-O2",
      (Arg.Unit(fun () -> opt_level := 2)),
      "       A balance between efficient code and compilation speed");
   (  "-O3",
      (Arg.Unit(fun () -> opt_level := 3)),
      "       Produce the most efficient code possible");
   (  "-D", 
      (Arg.String(ExternalCompiler.add_flag ~switch:"-D")),
      "macro Define a macro when invoking the second-stage compiler");
   (  "-F",
      (Arg.String(fun opt -> 
         if not (List.mem opt optimizations) then
            raise (Arg.Bad("Invalid -F flag: "^opt))
         else
            Debug.activate opt)),
      "flag   Activate the specified optimization flag");
   (  "-?", 
      (Arg.Unit(fun () -> raise (Arg.Bad("")) )), 
      "       Display this list of options");
];;

Arg.parse specs (fun x -> files := !files @ [x]) (
   "dbtoaster [opts] sourcefile1 [sourcefile2 [...]]"^
   "\n---- Supported Languages ----"^
   (ListExtras.string_of_list ~sep:"" (fun (short,(_,title,show)) ->
      if not show then "" else 
         "\n  "^short^(String.make (15-(String.length short)) ' ')^title
   ) languages)^
   "\n---- Options ----"
)
          
;;
if List.length !files < 1 then (
   error "No Files Specified; Exiting"
)
;;

(************ Optimization Choices ************)
if (!opt_level < 0) || (!opt_level > List.length optimizations_by_level) then 
   bug ("Invalid -O flag :"^(string_of_int !opt_level))
else
   List.iter Debug.activate 
             (List.nth optimizations_by_level (!opt_level - 1))
;;

let imperative_opts:ImperativeCompiler.compiler_options ref = ref {
   ImperativeCompiler.desugar = not (Debug.active "IMP-NO-DESUGAR");
   ImperativeCompiler.profile = Debug.active "ENABLE-PROFILING";
};;

(************ Stage Planning ************)

let suffix_regexp = Str.regexp ".*\\.\\([^.]*\\)$" in
   if !input_language == Auto then (
      let first_file = (List.hd !files) in
      if Str.string_match suffix_regexp first_file 0 then 
         input_language := (
            let suffix = (Str.matched_group 1 first_file) in
            match String.lowercase suffix with
               | "sql" -> SQL
               | "m3"  -> M3
               | "k3"  -> K3
               | _     -> SQL
         )
      else (* If you can't autodetect it from the suffix, fall back to SQL *)
         input_language := SQL
   );
   if !output_language = Auto then (
      let default_language = if (!binary_file) = "" then Interpreter else CPP in
      output_language := 
         if !output_file = "-" then default_language else
         if Str.string_match suffix_regexp !output_file 0
         then let suffix = (Str.matched_group 1 !output_file) in
            begin match suffix with
             | "sql"   -> SQL
             | "m3"    -> M3
             | "k3"    -> K3
             | "cpp"
             | "hpp"
             | "h"     -> CPP
             | "scala" -> Scala
             | _       -> K3
            end
         else default_language
   )            
;;

Debug.print "LOG-DRIVER" (fun () ->
   let language_map = List.map (fun (_,(key,name,_)) ->
      (key, name)
   ) languages in
      "Input Language: "^(List.assoc !input_language language_map)^"\n"^
      "Output Language: "^(List.assoc !output_language language_map)
)

type stage_t = 
   | StageParseSQL
 | SQLMarker
   | StagePrintSQL
   | StageSQLToCalc
   | StageParseCalc
 | CalcMarker
   | StagePrintCalc
   | StagePrintSchema
   | StageCompileCalc
 | PlanMarker
   | StagePrintPlan
   | StagePlanToM3
   | StageParseM3
 | M3Marker
   | StageM3DomainMaintenance
   | StageM3ToDistM3
   | StagePrintM3
   | StagePrintM3DomainMaintenance
   | StageM3ToK3
   | StageM3DMToK3
   | StageParseK3
 | K3Marker
   | StageOptimizeK3
   | StagePrintK3
   | StageK3ToTargetLanguage
 | FunctionalTargetMarker
   | StageImpToTargetLanguage
   | StagePrintImp
 | ImperativeTargetMarker
   | StageRunInterpreter
   | StageOutputSource
   | StageCompileSource

let output_stages = 
   [  StagePrintSQL; StagePrintSchema; StagePrintCalc; StagePrintPlan;
      StagePrintM3; StagePrintM3DomainMaintenance; StagePrintK3; StagePrintImp;
      StageOutputSource;  ]
let input_stages = 
   [  StageParseSQL; StageParseCalc; StageParseM3; StageParseK3  ]
let optional_stages =
   [  StageM3ToDistM3; StageRunInterpreter; StageCompileSource  ]

(* The following list (core_stages) MUST be kept in order of desired execution.

   The list of active stages is created with the user's choice of input and 
   output formats determining which stages are unnecessary: This includes all 
   stages before the stage that would normally be used to generate the desired 
   input format (e.g., StageSQLToCalc if Calculus is the input format), and all 
   stages after the stage that produces the desired output format. *)
let core_stages = 
   [  SQLMarker;   (* -> *) StageSQLToCalc; 
      CalcMarker;  (* -> *) StageCompileCalc; 
      PlanMarker;  (* -> *) StagePlanToM3;
      M3Marker;    (* -> *) StageM3DomainMaintenance; StageM3ToK3; 
                            StageM3DMToK3;
      K3Marker;    (* -> *) StageOptimizeK3; StageK3ToTargetLanguage;
      FunctionalTargetMarker; StageImpToTargetLanguage; 
      ImperativeTargetMarker; 
   ]

let stages_from (stage:stage_t): stage_t list =
   ListExtras.sublist (ListExtras.index_of stage core_stages) (-1) core_stages
let stages_to   (stage:stage_t): stage_t list =
   ListExtras.sublist 0 ((ListExtras.index_of stage core_stages)+1) core_stages

let compile_stages final_stage target_compiler =
   compiler := target_compiler;
   StageOutputSource::(stages_to final_stage);;
let functional_stages = compile_stages FunctionalTargetMarker;;
let imperative_stages = compile_stages ImperativeTargetMarker;;

let active_stages = ref (ListAsSet.inter
   ((match !input_language with
      | Auto -> bug "input language still auto"; []
      | SQL  -> StageParseSQL::(stages_from SQLMarker)
      | Calc -> StageParseCalc::(stages_from CalcMarker)
      | M3   -> StageParseM3::(stages_from M3Marker)
      | K3   -> StageParseK3::(stages_from K3Marker)
      | _    -> error "Unsupported input language"; []
    )@output_stages@optional_stages)
   ((match !output_language with
      | Auto   -> bug "output language still auto"; []
      | SQL    -> StagePrintSQL::(stages_to SQLMarker)
      | Calc   -> StagePrintCalc::(stages_to CalcMarker)
      | MPlan  -> StagePrintPlan::(stages_to PlanMarker)
      | M3     -> StagePrintM3::(stages_to M3Marker)
      | DistM3 -> StagePrintM3::StageM3ToDistM3::(stages_to M3Marker)
      | M3DM   -> StagePrintM3DomainMaintenance::
                     (stages_to StageM3DomainMaintenance)
      | K3     -> StagePrintK3::StageOptimizeK3::(stages_to K3Marker)
      | IMP    -> StagePrintImp::(stages_to FunctionalTargetMarker)
      | Scala  -> functional_stages ExternalCompiler.scala_compiler
      | Ocaml  -> functional_stages ExternalCompiler.ocaml_compiler
      | CPP    -> imperative_stages ExternalCompiler.cpp_compiler
         (* CPP is defined as a functional stage because the IMP implementation
            desperately needs to be redone before we can actually use it as an 
            reasonable intermediate representation.  For now, we avoid the
            imperative stage and produce C++ code directly after the 
            functional stage. *)
      | Interpreter
              -> StageRunInterpreter::(stages_to FunctionalTargetMarker)
(*    | _     -> error "Unsupported output language"*)
    )@input_stages))
;;
let stage_is_active s = List.mem s !active_stages;;
let activate_stage s  = active_stages := ListAsSet.union !active_stages [s];;
Debug.exec "LOG-SQL"    (fun () -> activate_stage StagePrintSQL);;
Debug.exec "LOG-CALC"   (fun () -> activate_stage StagePrintCalc);;
Debug.exec "LOG-SCHEMA" (fun () -> activate_stage StagePrintSchema);;
Debug.exec "LOG-PLAN"   (fun () -> activate_stage StagePrintPlan);;
Debug.exec "LOG-M3"     (fun () -> activate_stage StagePrintM3);;
Debug.exec "LOG-M3DM"   (fun () -> activate_stage 
                                   StagePrintM3DomainMaintenance);;
Debug.exec "LOG-K3"     (fun () -> activate_stage StagePrintK3);;
Debug.exec "LOG-PARSER" (fun () -> let _ = Parsing.set_trace true in ());;
Debug.exec "DEBUG-DM"   (fun () -> 
      Debug.activate "DEBUG-DM-IVC"; 
      Debug.activate (*"K3-NO-OPTIMIZE"*)"K3-NO-OPTIMIZE-LIFT-UPDATES";
      Debug.activate "DEBUG-DM-WITH-M3";
  if not (Debug.active "DEBUG-DM-NO-LEFT") then
      Debug.activate "DEBUG-DM-LEFT"
  else
      Debug.activate "M3TOK3-GENERATE-INIT"
);;

(* If we're compiling to a binary (i.e., the second-stage compiler is being
   invoked), then we need to store the compiler output in a temporary file.
   Here, we assume that the user does not wish to see the compilation results.
*)
if !binary_file <> "" then (
   activate_stage StageCompileSource;
   if !output_file = "-" then (
      let (fname, stream) = 
         Filename.open_temp_file ~mode:[Open_wronly; Open_trunc]
                                 "dbtoaster_"
                                 (!compiler).ExternalCompiler.extension
      in
         output_file := fname;
         output_filehandle := Some(stream);
         at_exit (fun () -> Unix.unlink fname)
   )
)

(************ Globals Used Across All Stages ************)
(**Program generated from the SQL files in the input.  Generated in 
   StageParseSQL *)
let sql_program:Sql.file_t ref = ref Sql.empty_file;;

(**Schema of the database, including adaptor and related information.  Generated
   by either StageSQLToCalc (based on sql_program) or in StageParseM3 *)
let db_schema:Schema.t = Schema.empty_db ();;

(**Calculus representation of the input queries.  Generated by StageSQLToCalc
   Each query is labeled with a string identifier.  Note that a single SQL 
   query may map to multiple calculus queries, since each aggregated column in
   the output must be generated as a separate query (this can be eliminated in
   the future by adding support for tuple values to the calculus) *)
let calc_queries:(string * Calculus.expr_t) list ref = ref [];;

(**The materialization plan.  This is typically an intermediate stage after
   the deltas have been generated, but before the triggers have been gathered
   together into an M3 program. *)
let materialization_plan:(Plan.plan_t ref) = ref [];;

(**Representation of the input queries after compilation/materialization.  This
   is the expression that needs to be evaluated to get the query result (in 
   general, each of these will be a single map access) *)
let toplevel_queries:(string * Calculus.expr_t) list ref = ref [];;

(**An initial representation of the compiled program in the M3 language.  This 
   representaiton is equivalent to materialization_plan, but is organized by 
   trigger rather than by datastructure.  The m3 program also contains the
   db_schema and the toplevel_queries fields above. *)
let m3_program:(M3.prog_t ref) = ref (M3.empty_prog ());;
 
(**Representation of the Domain Maintenance Triggers (DMT). It contains a list 
   of triggers which has the same type as the M3 triggers. For each event it 
   contains a list of expressions which shows the computations which are needed 
   for domain maintenance. In this expressions, the domain of the variables for 
   each map is represented by relation.
*)
let dm_program:(M3DM.prog_t ref) = ref( M3DM.empty_prog () );;

(**The K3 program's representation.  This can either be loaded in directly 
   (via StageParseK3) or will be generated by combining m3_program and 
   dm_program.  It contains db_schema and toplevel_queries, as above, and a
   set of triggers, each of which causes the execution of a k3 expression. 
   The K3 program also includes a list of map patterns.
   *)
let k3_program:(K3.prog_t ref) = ref (Schema.empty_db (), ([],[]),[],[]);;

(**If we're running in interpreter mode, we'll need to compile the query into
   an ocaml-executable form.  This is where that executable ``code'' goes. 
   In addition to the code itself, we keep around the map and patterns from the
   k3 program that generated this so that we can properly initialize the 
   database *)
let interpreter_program:
   (K3.map_t list * Patterns.pattern_map * K3Interpreter.K3CG.code_t) ref = 
      ref ([], [], K3Interpreter.K3CG.const(Constants.CInt(0)));;

(**If we're compiling to an imperative language, we have one final stage before
   producing source code: The imperative stage is a flattening of the K3
   representation of the program.  No optimization happens at this stage, but
   because this transformation is likely to be common to all imperative 
   languages, we use this intermediate stage to make the final target language
   compilers as trivial as possible. *)
let imperative_program:(ImperativeCompiler.Target.imp_prog_t ref)
   = ref (ImperativeCompiler.Target.empty_prog ());;

(* String representation of the source code being produced *)
let source_code:string list ref = ref [];;

(************ SQL Stages ************)

if stage_is_active StageParseSQL then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: ParseSQL");
   try 
      List.iter (fun f ->
         let lexbuff = ParsingExtras.lexbuf_for_file_or_stdin f in
         let sql_parsed_file = Sqlparser.dbtoasterSqlFile Sqllexer.tokenize 
                                                          lexbuff 
         in
         sql_program := Sql.merge_files !sql_program sql_parsed_file
      ) !files
   with 
      | Sql.SQLParseError(msg, pos) ->
         error ~exc:true ("Syntax error: "^msg^" "^(
                          ParsingExtras.format_error_at_position pos))
      | Sql.SqlException("",msg) ->
         error ~exc:true ("Sql Error: "^msg)
      | Sql.SqlException(detail,msg) ->
         error ~exc:true ~detail:(fun () -> detail) ("Sql Error: "^msg)
      | Parsing.Parse_error ->
         error ("Sql Parse Error.  (try using '-d log-parser' to track the "^
                "error)")
      | Sys_error(msg) ->
         error msg
      | Sql.InvalidSql(m,d,s) -> 
         let (msg,detail) = Sql.string_of_invalid_sql(m,d,s) in
         error ~exc:true ~detail:(fun () -> detail) msg
   
)
;;
if stage_is_active StagePrintSQL then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintSQL");
   let (tables, queries) = !sql_program in
      output_endline (
         (ListExtras.string_of_list ~sep:"\n" Sql.string_of_table tables)^
         "\n\n"^
         (ListExtras.string_of_list ~sep:"\n" Sql.string_of_select queries)
      )
)
;;

(************ Calculus Stages ************)
let query_id = ref 0;;

if stage_is_active StageSQLToCalc then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: SQLToCalc");
   let (tables, queries) = !sql_program in
      (* Convert the tables into a more friendly format that we'll use 
         throughout the rest of the program.  (Sql should be able to use this
         format directly.  TODO: Recode Sql to do so) *)
      SqlToCalculus.extract_sql_schema db_schema tables;
      
      (* Then convert the queries into calculus. Most of this code lives in
         calculus/SqlToCalculus, but we still need to iterate over all of the
         queries that we were given, and do some target renaming to ensure that
         each of the root level queries has a unique name. *)
      let rename_query = 
         if (List.length queries > 1) then (fun q -> 
            (* If there's more than 1 query name, then make it unique by
               prepending QUERY_[#]_ to the target name *)
            query_id := !query_id + 1;
            "QUERY_"^(string_of_int !query_id)^"_"^q
         ) else (fun q -> q)
      in
      try 
         calc_queries := List.flatten (List.map 
            (fun q -> List.map 
               (fun (tgt_name, tgt_calc) -> (rename_query tgt_name, tgt_calc)) 
               (SqlToCalculus.calc_of_query tables q))
            queries);         
      with 
         | Sql.SqlException("",msg) ->
            error ~exc:true ("Sql Error: "^msg)
         | Sql.SqlException(detail,msg) ->
            error ~exc:true ~detail:(fun () -> detail) ("Sql Error: "^msg)
         | Sql.InvalidSql(m,d,s) -> 
            let (msg,detail) = Sql.string_of_invalid_sql(m,d,s) in
            error ~exc:true ~detail:(fun () -> detail) msg

)
;;
if stage_is_active StageParseCalc then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: ParseCalculus");
   try 
      List.iter (fun f ->
         let lexbuff = 
            Lexing.from_channel (if f <> "-" then (open_in f) else stdin) 
         in let (db_schema0, calc_queries0) =
               (Calculusparser.statementList Calculuslexer.tokenize lexbuff)
         in db_schema := !db_schema0;
            calc_queries := calc_queries0@(!calc_queries)
      ) !files
   with 
      | Parsing.Parse_error ->
         error ("Calculus Parse Error.  (try using '-d log-parser' " ^ 
                "to track the error)")
      | Sys_error(msg) ->
         error msg
   
)
;;
if stage_is_active StagePrintSchema then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintSchema");
   output_endline (Schema.string_of_schema db_schema)
)
;;
if stage_is_active StagePrintCalc then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintCalc");
   let opt = 
      if Debug.active "PRINT-RAW-CALC" then (fun x -> x)
      else (fun expr -> 
         let schema = Calculus.schema_of_expr expr in
            CalculusTransforms.optimize_expr schema expr)
   in
   try
      output_endline (
         (ListExtras.string_of_list ~sep:"\n" 
            (fun (name, calc) -> name^": \n"^
               (CalculusPrinter.string_of_expr (opt calc)))
            !calc_queries)
      )
   with 
   | Calculus.CalculusException(expr, msg) ->
      bug ~exc:true
          ~detail:(fun () -> CalculusPrinter.string_of_expr expr) 
          msg
   | Functions.InvalidFunctionArguments(msg) ->
      error msg
)
;;
if stage_is_active StageCompileCalc then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: CompileCalc");

   (* Compile things and save the accessor expressions *)
   try 
      let mp, tlq = Compiler.compile ~max_depth:!max_compile_depth 
                                     db_schema
                                     !calc_queries 
      in
         materialization_plan := mp;
         toplevel_queries := tlq
   with
   | Calculus.CalculusException(expr, msg) ->
      bug ~exc:true
          ~detail:(fun () -> CalculusPrinter.string_of_expr expr) 
          msg
   | Calculus.CalcRing.NotAValException(expr) ->
      bug ~exc:true
          ~detail:(fun () -> CalculusPrinter.string_of_expr expr)
          "Not A Value"
   | Failure(msg) ->
      bug ~exc:true
          ~detail:(fun () -> ListExtras.string_of_list ~sep:"\n" 
                                (fun (_,x) -> CalculusPrinter.string_of_expr x)
                                !calc_queries)
          msg
   | Functions.InvalidFunctionArguments(msg) ->
      error msg
)
;;
if Debug.active "TIME-CALCOPT" then (
   CalculusTransforms.dump_timings ()
)
;;
if stage_is_active StagePrintPlan then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintPlan");
   output_endline (Compiler.string_of_plan !materialization_plan)
)
;;

(************ M3 Stages ************)
if stage_is_active StagePlanToM3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PlanToM3");
   m3_program := M3.plan_to_m3 db_schema !materialization_plan;
   List.iter (fun (qname,qexpr) ->
      M3.add_query !(m3_program) qname qexpr
   ) !toplevel_queries
)
;;
if stage_is_active StageParseM3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: ParseM3");
   if List.length !files > 1 then
      error "Multiple M3 files not supported yet"
   else 
   let f = List.hd !files in
   let lexbuff = Lexing.from_channel 
      (if f <> "-" then (open_in f) else stdin)
   in 
      try 
         m3_program := 
               Calculusparser.mapProgram Calculuslexer.tokenize lexbuff   
      with
         | Calculus.CalculusException(expr, msg) ->
            error ~exc:true
                  ~detail:(fun () -> CalculusPrinter.string_of_expr expr) 
                  msg)
;;
if stage_is_active StageM3ToDistM3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: M3ToDistM3");
   
   m3_program := DistributedM3.distributed_m3_of_m3 !m3_program
)
;;
if stage_is_active StagePrintM3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintM3");
   output_endline (M3.string_of_m3 !m3_program)
)
;;

(************ M3 Domain Maintenance Stages ************)
if stage_is_active StageM3DomainMaintenance then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: M3DomainMaintenance");
   dm_program := M3DM.m3_to_m3dm (!m3_program);
)
;;
if stage_is_active StagePrintM3DomainMaintenance then (
   Debug.print "LOG-DRIVER" 
               (fun () -> "Running Stage: PrintM3DomainMaintenance");
   output_endline (M3DM.string_of_m3DM !dm_program)
)
;;

(************ K3 Stages ************)

if stage_is_active StageM3ToK3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: M3ToK3");
   if not (Debug.active "DEBUG-DM") then Debug.activate "M3TOK3-GENERATE-INIT";
   try
      let gen_init = (Debug.active "M3TOK3-GENERATE-INIT") in
      k3_program := M3ToK3.m3_to_k3 ~generate_init:gen_init !m3_program; 
      let (_,(_, pats), _, _) = !k3_program in
         Debug.print "LOG-PATTERNS" 
            (fun () -> Patterns.patterns_to_nice_string pats)
   with 
      | Calculus.CalculusException(calc, msg) ->
         bug ~exc:true ~detail:(fun () -> CalculusPrinter.string_of_expr calc)
             msg
      | M3ToK3.M3ToK3Failure(Some(calc), _, msg) ->
         bug ~exc:true ~detail:(fun () -> Calculus.string_of_expr calc) msg
      | M3ToK3.M3ToK3Failure(_, Some(k3), msg) ->
         bug ~exc:true ~detail:(fun () -> K3.string_of_expr k3) msg
      | M3ToK3.M3ToK3Failure(_, _, msg) ->
         bug ~exc:true ~detail:(fun () -> M3.string_of_m3 !m3_program) msg
      | K3Typechecker.K3TypecheckError(stack,msg) ->
         bug ~exc:true ~detail:(fun () -> 
                                K3Typechecker.string_of_k3_stack stack) msg
)
;;

if stage_is_active StageM3DMToK3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: M3DMToK3");
   if (Debug.active "DEBUG-DM") then
   begin
      let k3_printer = 
         if (Debug.active "NICE-K3") then K3.nice_code_of_prog 
                                     else K3.code_of_prog in
      let k3_queries = DMToK3.m3dm_to_k3 !k3_program !dm_program in
      Debug.print "LOG-DMTOK3" (fun () -> k3_printer k3_queries);
      k3_program := k3_queries
   end
)
;;

if stage_is_active StageParseK3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: ParseK3");
   if List.length !files > 1 then
      error "Multiple K3 files not supported yet"
   else 
   let f = List.hd !files in
   let lexbuff = Lexing.from_channel 
      (if f <> "-" then (open_in f) else stdin)
   in 
      k3_program := 
            K3parser.dbtoasterK3Program K3lexer.tokenize lexbuff;
      let (_,(_, pats), _, _) = !k3_program
      in
         Debug.print "LOG-PATTERNS" (fun () -> 
            Patterns.patterns_to_nice_string pats)
)
;;
if (stage_is_active StageOptimizeK3) then (
   if not (Debug.active "K3-NO-OPTIMIZE") then (
      Debug.print "LOG-DRIVER" (fun () -> "Running Stage: OptimizeK3");
      let optimizations = ref [] in
      if not (Debug.active "K3-NO-CSE-OPT")
         then optimizations := K3Optimizer.CSE :: !optimizations;
      if not (Debug.active "K3-NO-BETA-OPT")
         then optimizations := K3Optimizer.Beta :: !optimizations;
      let (db,(maps,patterns),triggers,tlqs) = !k3_program in
      try 
         k3_program := (
            db,
            (maps, patterns),
            List.map (fun (event, stmts) ->
               let trigger_vars = Schema.event_vars event in (
                  event, 
                  if not(Debug.active "DEBUG-DM") || trigger_vars = [] then
                    List.map (
                      K3Optimizer.optimize ~optimizations:!optimizations
                      (List.map fst trigger_vars)  
                    ) stmts
                  else
                    List.map (
                           K3Optimizer.dm_optimize ~optimizations:!optimizations
                             (List.map fst trigger_vars)
                          ) stmts
               )
            ) triggers,
            tlqs
         )
      with 
         | K3Typechecker.K3TypecheckError(stack, msg) -> 
               bug ~exc:true ~detail:(fun () -> 
                      K3Typechecker.string_of_k3_stack stack) msg
   )
)
;;
if stage_is_active StagePrintK3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintK3");
   if Debug.active "ORIGINAL-K3" then
      output_endline (K3.code_of_prog !k3_program)
   else
      output_endline (K3.nice_code_of_prog !k3_program)
)
;;
module K3InterpreterCG = K3Compiler.Make(K3Interpreter.K3CG)
module K3ScalaCompiler = K3Compiler.Make(K3Scalagen.K3CG)
;;
if stage_is_active StageK3ToTargetLanguage then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: K3ToTargetLanguage");
   match !output_language with
      | Interpreter -> (
         try 
            StandardAdaptors.initialize ();
            let (_, (maps, patterns), _, _) = !k3_program in
            interpreter_program := (
               maps, patterns, 
               K3InterpreterCG.compile_k3_to_code !k3_program
            )
         with 
         | K3Interpreter.InterpreterException(expr,msg) ->
            (begin match expr with 
               | Some(s) -> error ~exc:true 
                                  ~detail:(fun () -> K3.string_of_expr s)
                                  msg
               | None    -> error ~exc:true msg
            end)
         | K3Typechecker.K3TypecheckError(stack,msg) ->
            bug ~exc:true ~detail:(fun () -> 
               K3Typechecker.string_of_k3_stack stack) msg
         | Failure(msg) ->
            error ~exc:true msg
         | K3.AnnotationException(None, Some(t), msg) ->
            error ~exc:true ~detail:(fun () -> K3.string_of_type t) msg
         | K3.AnnotationException(_, _, msg) ->
            error ~exc:true msg
         
               
      )   
      | Ocaml       -> bug "Ocaml codegen not implemented yet"
      | Scala       -> 
         source_code := [K3ScalaCompiler.compile_query_to_string !k3_program]
         
         (* All imperative languages now produce IMP *)
      | IMP
      | CPP         -> 
         begin try 
            imperative_program := 
               ImperativeCompiler.Compiler.imp_of_k3 !imperative_opts 
                                                     !k3_program 
         with 
            | K3Typechecker.K3TypecheckError(stack,msg) ->
               bug ~exc:true ~detail:(fun () -> 
                  K3Typechecker.string_of_k3_stack stack) msg
         end

      | _ -> bug "Unexpected K3 target language"
)
;;
if stage_is_active StagePrintImp then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintImp");
   let ((_,_,(imp_core, _)),_,_) = !imperative_program in
      List.iter (fun (_,(_,expr)) ->
         output_endline 
            ((ImperativeCompiler.Target.string_of_ext_imp expr)^"\n\n")
      ) imp_core            
)
;;

(************ Imperative Stages ************)

if stage_is_active StageImpToTargetLanguage then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: ImpToTargetLanguage");
   source_code := 
      ImperativeCompiler.Compiler.compile_imp !imperative_opts
                                              !imperative_program
)
;;

(************ Program Management Stages ************)
if stage_is_active StageRunInterpreter then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: RunInterpreter");
   try 
      let db_checker = 
         if Debug.active "DBCHECK-ON" 
         then Some(DBChecker.DBAccess.init !sql_program)
         else None 
      in
      let (maps,patterns,compiled) = !interpreter_program in
      let db = Database.NamedK3Database.make_empty_db maps patterns in
      let result = K3Interpreter.K3CG.eval db_checker compiled [] [] db in
      if result <> Values.K3Value.Unit then
         output_endline (  "UNEXPECTED RESULT: "^
                           (Values.K3Value.string_of_value result) )   
   with 
   | K3Interpreter.InterpreterException(expr,msg) ->
      (begin match expr with 
         | Some(s) -> error ~exc:true 
                            ~detail:(fun () -> K3.string_of_expr s) 
                            msg
         | None -> error ~exc:true msg
      end)
   | Failure(msg) ->
      bug ~exc:true msg
)
;;
if stage_is_active StageOutputSource then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: OutputSource");
   List.iter output_endline !source_code
)
;;
if stage_is_active StageCompileSource then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: CompileSource");
   flush_output ();
   mk_path !binary_file;
   try 
      (!compiler).ExternalCompiler.compile !output_file !binary_file
   with 
      | Unix.Unix_error(errtype, fname, fargs) ->
         error ~detail:(fun () -> 
            "While running "^fname^"("^fargs^")"
         ) ("Error while compiling: "^(Unix.error_message errtype))
)
;;
