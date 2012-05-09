open Calculus
open Plan

let log t post ?(detail=None) msg =
   (prerr_endline (t^": "^msg); 
    begin match detail with 
      | Some(s) -> if Debug.active "DETAIL" then prerr_endline s
      | None -> ()
    end; match post with Some(s) -> (s ()) | None -> ())

let inform = log "INFO"  None
let warn   = log "WARN"  None
let error  = log "ERROR" (Some(fun _ -> exit (-1)))
let bug    = log "BUG"   (Some(fun _ -> exit (-1)))

;;

(************ Language Names ************)
type language_t =
   | Auto | SQL | Calc | MPlan | M3 | M3DM | K3 | IMP | CPP | Scala | Ocaml
   | Interpreter

let languages =
   [  "AUTO" , (Auto       , "automatic"                    ); 
      "SQL",   (SQL        , "DBToaster SQL"                );
      "CALC" , (Calc       , "DBToaster Relational Calculus");
      "PLAN" , (MPlan      , "Materialization Plan"         );
      "M3"   , (M3         , "M3 Program"                   );
      "M3DM" , (M3DM       , "M3 Domain Maintenance Program");
      "K3"   , (K3         , "K3 Program"                   );
      "IMP"  , (IMP        , "Abstract Imperative Program"  );
      "SCALA", (Scala      , "Scala Code"                   );
      "OCAML", (Ocaml      , "Ocaml Code"                   );
      "RUN",   (Interpreter, "Ocaml Interpreter"            );
      "CPP"  , (CPP        , "C++ Code"                     );
   ]

let input_language  = ref Auto
let output_language = ref Auto

let parse_language lang = 
   if List.mem_assoc (String.uppercase lang) languages then
      fst (List.assoc (String.uppercase lang) languages)
   else 
      raise (Arg.Bad("Unknown language "^lang))
;;

(************ Output Formatting ************)
let (files:string list ref) = ref [];;
let output_file = ref "-";;
let output_filehandle = ref None;;
let binary_file = ref "";;
let compiler = ref ExternalCompiler.null_compiler;;

let output s = 
   let fh = 
      match !output_filehandle with 
      | None -> 
         let fh = 
            if !output_file = "-" then stdout
            else open_out !output_file
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
   

(************ Command Line Parsing ************)
let specs:(Arg.key * Arg.spec * Arg.doc) list  = Arg.align [ 
   (  "-l", 
      (Arg.String(fun x -> output_language := parse_language x)), 
      "lang  Set the compiler's output language to lang (default: auto)");
   (  "-i",
      (Arg.String(fun x -> input_language := parse_language x)),
      "lang  Set the compiler's input language to lang (default: auto)");
   (  "-d",
      (Arg.String(fun x -> Debug.activate (String.uppercase x))),
      "mode  Activate indicated debugging output mode");
   (  "-o",
      (Arg.Set_string(output_file)),
      "file  Direct the output to the indicated file (default: '-'/stdout)" );
   (  "-c",
      (Arg.Set_string(binary_file)),
      "file  Invoke the appropriate second stage compiler on the source file");
   (  "-r",
      (Arg.Unit(fun () -> output_language := Interpreter)),
      "      Run the query in the internal interpreter")
];;

Arg.parse specs (fun x -> files := !files @ [x]) 
          "dbtoaster [opts] file1 [file2 [...]]"
;;
if List.length !files < 1 then (
   error "No Files Specified; Exiting"
)
;;

(************ Optimization Choices ************)
let imperative_opts:ImperativeCompiler.compiler_options ref = ref {
   ImperativeCompiler.desugar = not (Debug.active "IMP-NO-DESUGAR");
   ImperativeCompiler.profile = Debug.active "ENABLE-PROFILING";
};;

(************ Stage Planning ************)

if !input_language == Auto then (
   let suffix_regexp = Str.regexp ".*\\.\\([^.]*\\)$" in
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
)
   
;;
if !output_language = Auto
   then (
      if (!binary_file) = "" 
      then output_language := M3
      else output_language := CPP
   )
;;

type stage_t = 
   | StageParseSQL
 | SQLMarker
   | StagePrintSQL
   | StageSQLToCalc
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
   | StagePrintM3
   | StagePrintM3DomainMaintenance
   | StageM3ToK3
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
      StageRunInterpreter; StageOutputSource; StageCompileSource;  ]
let input_stages = 
   [  StageParseSQL; StageParseM3; StageParseK3 ]


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
      | M3   -> StageParseM3::(stages_from M3Marker)
      | K3   -> StageParseK3::(stages_from K3Marker)
      | _    -> error "Unsupported input language"; []
    )@output_stages)
   ((match !output_language with
      | Auto  -> bug "output language still auto"; []
      | SQL   -> StagePrintSQL::(stages_to SQLMarker)
      | Calc  -> StagePrintCalc::(stages_to CalcMarker)
      | MPlan -> StagePrintPlan::(stages_to PlanMarker)
      | M3    -> StagePrintM3::(stages_to M3Marker)
      | M3DM  -> StagePrintM3DomainMaintenance::
                     (stages_to StageM3DomainMaintenance)
      | K3    -> StagePrintK3::StageOptimizeK3::(stages_to K3Marker)
      | IMP   -> StagePrintImp::(stages_to FunctionalTargetMarker)
      | Scala -> functional_stages ExternalCompiler.null_compiler
      | Ocaml -> functional_stages ExternalCompiler.ocaml_compiler
      | CPP   -> imperative_stages ExternalCompiler.cpp_compiler
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
Debug.exec "LOG-K3"     (fun () -> activate_stage StagePrintK3);;
Debug.exec "LOG-PARSER" (fun () -> let _ = Parsing.set_trace true in ());;

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
let dm_program:(M3DM.dm_prog_t ref) = ref( ref ([]));;

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
      ref ([], [], K3Interpreter.K3CG.const(Types.CInt(0)));;

(**If we're compiling to an imperative language, we have one final stage before
   producing source code: The imperative stage is a flattening of the K3
   representation of the program.  No optimization happens at this stage, but
   because this transformation is likely to be common to all imperative 
   languages, we use this intermediate stage to make the final target language
   compilers as trivial as possible. *)
let imperative_program:(ImperativeCompiler.Compiler.imp_prog_t ref)
   = ref (ImperativeCompiler.Compiler.empty_prog ());;

(* String representation of the source code being produced *)
let source_code:string list ref = ref [];;

(************ SQL Stages ************)

if stage_is_active StageParseSQL then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: ParseSQL");
   try 
      List.iter (fun f ->
         let lexbuff = 
            Lexing.from_channel (if f <> "-" then (open_in f) else stdin) 
         in sql_program := Sql.merge_files !sql_program
               (Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff)
      ) !files
   with 
      | Sql.SqlException(msg) ->
         error ("Sql Error: "^msg)
      | Sql.Variable_binding_err(_,_) as vb_err ->
         error ("Sql Error: "^(Sql.string_of_var_binding_err vb_err))
   
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
         ring/SqlToCalculus, but we still need to iterate over all of the
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
      List.iter (fun q -> 
         List.iter (fun (tgt_name, tgt_calc) ->
            calc_queries := (rename_query tgt_name, tgt_calc)::!calc_queries
         ) (List.rev (SqlToCalculus.calc_of_query tables q))
         (* Reverse the order to use :: to build up the query list *)
      ) queries;
)
;;
if stage_is_active StagePrintSchema then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintSchema");
   output_endline (Schema.string_of_schema db_schema)
)
;;
if stage_is_active StagePrintCalc then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintCalc");
   output_endline (
      (ListExtras.string_of_list ~sep:"\n" 
         (fun (name, calc) -> name^": \n"^
            (CalculusPrinter.string_of_expr calc))
         !calc_queries)
   )
)
;; 				
if stage_is_active StageCompileCalc then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: CompileCalc");
   let query_ds_list = List.map (fun (qname,qexpr) -> {
      Plan.ds_name = Plan.mk_ds_name qname 
                                     (Calculus.schema_of_expr qexpr)
                                     (Calculus.type_of_expr qexpr);
      Plan.ds_definition = qexpr
   }) !calc_queries in

      (* Compile things *)
      if Debug.active "LOG-ERROR-DETAIL" then (
         try 
            materialization_plan := Compiler.compile db_schema query_ds_list
         with Calculus.CalculusException(expr, msg) ->
            Debug.print "LOG-ERROR-DETAIL" (fun () ->
               msg^" while processing: \n"^
               (CalculusPrinter.string_of_expr expr)
            );
            failwith msg
      ) else (
         materialization_plan := Compiler.compile db_schema query_ds_list
      );
      

      (* And save the accessor expressions *)
      toplevel_queries := List.map (fun q_ds ->
         let (q_name, _, _, _, _) = Plan.expand_ds_name q_ds.ds_name in
            (q_name, q_ds.ds_name)
      ) query_ds_list
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
      m3_program := 
            Calculusparser.mapProgram Calculuslexer.tokenize lexbuff   
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
   dm_program := M3DM.make_DM_triggers (M3.get_triggers !m3_program);
   if (Debug.active "LOG-DM") then
   begin
      let k3_queries = M3ToK3.m3dm_to_k3 !m3_program !dm_program in
      Debug.print "LOG-DM" (fun () -> K3.nice_code_of_prog k3_queries)
   end
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
   Debug.activate "M3TOK3-GENERATE-INIT"; (* Temporary hack until we get M3DM set up *)
   
   k3_program := M3ToK3.m3_to_k3 !m3_program;
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
         Debug.print "PATTERNS" (fun () -> Patterns.patterns_to_string pats)
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
      k3_program := (
         db,
         (maps, patterns),
         List.map (fun (event, stmts) ->
            let trigger_vars = Schema.event_vars event in (
               event, 
               List.map (
                  K3Optimizer.optimize ~optimizations:!optimizations
                                       (List.map fst trigger_vars)
               ) stmts
            )
         ) triggers,
         tlqs
      )
   )
)
;;
if stage_is_active StagePrintK3 then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintK3");
   output_endline (K3.code_of_prog !k3_program)
)
;;
module K3InterpreterCG = K3Compiler.Make(K3Interpreter.K3CG)
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
         with K3Interpreter.InterpreterException(expr,msg) ->
            (begin match expr with 
               | Some(s) -> error ~detail:(Some(K3.string_of_expr s)) msg
               | None    -> error msg
            end)
               
      )   
      | Ocaml       -> bug "Ocaml codegen not implemented yet"
      | Scala       -> bug "Scala codegen not implemented yet"

         (* All imperative languages now produce IMP *)
      | IMP
      | CPP         -> 
         imperative_program := 
            ImperativeCompiler.Compiler.imp_of_k3 !imperative_opts 
                                                  !k3_program 

      | _ -> bug "Unexpected K3 target language"
)
;;
if stage_is_active StagePrintImp then (
   Debug.print "LOG-DRIVER" (fun () -> "Running Stage: PrintImp");
   let ((_,_,(imp_core, _)),_,_) = !imperative_program in
      List.iter (fun (_,(_,expr)) ->
         output_endline 
            ((ImperativeCompiler.CPPTarget.string_of_ext_imp expr)^"\n\n")
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
      let (maps,patterns,compiled) = !interpreter_program in
      let db = Database.NamedK3Database.make_empty_db maps patterns in
      let result = K3Interpreter.K3CG.eval compiled [] [] db in
      if result <> Values.K3Value.Unit then
         output_endline (  "UNEXPECTED RESULT: "^
                           (Values.K3Value.string_of_value result) )
   with K3Interpreter.InterpreterException(expr,msg) ->
      (begin match expr with 
         | Some(s) -> error ~detail:(Some(K3.string_of_expr s)) msg
         | None -> error msg
      end)
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
   (!compiler).ExternalCompiler.compile !output_file !binary_file
)
;;
