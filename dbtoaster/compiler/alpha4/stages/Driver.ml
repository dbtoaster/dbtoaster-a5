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
open Database
module DB         = NamedM3Database
module DBTRuntime = Runtime.Make(DB)


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
      (["-I"],
          ("INCLUDE_HDR", ParseArgs.ARG_LIST), "<path> [<path> [...]]",
          "Add a header include path for C++ compilation (equivalent to the DBT_HDR environment variable)");
      (["-L"],
          ("INCLUDE_LIB", ParseArgs.ARG_LIST), "<path> [<path> [...]]",
          "Add a library include path for C++ compilation (equivalent to the DBT_LIB environment variable)");
      (["-f";"--enable-optimization"],
          ("OPTFLAGS", ParseArgs.ARG_LIST), "<optimization flag> [<flag> [...]]", 
          "Enable K3 optimizations")
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
        "calc          DBToaster-style Relational Calculus\n"^
        "delta         DBToaster-style Relational Calculus Deltas\n"^
        "m3            Map Maintenance Message Code\n"^
        "m3:prep       Map Maintenance Message Code (prepared)\n"^
        "k3            Functional update triggers\n"^
        "k3:noopt      Functional update triggers without optimizations\n"^
        "k3:noopt      Functional update triggers with IR tags\n"^
        "ocaml         OCaml source file (via K3)\n"^
        "ocaml:m3      OCaml source file (via M3)\n"^
        "ocaml:k3      OCaml source file (via K3)\n"^
        "plsql         Postgres PLSQL source file\n"^
        "imp           Imperative update triggers\n"^
        "imp:desugar   Imperative update triggers (desugared for C++)\n"^
        "c++ | cpp     C++ source file\n"^
        "cpp:prof      C++ profiled source file\n"^
        "run           Run the query with the default interpreter (K3)\n"^
        "run:m3        Run the query with the M3 interpreter\n"^
        "run:k3        Run the query with the K3 interpreter (default run)\n"
      );
    exit 0
  )
else ();;

(********* EXTRACT/VALIDATE COMMAND LINE ARGUMENTS *********)

let input_files = if (flag_bool "FILES") then flag_vals "FILES" 
                  else give_up "No files provided";;
    
type ocaml_codegen_t = OCG_M3 | OCG_K3
type sql_language_t = SL_SQL | SL_PLSQL

type language_t = 
   | L_CALC
   | L_DELTA
   | L_M3 of bool (* prepared? *)
   | L_K3 of bool (* optimized? *)
   | L_K3IR
   | L_INTERPRETER of ocaml_codegen_t
   | L_OCAML of ocaml_codegen_t
   | L_IMP of bool (* desugar *)
   | L_CPP of bool (* profiled *) 
   | L_SQL of sql_language_t
   | L_NONE

let language = 
  if flag_bool "INTERPRETER" then L_INTERPRETER(OCG_K3)
  else
    match flag_val "LANG" with
    | None -> L_CPP(false)
    | Some(a) -> match String.uppercase a with
      | "OCAML"    -> L_OCAML(OCG_K3)
      | "OCAML:K3" -> L_OCAML(OCG_K3)
      | "OCAML:M3" -> L_OCAML(OCG_M3)
      | "C++"      -> L_CPP(false)
      | "CPP"      -> L_CPP(false)
      | "C++:prof" -> L_CPP(true)
      | "CPP:prof" -> L_CPP(true)
      | "CALCULUS" -> L_CALC
      | "CALC"     -> L_CALC
      | "M3"       -> L_M3(false)
      | "M3:PREP"  -> L_M3(true) 
      | "K3"       -> L_K3(true)
      | "K3:NOOPT" -> L_K3(false)
      | "K3:IR"    -> L_K3IR
      | "IMP"      -> L_IMP(false)
      | "IMP:DESUGAR" -> L_IMP(true)
      | "SQL"      -> L_SQL(SL_SQL) (* Translates DBT-SQL + sources -> SQL  *)
      | "PLSQL"    -> L_SQL(SL_PLSQL)
      | "RUN"      -> L_INTERPRETER(OCG_K3)
      | "RUN:M3"   -> L_INTERPRETER(OCG_M3)
      | "RUN:K3"   -> L_INTERPRETER(OCG_K3)
      | "RK3"      -> L_INTERPRETER(OCG_K3)
      | "NONE"     -> L_NONE (* Used for getting just debug output *)
      | "DEBUG"    -> L_NONE
      | "DELTA"    -> L_DELTA
      | "DELTAS"   -> L_DELTA
      | _          -> (give_up ("Unknown output language '"^a^"'"));;

let output_file = match flag_val "OUTPUT" with
  | None | Some("-") -> (fun () -> stdout)
  | Some(f)          -> ExternalCompiler.set_source_file f; 
                        (fun () -> open_out f);;

match flag_val "COMPILE" with
   | None    -> ()
   | Some(a) -> ExternalCompiler.compile_to_file a;;

List.iter (fun e ->
   ExternalCompiler.set_env e (flag_vals e)
) ["INCLUDE_HDR"; "INCLUDE_LIB"];;

let adaptor_dir = flag_val "RUN ADAPTORS";;

Debug.exec "ARGS" (fun () -> 
  StringMap.iter (fun k v ->
    print_endline (k^":");
    List.iter (fun o -> print_endline ("   "^o)) v
  ) arguments
);;

let compile_depth = 
  match flag_val "COMPILE_DEPTH" with
  | None -> None
  | Some("-") -> None
  | Some(i) -> Some(int_of_string i);;

(********* TRANSLATE SQL TO RELCALC *********)

Debug.print "LOG-DRIVER" (fun () -> "PARSING SQL");;

Debug.exec "PARSE" (fun () -> Parsing.set_trace true);;

let sql_file_to_calc f =
  let lexbuff = Lexing.from_channel (if f <> "-" then (open_in f) else stdin) in
  Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff;;

let (queries, sources) = 
  let (queries, sources) = 
    List.split (List.map sql_file_to_calc input_files)
  in (List.flatten queries, List.flatten sources);;

let query_list_to_calc_string qlist = 
  List.fold_left 
    (fun accum (query_exprs,_,qvars) ->
      accum^(List.fold_left (fun in_accum calc_term ->
        in_accum^"["^(String.concat "," (List.map fst qvars))^"]"^
        (Calculus.code_of_term (Calculus.make_term calc_term))^"\n"
      ) "" query_exprs)
    ) "" qlist;;

Debug.print "CALCULUS" (fun () -> (query_list_to_calc_string queries));;

if language == L_CALC then
  (
    output_string (output_file ()) (query_list_to_calc_string queries);
    exit 0
  )
else ();;

(********* PRODUCE SQL FOR COMPARISON *********)

if language == L_SQL(SL_SQL) then
  (
    failwith "Translation to normal SQL unsupported at the moment";
  )
else ();;

(********* IF NEEDED, PRODUCE SEMICOMPILED CALC *********)


Debug.print "LOG-DRIVER" (fun () -> "COMPILING DELTAS");;

if language == L_DELTA then
  (
    output_string (output_file ()) ((
      string_of_list "\n" (
        List.fold_left (fun accum (qlist,dbschema,qvars) ->
          List.fold_left (fun accum q ->
            (Compiler.compile 
             ~top_down_depth:compile_depth
              Calculus.ModeOpenDomain 
              dbschema
              (Calculus.make_term q,
              Calculus.map_term "QUERY" qvars)
              Compiler.generate_unit_test_code
              accum)
            ) accum qlist
          ) [] queries
    ))^"\n");
    exit 0
  )
else ();;

(********* RUN ADAPTORS ONLY IF REQUESTED ********************)

begin match adaptor_dir with
  | None -> ()
  | Some(f) ->
    let mkd f =
      try if not((Unix.stat f).Unix.st_kind = Unix.S_DIR) then
            failwith ("cannot output adaptor data to non-directory: "^f);
      with Unix.Unix_error _ -> Unix.mkdir f 0o755
    in
      try mkd f;
          Runtime.run_adaptors f sources;
          exit 0
      with Unix.Unix_error (_,_,_) ->
        failwith ("failed to create adaptor data output dir"^f)
end;;

(********* TRANSLATE RELCALC TO M3 *********)

let toplevel_queries = ref []

let calc_into_m3_inprogress qname (qlist,dbschema,qvars) m3ip = 
  fst
    (List.fold_left (fun (accum,sub_id) q ->
      ( 
        let subq_name = (qname^"_"^(string_of_int sub_id)) in
        (
          toplevel_queries := !toplevel_queries @ [subq_name];
          let map_ref =
             ( Calculus.make_term q,
               Calculus.map_term 
                   (qname^"_"^(string_of_int sub_id))
                   qvars
             ) in
          match compile_depth with
            | Some(i) when i <= 0 -> 
               CalcToM3.nonincremental
                  dbschema
                  map_ref
                  accum
            | _ -> 
                CalcToM3.compile
                   ~top_down_depth:compile_depth
                   dbschema
                   map_ref
                   accum
        )
      ), sub_id+1 ) (m3ip,1) qlist
    )
;;
Debug.print "LOG-DRIVER" (fun () -> "FINALIZING M3");;

let m3_prog = 
  let (m3_prog_in_prog,_) = 
    List.fold_left (fun (accum,id) q ->
      (calc_into_m3_inprogress ("QUERY_"^(string_of_int id)) q accum, id + 1)
    ) (CalcToM3.M3InProgress.init, 1) queries
  in
    CalcToM3.M3InProgress.finalize m3_prog_in_prog;;

Debug.print "M3" (fun () -> (M3Common.pretty_print_prog m3_prog));;

(********* Use the M3 in place if appropriate *********)

module M3OCamlCompiler = M3Compiler.Make(M3OCamlgen.CG);;
module M3OCamlInterpreterCompiler = M3Compiler.Make(M3Interpreter.CG);;

let dbschema = List.flatten (List.map (fun (_,x,_) -> x) queries)
;;

match language with
   | L_OCAML(OCG_M3) -> (
      let compile = 
         M3OCamlCompiler.compile_query 
            dbschema 
            (m3_prog, sources) 
            !toplevel_queries
      in
         ExternalCompiler.OCaml.compile_and_output compile;
         exit 0
      )
   | L_INTERPRETER(OCG_M3) -> (
         StandardAdaptors.initialize ();
         M3OCamlInterpreterCompiler.compile_query
            dbschema
            (m3_prog, sources)
            !toplevel_queries
            (GenericIO.O_FileDescriptor(output_file ()));
         exit 0
      )
   | L_M3(prepared) -> (
         output_string (output_file ()) (
            if prepared
            then
               let (sch,trigs) = m3_prog in
               let (ptrigs,pats) = M3Compiler.prepare_triggers trigs in
                  M3Common.PreparedPrinting.pretty_print_prog (sch,ptrigs)
            else
               M3Common.pretty_print_prog m3_prog
         );
         exit 0
      )
   | _ -> ()
;;

Debug.print "LOG-DRIVER" (fun () -> "BUILDING K3");;

(********* If we're up to this point, K3 is required *********)

let k3_prog = K3Builder.m3_to_k3 m3_prog;;

let output_k3_program (_,_,triggers) = 
   let fd = (output_file ()) in
      List.iter (fun (pm, rel, args, stmts) ->
         output_string fd
            ("\nON_"^(match pm with M3.Insert -> "insert"
                                  | M3.Delete -> "delete")^
             "_"^rel^"("^(Util.string_of_list "," args)^")\n");
         List.iter (fun (_,e) -> 
            output_string fd ((K3.SR.string_of_expr e)^"\n")
         ) stmts
      ) triggers
   
;;
if language = L_K3(false) then (
   output_k3_program k3_prog;
   exit 0
)
;;

(********* Apply K3 optimizations *********)

Debug.print "LOG-DRIVER" (fun () -> "OPTIMIZING K3");;

let k3_optimizations = 
  let parse_opt_flag s = match String.uppercase s with
     | "CSE" -> K3Optimizer.CSE
     | "BREDUCE" -> K3Optimizer.Beta
     | _ -> give_up ("invalid K3 optimization flag: "^s)
   in
   begin match (flag_vals "OPTFLAGS") with
    | [] -> [K3Optimizer.CSE; K3Optimizer.Beta]
    | x -> List.map parse_opt_flag x
   end
;;
let k3_opt_prog = 
   if Debug.active "NO-K3-OPT" then k3_prog
   else
      let (schema,patterns,k3_trigs) = k3_prog in
         (  schema, patterns, 
            List.map (fun (pm,rel,trig_args,stmtl) ->
             (pm, rel, trig_args,
               (List.map (fun (lhs,rhs) -> (
                           lhs, 
                           (K3Optimizer.optimize 
                              ~optimizations:k3_optimizations 
                              trig_args 
                              rhs)
                         ))
                         stmtl)
             )) k3_trigs
         )
;;

module K3OCamlCompiler = K3Compiler.Make(K3OCamlgen.K3CG);;
module K3PLSQLCompiler = K3Compiler.Make(K3Plsql.CG);;
module K3InterpreterCompiler = K3Compiler.Make(K3Interpreter.K3CG);;

match language with
   | L_K3(true) -> (
         output_k3_program k3_opt_prog;
         exit 0
      )
   | L_K3IR -> (
         let fd = (output_file ()) in
         let (_,_,triggers) = k3_opt_prog in
            output_string fd (string_of_list0 "\n\n" (fun (pm,rel,vars,stmts) ->
               "ON "^(if pm = M3.Insert then "+" else "-")^rel^
               (list_to_string (fun x->x) vars)^": \n"^
               (string_of_list0 "\n" (fun (_,stmt) ->
                  let string_of_meta meta = 
                     match meta with 
                        | ImpBuilder.Common.TypedSym(None,(tsym,_)) -> tsym
                        | ImpBuilder.Common.TypedSym(Some(sym),(tsym,_)) -> 
                                                                sym^";"^tsym
                  in
                  let ir = ImpBuilder.DirectIRBuilder.ir_of_expr stmt in
                     (ImpBuilder.AnnotatedK3.string_of_k3ir 
                        ~string_of_meta:string_of_meta ir)
               ) stmts)
            ) triggers);
            exit 0
      )
   | L_OCAML(OCG_K3) -> (
         let compile = 
            K3OCamlCompiler.compile_query
               dbschema
               (k3_opt_prog, sources)
               !toplevel_queries
         in
            ExternalCompiler.OCaml.compile_and_output compile;
            exit 0
      )
   | L_SQL(SL_PLSQL) -> (
         K3PLSQLCompiler.compile_query 
            dbschema
            (k3_opt_prog, sources)
            !toplevel_queries
            (GenericIO.O_FileDescriptor(output_file ()));
            exit 0
      )
         
   | L_INTERPRETER(OCG_K3) -> (
         StandardAdaptors.initialize ();
         let (_,_,trigs) = k3_opt_prog in
            List.iter (fun (evt,rel,args,stmts) ->
               List.iter (fun (_,stmt) ->
                  let t = K3Typechecker.typecheck_expr stmt in 
                     if t <> K3.SR.TUnit then (
                        output_string stderr (
                           "Trigger returns type '"^(K3.SR.string_of_type t)^
                           "': "^(K3.SR.string_of_expr stmt)
                        );
                        failwith "Typing error"
                     )
               ) stmts
            ) trigs;
         K3InterpreterCompiler.compile_query
            dbschema
            (k3_opt_prog, sources)
            !toplevel_queries
            (GenericIO.O_FileDescriptor(output_file ()));
            exit 0
      )
   | _ -> ()
;;

(********* If we're up to this point, IMP is required *********)

Debug.print "LOG-DRIVER" (fun () -> "BUILDING IMP");;

let ic_opts = {
   ImpCompiler.desugar = (if language = L_IMP(false) then false else true);
   ImpCompiler.profile = (if language = L_CPP(true)  then true  else false)
};;

let ic_prog = 
   let (schema,patterns,_) = k3_opt_prog in
   (  schema, patterns, 
      ImpCompiler.Compiler.compile_triggers
         ic_opts
         dbschema
         k3_opt_prog
   )
      
;;

Debug.print "LOG-DRIVER" (fun () -> "BUILDING C++ AND COMPILING");;

match language with
   | L_CPP(_) -> 
      let compile = ImpCompiler.Compiler.compile_imp
         ic_opts
         dbschema
         ic_prog
         sources
         !toplevel_queries
      in
         ExternalCompiler.CPP.compile_and_output compile
   | L_IMP(_) ->
      let (_,_,trigs) = ic_prog in
      let fd = (output_file ()) in
         List.iter (fun (_,(_,_,_,imp)) ->
            output_string fd ((ImpCompiler.CPPTarget.string_of_ext_imp imp)^
                              "\n\n")
         ) (fst trigs)
   | L_NONE -> ()
   | _ -> failwith ("Error: Asked to output unknown language: "^
                    match (flag_val "LANGUAGE") with None -> "[none]" 
                                                   | Some(a) -> a)

;;

