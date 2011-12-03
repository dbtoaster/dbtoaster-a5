open Calculus

exception DriverError of string

let error msg = raise (DriverError(msg))
let bug   msg = failwith ("BUG : "^msg)

;;

(************ Language Names ************)
type language_t =
   | Auto | SQL | Calc | MPlan | M3

let languages =
   [  "AUTO", (Auto , "automatic"     ); 
      "SQL",  (SQL  , "DBToaster SQL" );
      "CALC", (Calc , "DBToaster Relational Calculus");
      "PLAN", (MPlan, "Materialization Plan");
      "M3"  , (M3   , "M3 Program");
   ]

let input_language  = ref Auto
let output_language = ref Auto

let parse_language lang = 
   if List.mem_assoc (String.uppercase lang) languages then
      fst (List.assoc (String.uppercase lang) languages)
   else 
      raise (Arg.Bad("Unknown language "^lang))
;;

let (files:string list ref) = ref [];;

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
];;

Arg.parse specs (fun x -> files := !files @ [x]) 
          "dbtoaster [opts] file1 [file2 [...]]";;

(************ Stage Planning ************)
if !input_language == Auto 
   then input_language := SQL
;;
if !output_language = Auto
   then output_language := Calc
;;

type stage_t = 
   | StageParseSQL
   | StagePrintSQL
   | StageSQLToCalc
   | StagePrintCalc

let output_stages = [StagePrintSQL; StagePrintCalc]
let input_stages = [StageParseSQL]
let core_stages = [StageSQLToCalc]

let stages_from (stage:stage_t): stage_t list =
   ListExtras.sublist (ListExtras.index_of stage core_stages) (-1) core_stages
let stages_to   (stage:stage_t): stage_t list =
   ListExtras.sublist 0 ((ListExtras.index_of stage core_stages)+1) core_stages

let active_stages = ListAsSet.inter
   ((match !input_language with
      | Auto -> bug "input language still auto"
      | SQL  -> StageParseSQL::(stages_from StageSQLToCalc)
      | _    -> error "Unsupported input language"
    )@output_stages)
   ((match !output_language with
      | Auto -> bug "output language still auto"
      | SQL  -> StagePrintSQL::[]
      | Calc -> StagePrintCalc::(stages_to StageSQLToCalc)
      | _    -> error "Unsupported output language"
    )@input_stages)
;;
let stage_is_active s = List.mem s active_stages
;;

(************ Globals Used Across All Stages ************)
let sql_program :Sql.file_t ref  = ref Sql.empty_file;;
let db_schema   :DBSchema.t      = DBSchema.empty_db;;
let calc_queries:(string * BasicCalculus.expr_t) list ref
                                 = ref [];;

(************ SQL Stage ************)

if stage_is_active StageParseSQL then (
   List.iter (fun f ->
      let lexbuff = 
         Lexing.from_channel (if f <> "-" then (open_in f) else stdin) 
      in sql_program := Sql.merge_files !sql_program
            (Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff)
   ) !files
)
;;
if stage_is_active StagePrintSQL then (
   let (tables, queries) = !sql_program in
      print_endline (
         (ListExtras.string_of_list ~sep:"\n" Sql.string_of_table tables)^
         "\n\n"^
         (ListExtras.string_of_list ~sep:"\n" Sql.string_of_select queries)
      )
)
;;

(************ Calculus Stage ************)
let query_id = ref 0;;

if stage_is_active StageSQLToCalc then (
   let (tables, queries) = !sql_program in
      (* Convert the tables into a more friendly format that we'll use 
         throughout the rest of the program.  (Sql should be able to use this
         format directly.  TODO: Recode Sql to do so) *)
      SqlToCalculus.extract_sql_schema db_schema tables;
      
      (* Then convert the queries into calculus. Most of this code lives in
         ring/SqlToCalculus, but we still need to iterate over all of the
         queries that we were given, and do some target renaming to ensure that
         each of the root level queries has a unique name. *)
      List.iter (fun q -> 
         query_id := !query_id + 1;
         let query_name = "QUERY_"^(string_of_int !query_id)^"_" in
         (* Prepend QUERY_[#]_ to the target name *)
         List.iter (fun (tgt_name, tgt_calc) ->
            calc_queries := (query_name^tgt_name, tgt_calc)::!calc_queries
         ) (List.rev (SqlToCalculus.calc_of_query tables q))
         (* Reverse the order to use :: to build up the query list *)
      ) queries;
)
;;
if stage_is_active StagePrintCalc then (
   print_endline (
      (ListExtras.string_of_list ~sep:"\n" 
         (fun (name, calc) -> name^": \n  "^(BasicCalculus.string_of_expr calc))
         !calc_queries)
   )
)
;; 
