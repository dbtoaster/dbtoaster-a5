
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
      "lang  Set the compiler's output language to lang");
   (  "-i",
      (Arg.String(fun x -> input_language := parse_language x)),
      "lang  Set the compiler's input language to lang");
   (  "-d",
      (Arg.String(Debug.activate)),
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
let core_stages = [StageParseSQL; StageSQLToCalc]

let stages_from (stage:stage_t): stage_t list =
   ListExtras.sublist (ListExtras.index_of stage core_stages) (-1) core_stages
let stages_to   (stage:stage_t): stage_t list =
   ListExtras.sublist 0 ((ListExtras.index_of stage core_stages)+1) core_stages

let active_stages = ListAsSet.inter
   ((match !input_language with
      | Auto -> bug "input language still auto"
      | SQL  -> core_stages
      | _    -> error "Unsupported input language"
    )@output_stages)
   (match !output_language with
      | Auto -> bug "output language still auto"
      | SQL  -> StagePrintSQL::(stages_to StageParseSQL)
      | Calc -> StagePrintCalc::(stages_to StageSQLToCalc)
      | _    -> error "Unsupported output language")

;;

(************ SQL Stage ************)

let parsed_sql = ref Sql.empty_file
;;
if List.mem StageParseSQL active_stages then (
   List.iter (fun f ->
      let lexbuff = 
         Lexing.from_channel (if f <> "-" then (open_in f) else stdin) 
      in parsed_sql := Sql.merge_files !parsed_sql
            (Sqlparser.dbtoasterSqlList Sqllexer.tokenize lexbuff)
   ) !files
)
;;
if List.mem StagePrintSQL active_stages then (
   let (tables, queries) = !parsed_sql in
      print_endline (
         (ListExtras.string_of_list ~sep:"\n" Sql.string_of_table tables)^
         "\n\n"^
         (ListExtras.string_of_list ~sep:"\n" Sql.string_of_select queries)
      )
)

