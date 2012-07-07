(**
   Utilities for invoking second-stage compilers.  The basic operation here is
   defined in [ExternalCompiler.t.compile], which takes the name of an input and
   an output file, and invokes the relevant compiler.
*)

(**/**)
module StringMap = Map.Make(String);;
(**/**)

(** The base directory in which all the binaries live (alpha5/bin) *)
let binary_base_dir = Filename.dirname (Array.get Sys.argv 0);;

(** Potentially applicable environment variables.  This includes library and
    include search paths. *)
let compile_env : string list StringMap.t ref = ref StringMap.empty;;

(** Raw flags to pass to the compiler (e.g., via the command line) *)
let compiler_flags  = ref [];;

(** 
   Set a specific environment variable 
   @param k The environment variable set
   @param v A list of values to set the value to
*)
let set_env k v         = compile_env := StringMap.add k v !compile_env;;

(** 
   Append to a specific environment variable 
   @param k The environment variable set
   @param v A value to append to the list
*)
let add_env k v         = 
   if StringMap.mem k !compile_env
   then compile_env := StringMap.add k (v::(StringMap.find k !compile_env))
                                     !compile_env
   else compile_env := StringMap.add k [v] !compile_env
;;

(**
   Set the inline compiler flags to the provided value
   @param f The new list of raw compiler flags
*)
let set_flags f         = compiler_flags := f;;

(**
   Append a set of flags to the raw flags to pass to the compiler
   @param switch optional A switch to prepend to the flag
   @param f      the flag to append
*)
let add_flag ?(switch="") f =
   compiler_flags := !compiler_flags @ 
      (if switch = "" then [f] else [switch; f])

(**
   The signature for an external compiler implementation
*)
type t = {
   (** The extension used by files read by this type of external compiler *)
   extension : string;

   (** Compile the provided input and output (respectively) files. *)
   compile   : string -> string -> unit
}

(**
   The External Ocaml compiler 
*)
let ocaml_compiler = {
   extension = ".ml" ;
   compile = (fun in_file_name out_file_name ->
      let ocaml_cc = "ocamlopt" in
      let ocaml_lib_ext = ".cmxa" in
      let dbt_lib_ext = ".cmx" in
      let ocaml_libs = [ "unix"; "str" ] in
      let dbt_lib_path = Filename.dirname binary_base_dir in
      let dbt_includes = [ "util"; "stages"; "stages/maps"; "lib/dbt_ocaml";
                           "stages/functional" ] in
      let dbt_libs = [ "util/Util";
                       "stages/maps/M3";
                       "stages/maps/M3Common";
                       "lib/ocaml/SliceableMap";
                       "lib/ocaml/Values";
                       "lib/ocaml/Database";
                       "lib/ocaml/Sources";
                       "lib/ocaml/StandardAdaptors";
                       "lib/ocaml/Runtime" ] in
      (* would nice to generate args dynamically off the makefile *)
      Unix.execvp ocaml_cc 
      ( Array.of_list (
        [ ocaml_cc; "-ccopt"; "-O3"; "-nodynlink"; ] @ (!compiler_flags) @
        (if Debug.active "COMPILE-WITH-GDB" then [ "-g" ] else []) @
        (List.flatten (List.map (fun x -> [ "-I" ; x ]) dbt_includes)) @
        (List.map (fun x -> x^ocaml_lib_ext) ocaml_libs) @
        (List.map (fun x -> dbt_lib_path^"/"^x^dbt_lib_ext) dbt_libs) @
        ["-" ; in_file_name ; "-o" ; out_file_name ]
      ))
   );
};;

(**
   The External C++ compiler 
*)
let cpp_compiler = {
   extension = ".hpp";
   compile = (fun in_file_name out_file_name ->
     let compile_flags  flag_name env_name =
        ( if StringMap.mem flag_name !compile_env
          then StringMap.find flag_name !compile_env
          else [] ) @
        ( try (Str.split (Str.regexp ":") (Unix.getenv env_name)) 
          with Not_found -> [] ) in
     let (main_fname, main_stream) = 
         Filename.open_temp_file ~mode:[Open_wronly; Open_trunc]
                                 "dbtoaster_main_"  ".cpp" in
     let cpp_cc = "g++" in
     let cpp_args = (
         cpp_cc ::
         [ main_fname; 
           "-include";in_file_name; 
           "-o"; out_file_name; ] @
         (if Debug.active "COMPILE-WITH-PROFILE" then ["-pg"] else []) @
         (if Debug.active "COMPILE-WITH-GDB" then ["-g"] else []) @
         (if Debug.active "COMPILE-WITHOUT-OPT" then [] else ["-O3"]) @
         (if Debug.active "COMPILE-WITH-STATIC" then ["-static"] else []) @
         [ "-I."; ] @
         (List.map (fun x->"-I"^x) (compile_flags "INCLUDE_HDR" "DBT_HDR")) @
         (List.map (fun x->"-L"^x) (compile_flags "INCLUDE_LIB" "DBT_LIB")) @
         (  (if Debug.active "MT" then
               List.map (fun x -> x^"-mt") else (fun x -> x))
            [ "-lboost_program_options";
              "-lboost_serialization";
              "-lboost_system";
              "-lboost_filesystem";
              "-lboost_chrono";
              "-lboost_thread";
            ]
         ) @ [
           "-lpthread";       
         ] @ (!compiler_flags)
      ) in
         Debug.print "LOG-GCC" (fun () -> (
            ListExtras.string_of_list ~sep:" " (fun x->x) cpp_args));
         at_exit (fun () -> Unix.unlink main_fname);
         output_string main_stream MainCpp.code;
         flush main_stream;
         Unix.execvp cpp_cc (Array.of_list cpp_args)
   )
};;

let scala_compiler = {
   extension = ".scala";
   compile = (fun in_file_name out_file_name ->
    let out = out_file_name ^ ".jar" in
    let scalac = "scalac" in

    let flags = [
      ""; "-deprecation";
      "-unchecked";
      "-sourcepath";"lib/dbt_scala/src";
      "-classpath";"lib/dbt_scala/dbtlib.jar";
      "-d"; out;
    ] @ 
    (if Debug.active "COMPILE-WITHOUT-OPT" then [] else ["-optimise"]); 
    in

    let sourcefiles = [
      "lib/dbt_scala/src/org/dbtoaster/RunQuery.scala";
      in_file_name
    ] in

    let args = flags @ sourcefiles in
    Debug.print "LOG-SCALA" (fun () -> (
      ListExtras.string_of_list ~sep:" " (fun x->x) args));
    if Sys.file_exists out then
      Sys.remove out;
    Unix.execvp scalac (Array.of_list args)
   )
};;

(**
   A dummy "compiler" that will error if you try to compile something with it.
*)
let null_compiler = {
   extension = ".?";
   compile   = (fun _ _ -> failwith "Invalid external compiler")
}
