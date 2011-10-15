open Util


let binary_base_dir = Array.get Sys.argv 0;;
let should_compile  = ref false;;
let compile_fname   = ref "";;
let output_fname    = ref None;;
let compile_env : string list StringMap.t ref = ref StringMap.empty;;
let compiler_flags  = ref [];;

let compile_to_file   s = should_compile := true; compile_fname := s;;

let set_source_file   s = output_fname := Some(s);;

let set_env k v         = compile_env := StringMap.add k v !compile_env;;

let set_flags f         = compiler_flags := f;;

module type EC_Base = sig
   val extension : string
   val compile : string -> unit
end

module EC_Ocaml_Base : EC_Base = struct
   let extension = ".ml"
   let compile in_file_name =
      let ocaml_cc = "ocamlopt" in
      let ocaml_lib_ext = ".cmxa" in
      let dbt_lib_ext = ".cmx" in
      let ocaml_libs = [ "unix"; "str" ] in
      let dbt_lib_path = Filename.dirname binary_base_dir in
      let dbt_includes = [ "util"; "stages"; "stages/maps"; "lib/ocaml";
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
        ["-" ; in_file_name ; "-o" ; !compile_fname ]
      ));;
end

module EC_CPP_Base : EC_Base = struct
   let extension = ".cpp"
   let compile in_file_name =
     let compile_flags  flag_name env_name =
        ( StringMap.find flag_name !compile_env ) @
        ( try (Str.split (Str.regexp ":") (Unix.getenv env_name)) 
          with Not_found -> [] ) in
     let cpp_cc = "g++" in
     let cpp_args = (
         cpp_cc ::
         (List.map (fun x->"-I"^x) (compile_flags "INCLUDE_HDR" "DBT_HDR")) @
         (List.map (fun x->"-L"^x) (compile_flags "INCLUDE_LIB" "DBT_LIB")) @
         (if Debug.active "COMPILE-WITH-PROFILE" then ["-pg"] else []) @
         (if Debug.active "COMPILE-WITH-GDB" then ["-g"] else []) @
         (if Debug.active "COMPILE-WITHOUT-OPT" then [] else ["-O3"]) @
         [ "-I"; "." ;
           "-lboost_program_options";
           "-lboost_serialization";
           "-lboost_system";
           "-lboost_filesystem";
           "-lboost_chrono";
           "-DFUSION_MAX_VECTOR_SIZE=50";
           in_file_name ;
           "-o"; !compile_fname
         ] @ (!compiler_flags)
      ) in
         Debug.print "LOG-GCC" (fun () -> (string_of_list " " cpp_args));
         Unix.execvp cpp_cc (Array.of_list cpp_args)
   ;;

end

module Make(Compiler : EC_Base) =
   struct
      let compile_via_tmp (dbt_compile: GenericIO.out_t -> unit) =
         dbt_compile (GenericIO.O_TempFile(
            "dbtoaster_",
            Compiler.extension,
            Compiler.compile
         ))
      ;;
      let compile_via_output (dbt_compile: GenericIO.out_t -> unit) =
         match !output_fname with
           | None      -> compile_via_tmp dbt_compile
           | Some("-") -> compile_via_tmp dbt_compile
           | Some(a)   -> Compiler.compile a
      ;;
      let compile_and_output (dbt_compile: GenericIO.out_t -> unit) =
         (if (!output_fname <> None)||(not !should_compile) then 
            dbt_compile (GenericIO.O_FileDescriptor(
               match !output_fname with
                  | None      -> stdout
                  | Some("-") -> stdout
                  | Some(a)   -> open_out a
            )));
         if !should_compile then compile_via_output dbt_compile
   end

module OCaml = Make(EC_Ocaml_Base);;
module CPP = Make(EC_CPP_Base);;
