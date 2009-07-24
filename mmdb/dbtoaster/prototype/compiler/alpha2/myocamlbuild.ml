open Ocamlbuild_pack;;
open Ocamlbuild_plugin;;

let ocamlfind_query pkg =
    let cmd = Printf.sprintf "ocamlfind query %s" (Filename.quote pkg) in
        My_unix.run_and_open cmd (fun ic ->
            Log.dprintf 5 "Getting Ocaml directory from command %s" cmd;
            input_line ic)
;;

dispatch
    begin function
        | After_rules ->
              ocaml_lib ~extern:true ~dir:(ocamlfind_query "xml-light") "xml-light";

        | _ -> ()
    end
;;
