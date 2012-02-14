
module StringSet = Set.Make(String)

module DebugInternal =
struct
   let debug_modes = ref StringSet.empty;
end

let set_modes new_modes = DebugInternal.debug_modes := new_modes;;

let activate (mode:string): unit = 
   DebugInternal.debug_modes := 
      StringSet.add mode !DebugInternal.debug_modes;;
let deactivate (mode:string): unit = 
   DebugInternal.debug_modes := 
      StringSet.remove mode !DebugInternal.debug_modes;;

let exec (mode:string) (f:(unit->'a)): unit =
   if StringSet.mem mode !DebugInternal.debug_modes 
      then let _ = f () in () else ();;

let print (mode:string) (f:(unit->string)): unit =
   exec mode (fun () -> print_endline (f ()));;

let active df = StringSet.mem df !DebugInternal.debug_modes;;

let os () =
   let fdes = (Unix.open_process_in "uname") in
   let ostr = try input_line fdes with End_of_file -> "???" in
   let _ = (Unix.close_process_in fdes) in
      ostr

let showdiff exp_str fnd_str = 
   print_string ("--Expected--\n"^exp_str^
                 "\n\n--Result--\n"^fnd_str^"\n\n"); 
   if active "VISUAL-DIFF" then
      match (os ()) with
       | "Darwin" -> (
         let (exp,exp_fd) = Filename.open_temp_file "expected" ".diff" in
         let (fnd,fnd_fd) = Filename.open_temp_file "found" ".diff" in
            output_string exp_fd exp_str; close_out exp_fd;
            output_string fnd_fd fnd_str; close_out fnd_fd;
            let _ = Unix.system("opendiff "^exp^" "^fnd) in
            Unix.unlink exp;
            Unix.unlink fnd
         )
       | _ -> ()

let log_unit_test (title:string) (to_s:'a -> string) 
                  (result:'a) (expected:'a) : unit =
   if result = expected then print_endline (title^": Passed")
   else (
      print_endline (title^": Failed");
      showdiff (to_s expected) (to_s result);
      exit 1
   );;

let log_unit_test_list (title:string) (to_s:'a -> string) 
                       (result:'a list) (expected:'a list) : unit =
   if result = expected then print_endline (title^": Passed")
   else (
      let rec diff_lists rlist elist = 
         let recurse rstr estr new_rlist new_elist =
            let (new_rstr, new_estr) = diff_lists new_rlist new_elist in
               (rstr^"\n"^new_rstr,estr^"\n"^new_estr)
         in   
            if List.length rlist > 0 then
              if List.length elist > 0 then
                recurse (to_s (List.hd rlist)) (to_s (List.hd elist)) 
                        (List.tl rlist) (List.tl elist)
              else
                recurse (to_s (List.hd rlist)) "--empty line--" 
                        (List.tl rlist) []
            else
              if List.length elist > 0 then
                recurse "--empty line--" (to_s (List.hd elist)) 
                        [] (List.tl elist)
              else
                ("","")
     in
        let (result_diffs, expected_diffs) = diff_lists result expected in
           print_endline (title^": Failed");
           showdiff (expected_diffs) (result_diffs);
           exit 1
   );;
