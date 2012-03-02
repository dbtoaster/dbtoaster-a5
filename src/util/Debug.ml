
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

