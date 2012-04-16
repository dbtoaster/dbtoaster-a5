(**
   Functionality for managing debugging statements and 'hidden' functionality in  
   DBToaster.  Accessed through dbtoaster's -d flag.
   
   The debug module keeps track of a list of globally 'active' debug modes.  
   Each mode is identified in the string, and referenced within the code via the 
   Debug.exec, Debug.print, and Debug.active functions.  
   
   Debug modes can be explicitly managed in the code using Debug.activate or 
   Debug.deactivate.  Users can activate debug modes in the dbtoaster binary by
   passing -d [mode name].  A list of available Debug modes is being maintained 
   on the Assembla page.
*)

type debug_mode_t = string
;;
(**/**)
module StringSet = Set.Make(String)

module DebugInternal =
struct
   let debug_modes = ref StringSet.empty;
end
(**/**)

(** 
   Overwrite the list of active debug modes.
   @param new_modes The new list of active debug modes.
*)
let set_modes new_modes = DebugInternal.debug_modes := new_modes;;

(**
   Activate the indicated debug mode
   @param mode The name of the mode to activate
*)
let activate (mode:debug_mode_t): unit = 
   DebugInternal.debug_modes := 
      StringSet.add mode !DebugInternal.debug_modes;;
(**
   Deactivate the indicated debug mode
   @param mode The name of the mode to deactivate
*)
let deactivate (mode:debug_mode_t): unit = 
   DebugInternal.debug_modes := 
      StringSet.remove mode !DebugInternal.debug_modes;;

(**
   Execute a ( unit -> * ) function when the indicated debug mode is active
   @param mode The triggering mode
   @param f    The function to evaluate if the triggering mode is active
*)
let exec (mode:debug_mode_t) (f:(unit->'a)): unit =
   if StringSet.mem mode !DebugInternal.debug_modes 
      then let _ = f () in () else ();;

(**
   Print a string when the indicated debug mode is active.  The string should be 
   encapsulated in a generator function of type ( unit -> string )
   @param mode The triggering mode
   @param f    The string-generating function
*)
let print (mode:debug_mode_t) (f:(unit->string)): unit =
   exec mode (fun () -> print_endline (f ()));;

(**
   Determine whether the indicated debug mode is active
   @param mode The mode to be tested
   @return     True if the indicated mode is active
*)
let active (mode:debug_mode_t) = StringSet.mem mode !DebugInternal.debug_modes;;

(**
   Determine the operating system on which we are running
   @return A string describing the operating system (the output of unmame or ??? 
           if uname does not exist
*)
let os () =
   let fdes = (Unix.open_process_in "uname") in
   let ostr = try input_line fdes with End_of_file -> "???" in
   let _ = (Unix.close_process_in fdes) in
      ostr

