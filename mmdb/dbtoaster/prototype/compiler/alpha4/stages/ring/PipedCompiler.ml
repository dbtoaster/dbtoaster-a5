open Calculus
open PipedCalculus

let rec compile ?(compile_deletes = true)
                ?(bs_rewrite_mode = Calculus.ModeOpenDomain)
                (schema:(string * var_t list) list)
                (pterm:piped_term_t): unit = 
   (bigsum_rewrite ~rewrite_mode:bs_rewrite_mode pterm);
   (build_deltas ~compile_deletes:compile_deletes schema pterm);
   (build_subterms_for_deltas pterm);
   (List.iter 
      (compile ~compile_deletes:compile_deletes 
               ~bs_rewrite_mode:bs_rewrite_mode
               schema)
      (get_subterms pterm)
   )

let generate_unit_test_code = string_of_piped_term