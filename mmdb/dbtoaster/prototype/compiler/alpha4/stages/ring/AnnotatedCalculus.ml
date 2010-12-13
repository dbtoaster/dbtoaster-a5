open Util
open Calculus

module PipedCalculus = struct
   type piped_term_data_t = 
      | Expression
      | BigSum of var_t list
   
   type piped_term_t = 
      var_t list *      (* All variables = Input + Output, ordered*)
      var_t list *      (* Input variables *)
      term_t *          (* Term definition; the expression defining the term *)
      piped_term list * (* Subterm definitions *)
      piped_term_data   (* Contents *)
   
   type map_term_t = (string * piped_term_t)
   
   ;;
   
   let make_term (term:Calculus.term_t) (defn:Calculus.term_t) 
                 (ivars:Calculus.var_t list): piped_term_t =
      let (map_name,map_vars) = decode_map_term term in
         (  map_name, 
            map_vars,
            ivars,
            defn,
            [],
            Expression
         )
   
   ;;
   
   let convert_bigsum (term:Calculus.term_t) (defn:Calculus.term_t) 
                      (ivars:Calculus.var_t list) 
                      (theta:Calculus.term_mapping_t)
                      (bs_vars:Calculus.var_t list): piped_term_t =
      let (map_name,map_vars) = decode_map_term term in
      if (match defn with AggSum(_,_) -> false | _ -> true) then
         failwith ("PipedCalculus: Invalid term in bigsum: "^
                   (Calculus.term_to_string defn));
      else
         (  map_name,
            map_vars,
            ivars,
            defn,
            theta,
            (BigSum(bs_vars))
         )
   
   ;;
   
   let get_definition ((_,_,_,_,term,_,_):piped_term_t):term_t = 
      term
   let get_map_term ((map_name,map_vars,_,_,_,_):piped_term_t):term_t = 
      (map_term map_name map_vars)
   let get_vars ((_,map_vars,_,_,_,_):piped_term_t):var_t list =
      map_vars
   let get_ivars ((_,_,ivars,_,_,_):piped_term_t):var_t list = 
      ivars
   let get_ovars (pterm:piped_term_t):var_t list =  
      (ListAsSet.diff (get_vars pterm) (get_ivars pterm))
   let get_map_name ((map_name,_,_,_,_,_):piped_term_t):string =
      map_name
   let get_subterms ((_,_,_,_,subterms,_):piped_term_t):string =
      subterms
   let get_subterm (pterm:piped_term_t) (t:string):piped_term_t =
      List.find (fun (map_name,_,_,_,_,_) -> map_name = t) (get_subterms pterm)
   
   ;;
   
   let get_internals ((_,_,_,_,_,idata):piped_term_t):piped_term_data_t =
      idata
   let get_subterm_mappings (pterm:piped_term_t): term_mapping_t =
      (List.map (fun t -> (get_definition t, get_map_term t)) 
                (get_subterms pterm))
   
   ;;
end

(**************************************************************************)

module DeltaCalculus = struct
   
   type delta_term_t = 
      bool * 
      string *
      Calculus.var_t list *
      string *
      Calculus.var_t list
      Calculus.var_t 
   
   
   let simplified_delta_terms (delete:bool) (relname:string) (tuple:var_t list)
                              (pterm:piped_term_t): (delta_term_t list) =
      let ((delta_calc:term_t list),(new_bs_vars:var_t list)) = 
         List.split 
            (Calculus.simplify 
               (Calculus.term_delta 
                  (get_subterm_mappings pterm)
                  delete
                  relname
                  tuple
                  (get_definition pterm)
               )
               tuple
               (match (get_internals pterm) with | External -> [] | Bigsum(v) -> v)
               (get_vars pterm)
            )
      in
         (List.map
end

