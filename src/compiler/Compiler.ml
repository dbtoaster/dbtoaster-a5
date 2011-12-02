open Ring
open Arithmetic
open GlobalTypes
open Calculus
open Statement

type plan_t = todo_ds_t list ref * materialized_ds_t list ref

(*
let compile_datastructure (rels: DBSchema.rel_t list)
                          (((ds_name,ds_type), ds_expr):todo_ds_t): 
                          unmaterialized_ds_t =
   begin match ds_type with
      | Datastructure.MapT(ivars,ovars,_) -> 
         Datastructure.MapDS.compile m
         let ds_rel_names = BasicCalculus.rels_of_expr ds_expr in
         let ds_rels = 
            List.filter (fun (n,_,_) -> List.mem n ds_rel_names) rels 
         in
            
   end

*)