open Ring
open Arithmetic
open Types
open Calculus
open Statement

type todo_ds_t           = BasicCalculus.expr_t ds_t
type plan_t              = unmaterialized_ds_t list

type materialize_t       = BasicCalculus.expr_t -> 
                              unmaterialized_ds_t list * 
                              BasicCalculus.expr_t

let compile_map (materialize:materialize_t)
                (map_name:string) (ivars:var_t list) (ovars:var_t list)
                (map_type:type_t) (map_defn:BasicCalculus.expr_t): 
                BasicCalculus.expr_t ds_t list * unmaterialized_ds_t =
   ([], ((("foo", (Datastructure.TMap([],[],TInt))), BasicCalculus.CalcRing.one), []))
   


let materialize_expr (known_dses:todo_ds_t list)
                     (prefix_name:string)
                     (expr:BasicCalculus.expr_t): 
                     unmaterialized_ds_t list * BasicCalculus.expr_t =
   ([], BasicCalculus.CalcRing.one)

let compile (queries:(Datastructure.t * BasicCalculus.expr_t) list):plan_t =
   let todos:todo_ds_t list ref     = ref queries in
   let completed:todo_ds_t list ref = ref [] in
   let plan:plan_t ref              = ref [] in
   while List.length !todos > 0 do (
      let next_ds = List.hd !todos in todos := List.tl !todos;
      let new_todos, compiled_ds =
         begin match next_ds with
         | ((map_name, Datastructure.TMap(ivars, ovars, map_type)), map_defn) ->
            compile_map (materialize_expr (!todos@ !completed) map_name)
                        map_name ivars ovars map_type map_defn
         end
      in
      completed := next_ds :: !completed;
      todos     := !todos @ new_todos;
      plan      := compiled_ds :: !plan
   ) done; List.rev !plan