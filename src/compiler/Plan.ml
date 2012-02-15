open Calculus

(******************* Datastructures *******************)

type ds_t = {
   ds_name       : expr_t;
   ds_definition : expr_t
}

let mk_ds_name ?(ivc = None) (name:string) (schema:schema_t) (t:Types.type_t)=
   CalcRing.mk_val (External(name, (fst schema), (snd schema), t, ivc))

let expand_ds_name (name:expr_t) =
   begin match name with
      | CalcRing.Val(External(e)) -> e
      | _ -> failwith "Error: invalid datastructure name"
   end

(******************* Statements *******************)

type stmt_type_t = UpdateStmt | ReplaceStmt

type stmt_t = {
   target_map  : expr_t;
   update_type : stmt_type_t;
   update_expr   : expr_t
}

let string_of_ds (ds:ds_t): string =
   (string_of_expr ds.ds_name)^" := "^(string_of_expr ds.ds_definition)

let string_of_statement (stmt:stmt_t): string = 
   (string_of_expr (stmt.target_map))^
   (if stmt.update_type = UpdateStmt
      then " += "
      else " := ")^
   (string_of_expr (stmt.update_expr))

(******************* Compiled Datastructures *******************)
type ds_metadata_t = {
   init_at_start  : bool;
   init_on_access : bool
}
type compiled_ds_t = {
   description : ds_t;
   metadata    : ds_metadata_t;
   ds_triggers : (Schema.event_t * stmt_t) list
}
type plan_t = compiled_ds_t list

