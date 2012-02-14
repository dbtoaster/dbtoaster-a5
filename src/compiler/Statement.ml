
module C = Calculus

type ds_t = {
   ds_name       : C.expr_t;
   ds_definition : C.expr_t
}

let mk_ds_name ?(ivc = None) (name:string) (schema:C.schema_t) (t:Types.type_t)=
   C.CalcRing.mk_val (C.External(name, (fst schema), (snd schema), t, ivc))

let expand_ds_name (name:C.expr_t) =
   begin match name with
      | C.CalcRing.Val(C.External(e)) -> e
      | _ -> failwith "Error: invalid datastructure name"
   end

type stmt_type_t = UpdateStmt | ReplaceStmt

type stmt_t = {
   target_map  : C.expr_t;
   update_type : stmt_type_t;
   update_expr   : C.expr_t
}

let string_of_ds (ds:ds_t): string =
   (C.string_of_expr ds.ds_name)^" := "^(C.string_of_expr ds.ds_definition)

let string_of_statement (stmt:stmt_t): string = 
   (C.string_of_expr (stmt.target_map))^
   (if stmt.update_type = UpdateStmt
      then " += "
      else " := ")^
   (C.string_of_expr (stmt.update_expr))
