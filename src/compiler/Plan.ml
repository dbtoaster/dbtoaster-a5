open Arithmetic
open Types
open Calculus

module C = BasicCalculus

(*************** Datastructures ***************)
type ds_meta_t = {
   init_at_start:    bool,
   init_on_access:   bool
}

type ds_ref_t = C.expr_t
type ds_t     = ref_t * C.expr_t

let info_of_ds_ref (r:ds_ref_t): C.external_meta_t external_t = 
   begin match r with
      | C.CalcRing.Value(External(einfo)) -> einfo
      | _ -> failwith "Attempt to dereference non-referential expression"
   end

let name_of_ds_ref (r:ds_ref_t): string =
   let (name,_,_,_,_) = info_of_ds_ref r in name

let schema_of_ds_ref (r:ds_ref_t): (var_t list * var_t list) =
   let (_,ivars,ovars,_,_) = info_of_ds_ref r in (ivars,ovars)

let mk_ds_ref (name:string) (ivars:var_t list) (ovars:var_t list) 
              (dstype:type_t): ds_ref_t =
   C.CalcRing.mk_val (External(name, ivars, ovars, dstype, ()))

(*************** Statements ***************)
type stmt_type_t = UpdateStmt | ReplaceStmt
type stmt_t      = ds_ref_t * stmt_type_t * C.expr_t
type trigger_t   = (Schema.event_t * stmt_t)

(*************** Plan ***************)
type compiled_ds_t     = {
   datastructure : ds_t * 
   init_expr     : C.expr_t
   triggers      : trigger_t list
   meta          : ds_meta_t
}
type plan_t            = compiled_ds_t list
