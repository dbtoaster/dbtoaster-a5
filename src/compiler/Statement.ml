open Ring
open Arithmetic
open GlobalTypes
open Calculus

type stmt_type_t = UpdateStmt | ReplaceStmt
type 'expr_t generic_stmt_t = Datastructure.t * stmt_type_t * 'expr_t

type stmt_t = IVCCalculus.expr_t generic_stmt_t

type 'expr_t ds_t = (Datastructure.t * 'expr_t)
type 'expr_t trigger_t = (DBSchema.event_t * 'expr_t generic_stmt_t)

type 'expr_t compiled_ds_t = 'expr_t ds_t * 'expr_t trigger_t list

type todo_ds_t           = BasicCalculus.expr_t ds_t
type unmaterialized_ds_t = BasicCalculus.expr_t compiled_ds_t
type materialized_ds_t   = IVCCalculus.expr_t compiled_ds_t

let upgrade_calc (ivc_fn:string -> IVCCalculus.external_meta_t):
                 BasicCalculus.expr_t -> IVCCalculus.expr_t =
   BasicToIVC.translate (fun (en,_,_,_,_) -> ivc_fn en) 