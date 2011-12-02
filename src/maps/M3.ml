open Ring
open Arithmetic
open GlobalTypes
open Statement
open Calculus

type prog_t = 
   DBSchema.t *
   IVCCalculus.expr_t ds_t list *
   IVCCalculus.expr_t trigger_t list 