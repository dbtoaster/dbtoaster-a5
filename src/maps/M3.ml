open Ring
open Arithmetic
open Types
open Statement
open Calculus

type prog_t = 
   Schema.t *
   IVCCalculus.expr_t ds_t list *
   IVCCalculus.expr_t trigger_t list 