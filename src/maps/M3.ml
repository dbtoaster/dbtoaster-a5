open Ring
open Arithmetic
open GlobalTypes

module rec IVCMeta : Calculus.ExternalMeta = struct
   type meta_t = IVCCalculus.expr_t option
   let string_of_meta meta = 
      begin match meta with 
         | Some(s) -> IVCCalculus.string_of_expr s
         | None -> ""
      end
end and IVCCalculus : Calculus.Calculus = Calculus.Make(IVCMeta)

open IVCCalculus

type map_t = (Datastructure.t * expr_t)

type stmt_type_t = UpdateStmt | ReplaceStmt
type stmt_t = Datastructure.t * stmt_type_t * expr_t 
type trigger_t = DBSchema.event_t * stmt_t list

type prog_t = 
   DBSchema.t *
   map_t list *
   trigger_t list 