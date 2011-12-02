open Ring
open Arithmetic
open GlobalTypes
open Calculus.BasicCalculus

type work_queue_entry_t = (Datastructure.t * expr_t)
type compiled_datastructure_t =
   (Datastructure.t * expr_t) *
   (DBSchema.event_t * M3.stmt_t) list

type plan_t =
   work_queue_entry_t list ref *
   compiled_datastructure_t list ref


