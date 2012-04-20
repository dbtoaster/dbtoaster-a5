open Types
open UnitTest
open Sql
open SqlClient
;;

Debug.activate "LOG-POSTGRES";;

let client = Postgres.create ();;
let bob_sch = (List.map var ["A"; "B"; "C"]);;

Postgres.create_table client ~temporary:true "BOB" bob_sch;;
Postgres.insert client "BOB" bob_sch [[(CInt(1));(CInt(2));(CInt(3))]];;
Postgres.query client (
   ["A", (Var(Some("BOB"), "A", TInt))],
   ["BOB",Table("BOB")],
   ConstB(true),
   []
);;