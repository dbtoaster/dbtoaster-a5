open UnitTest
open Database
open Patterns
;;
let maps = [
   "A", [], [];
   "B", [], [TInt; TInt];
]
;;
let patterns = [
   "A", [];
   "B", [In(["X"], [0])];
]
;;
let db = NamedK3Database.make_empty_db maps patterns
;;
let test msg 