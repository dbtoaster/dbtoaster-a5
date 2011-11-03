open Common.Types
open Util

type schema_t = (var_t * bool) list

type external_t = External of 
   string      (* The name of the map being referenced *)
 * ( calc_t       (* The map's defining expression *)
   * invocation_t (* The initializer for the map *)
 )

and calc_t = external_t Calculus.calc_t

and map_t =
   string            (* Name *)
 * ( schema_t          (* Map Schema w.r.t. the defining expression *)
   * calc_t            (* Defining expression *)
   * (  event_t 
      * map_op_t option (* LHS Initializer (if needed) *)
      * map_op_t list   (* Update Operations *)
     ) list
 )
 
and map_op_t = MapOp of schema_t * invocation_t

and invocation_t =
   calc_t            (* Defining expression *)
 * (  calc_t         (* One of several possible implementations of the defining
                        expression; Generally instantiated over several maps
                        expressed as externals within the invocation *)
   *  map_t ref list (* Maps used in the expression's definition *)
   ) list

let convert_calc (calc:'a Calculus.calc_t): calc_t =
   Calculus.convert_calc (fun (e_name,_,_,_) -> 
      failwith ("Encountered External while attempting to convert generic "^
                "calculus expression to IncrementalPlan Calculus.  I don't "^
                "know how to handle it -- bailing out.  Expression: \n"^
                (Calculus.string_of_calc calc)))
      calc
;;
let compile_map (name:string) (defn:calc_t): map_t =
   (  name,
      (  (Calculus.get_schema defn),
         defn,
         []
      )
   )
;;
let compile_invocation (defn:calc_t): invocation_t =
   (  defn,
      []
   )

let 
   rec string_of_map ?(indent="") ((name,(sch,defn,trigs)):map_t): string =
      (indent ^ name ^ (string_of_schema sch) ^ " : " ^
         (Calculus.string_of_calc defn) ^ 
         (string_of_list ""
            (List.map 
               (fun ((pm,rel),init,stmts) -> "\n  "^indent^"ON "^(
                     match pm with Insert -> "+" | Delete -> "-"
                  )^rel^": "^(
                     match init with None -> ""
                                   | Some(x) -> "\n"^(
                        string_of_map_op ~indent:("  "^indent) name ":=" x
                     )
                  )^
                  (string_of_list ""
                     (List.map (fun x -> "\n"^
                        (string_of_map_op ~indent:("  "^indent) name "+=" x))
                      stmts)
                  )
               ) trigs
            )
         )
      )
   
   and string_of_schema (sch:schema_t): string =
      ("[" ^
         (string_of_list ","
            (List.map (fun ((x,_),_)->x)
               (List.filter (fun (_,b) -> not b) sch))) ^ "][" ^
         (string_of_list ","
            (List.map (fun ((x,_),_)->x)
               (List.filter snd sch))) ^ "]"
      )
            
   and string_of_map_op ?(indent="") (map:string) (op:string) 
                        (defn:map_op_t): string =
      match defn with MapOp(sch, inv) ->
         (indent ^ map ^ (string_of_schema sch) ^ " " ^ op ^ "\n" ^
         (string_of_invocation ~indent:("  "^indent) inv))
      
   and string_of_invocation ?(indent="") ((defn,impls):invocation_t): string =
      indent ^ (Calculus.string_of_calc defn) ^ " impelemented as : " ^
      (string_of_list ""
         (List.map
            (fun (impl,maps) -> 
               "\n"^indent^(Calculus.string_of_calc defn) ^ 
               (string_of_list ""
                  (List.map
                     (fun x -> "\n"^(string_of_map ~indent:("  "^indent) !x))
                     maps
                  )
               )
            ) impls
         )
      )
      