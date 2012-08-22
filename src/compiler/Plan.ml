(**
   A plan is the third representation in the compiler pipeline 
   (SQL -> Calc -> Plan)
   
   Each plan consists of a set of compiled datastructures, which consist of 
   {ul
      {- A datastructure description, including a name and a definition of the
         the query which the datastructure is responsible for maintaining}
      {- A set of triggers which are used to maintain the datastructure.}
   }

   @author Oliver Kennedy
*)

open Calculus

(******************* Datastructures *******************)
(** A datastructure description *)
type ds_t = {
   ds_name       : expr_t; (** The name of the datastructure.  The content of
                               this expr_t must always be a single external leaf
                               and can always be simply (assuming its schema
                               matches) dropped into an existing expression in
                               order to access this datastructure.  
                               {b mk_ds_name} and {b expand_ds_name} are 
                               utility methods for interacting with ds_names *)
   
   ds_definition : expr_t  (** The definition of the datastructure.  This is the
                               query that the map will be maintaining the result
                               of *)
}

(** Construct a ds_name for a ds_t *)
let mk_ds_name ?(ivc = None) (name:string) (schema:schema_t) (t:Type.type_t)=
   Calculus.mk_external name (fst schema) (snd schema) t ivc

(** Extract the name, schema, type, and ivc code from a ds_name *)
let expand_ds_name (name:expr_t) =
   begin match name with
      | CalcRing.Val(External(e)) -> e
      | _ -> failwith "Error: invalid datastructure name"
   end

(** Stringify a datastructure description.  The string conforms to the grammar 
    of Calculusparser *)
let string_of_ds (ds:ds_t): string =
   (string_of_expr ds.ds_name)^" := "^(string_of_expr ds.ds_definition)

(******************* Statements *******************)

(** A statement can either update (increment) the existing value of the key that
    it is writing to, or replace the value of the key that it is writing to 
    entirely *)
type stmt_type_t = UpdateStmt | ReplaceStmt

(** A statement which alters the contents of a datastructure when executed *)
type stmt_t = {
   target_map  : expr_t;      (** The datastructure to be modified *)
   update_type : stmt_type_t; (** The type of alteration to be performed *)
   update_expr   : expr_t     (** The calculus expression defining the new 
                                  value or update *)
}

(** Stringify a statement.  This string conforms to the grammar of 
    Calculusparser *)
let string_of_statement (stmt:stmt_t): string = 
   let expr_string = (CalculusPrinter.string_of_expr (stmt.update_expr)) in
   (string_of_expr (stmt.target_map))^
   (if stmt.update_type = UpdateStmt
      then " += "
      else " := ")^
   (if String.contains expr_string '\n' then "\n  " else "")^
   expr_string
   

(******************* Compiled Datastructures *******************)
(** A compiled datastructure *)
type compiled_ds_t = {
   description : ds_t;
   ds_triggers : (Schema.event_t * stmt_t) list
}

(** An incremental view maintenance plan (produced by Compiler) *)
type plan_t = compiled_ds_t list

