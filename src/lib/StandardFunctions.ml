(**
   An internally library of reference implementations of External functions.  
   
   These functions are defined through the Functions module
*)

open Type
open Constants
open Functions
;;

(**
   Floating point division.  
    - [fp_div] num returns [1/(float)num]
    - [fp_div] a;b returns [(float)a / (float)b]
*)
let fp_div (arglist:const_t list) (ftype:type_t) =
   begin match arglist with
      | [v]     -> Constants.Math.div1 ftype v
      | [v1;v2] -> Constants.Math.div2 ftype v1 v2
      | _ -> invalid_args "fp_div" arglist ftype 
   end
;; declare_std_function "/" fp_div 
   (function | (([_;_] as x) | ([_] as x)) -> (escalate (TFloat::x))
             | _ -> inference_error ());;

(**
   Bounded fan-in list minimum.
    - [min_of_list] elems returns the smallest number in the list.
*)
let min_of_list (arglist:const_t list) (ftype:type_t) =
   let (start,cast_type) = 
      match ftype with TInt -> (CInt(max_int), TInt)
                     | TAny 
                     | TFloat -> (CFloat(max_float), TFloat)
                     | _ -> (invalid_args "listmin" arglist ftype, TAny)
   in List.fold_left min start (List.map (Constants.type_cast cast_type) 
                                         arglist)
;; declare_std_function "listmin" min_of_list (fun x -> escalate (TInt::x))

(**
   Bounded fan-in list maximum.
    - [max_of_list] elems returns the largest number in the list.
*)
let max_of_list (arglist:const_t list) (ftype:type_t) =
   let (start,cast_type) = 
      match ftype with TInt -> (CInt(min_int), TInt)
                     | TAny 
                     | TFloat -> (CFloat(min_float), TFloat)
                     | _ -> (invalid_args "listmax" arglist ftype, TAny)
   in List.fold_left max start (List.map (Constants.type_cast cast_type) 
                                         arglist)
;; declare_std_function "listmax" max_of_list (fun x -> escalate (TInt::x)) ;;

(**
   Date part extraction
    - [date_part] 'year';date returns the year of a date as an int
    - [date_part] 'month';date returns the month of a date as an int
    - [date_part] 'day';date returns the day of the month of a date as an int
*)
let date_part (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CString(part);  CDate(y,m,d)] -> 
         begin match String.uppercase part with
            | "YEAR"  -> CInt(y)
            | "MONTH" -> CInt(m)
            | "DAY"   -> CInt(d)
            | _       -> invalid_args "date_part" arglist ftype
         end
      | _ -> invalid_args "date_part" arglist ftype
;; declare_std_function "date_part" date_part 
               (function [TString; TDate] -> TInt | _-> inference_error ());;

(**
   Regular expression matching
   - [regexp_match] regex_str; str returns true if regex_str matches str
*)
let regexp_match (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CString(regexp); CString(str)] ->
         Debug.print "LOG-REGEXP" (fun () -> "/"^regexp^"/ =~ '"^str^"'");
         if Str.string_match (Str.regexp regexp) str 0
         then CInt(1) else CInt(0)
      | _ -> invalid_args "regexp_match" arglist ftype
;; declare_std_function "regexp_match" regexp_match
               (function [TString; TString] -> TInt | _-> inference_error ()) ;;

(**
   Substring
   - [substring] [str; start; len] returns the substring of str from 
     start to start+len
*)
let substring (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CString(str); CInt(start); CInt(len)] ->
            CString(String.sub str start len)
      | _ -> invalid_args "substring" arglist ftype
;; declare_std_function "substring" substring
         (function [TString; TInt; TInt] -> TString | _-> inference_error ()) ;;
  
type vector_t = float * float * float
  
let vec_of_list (v: float list): vector_t = 
  match v with 
    | [x; y; z] -> (x, y, z)
    | _ -> failwith "vector is not 3D!";;

let calc_vec_length (v1: vector_t): float = 
  let (x1, y1, z1) = v1 in
  sqrt(x1*.x1+.y1*.y1+.z1*.z1);;

let calc_vec_dot (v1: vector_t) (v2: vector_t): float = 
  let (x1, y1, z1) = v1 in
  let (x2, y2, z2) = v2 in
  x1*.x2+.y1*.y2+.z1*.z2;;

let calc_vec_cross (v1: vector_t) (v2: vector_t): vector_t =
  let (x1, y1, z1) = v1 in
  let (x2, y2, z2) = v2 in
  let x = (y1*.z2-.z1*.y2) in
  let y = (z1*.x2-.x1*.z2) in
  let z = (x1*.y2-.y1*.x2) in
  (x,y,z);;


let calc_dihedral_angle (inputs: float list): float =
  match inputs with 
    | [x1; y1; z1;
      x2; y2; z2; 
      x3; y3; z3; 
      x4; y4; z4] ->
      let v1_x = x2-.x1 in
      let v1_y = y2-.y1 in 
      let v1_z = z2-.z1 in 
      let v2_x = x3-.x2 in 
      let v2_y = y3-.y2 in 
      let v2_z = z3-.z2 in 
      let v3_x = x4-.x3 in 
      let v3_y = y4-.y3 in 
      let v3_z = z4-.z3 in 
      let v1 = (v1_x, v1_y, v1_z) in 
      let v2 = (v2_x, v2_y, v2_z) in 
      let v3 = (v3_x, v3_y, v3_z) in 
    
      let n1 = calc_vec_cross(v1)(v2) in 
      let n2 = calc_vec_cross(v2)(v3) in 
      atan2 (calc_vec_length(v2) *. calc_vec_dot(v1)(n2)) 
        (calc_vec_dot(n1)(n2))
    | _ -> failwith "incorrect inputs for dihedral_angle";;

let pi = 4.0 *. atan 1.0;;

let calc_radian (degree: float): float =
  (degree /. 180.0) *. pi

let radians (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CFloat(x)] ->
        CFloat(calc_radian(x))
      | [CInt(x)] ->
        CFloat(calc_radian(float_of_int x))
      | _ -> invalid_args "radians" arglist ftype
;; declare_std_function "radians" radians
(function [TFloat] -> TFloat | [TInt] -> TFloat | _-> inference_error ()) ;;

let pow_sig (arglist:const_t list) (ftype:type_t) =
   match arglist with
     | [CFloat(x); CFloat(y)] ->
       CFloat(x ** y)
     | [CInt(x); CInt(y)] ->
       CFloat((float_of_int x) ** (float_of_int y))
     | [CInt(x); CFloat(y)] ->
       CFloat((float_of_int x) ** (y))
     | [CFloat(x); CInt(y)] ->
       CFloat((x) ** (float_of_int y))
      | _ -> invalid_args "pow" arglist ftype
;; declare_std_function "pow" pow_sig
(function [TFloat; TFloat] -> TFloat 
  | [TInt; TInt] -> TFloat 
  | [TFloat; TInt] -> TFloat 
  | [TInt; TFloat] -> TFloat 
  | _-> inference_error ()) ;;

let vec_length (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CFloat(x); CFloat(y); CFloat(z)] ->
        CFloat(calc_vec_length(x, y, z))
      | [CInt(x); CInt(y); CInt(z)] ->
        let v = List.map float_of_int [x; y; z] in
        CFloat(calc_vec_length(vec_of_list v))
      | _ -> invalid_args "vec_length" arglist ftype
;; declare_std_function "vec_length" vec_length
         (function [TFloat; TFloat; TFloat] -> TFloat | _-> inference_error ()) ;;

let vec_dot (arglist:const_t list) (ftype:type_t) =
   match arglist with
     | [CFloat(x1); CFloat(y1); CFloat(z1);
       CFloat(x2); CFloat(y2); CFloat(z2)] ->
       let v1 = [x1; y1; z1] in
       let v2 = [x2; y2; z2] in
       CFloat(calc_vec_dot(vec_of_list v1)(vec_of_list v2))
     | [CInt(x1); CInt(y1); CInt(z1);
       CInt(x2); CInt(y2); CInt(z2)] ->
       let v1 = List.map float_of_int [x1; y1; z1] in
       let v2 = List.map float_of_int [x2; y2; z2] in
       CInt(int_of_float(calc_vec_dot(vec_of_list v1)(vec_of_list v2)))
      | _ -> invalid_args "vec_dot" arglist ftype
;; declare_std_function "vec_dot" vec_dot
(function 
  [TFloat; TFloat; TFloat;
    TFloat; TFloat; TFloat] ->
    TFloat | _-> inference_error ()) ;;


(* it had some problems with types! So I decided to comment it out!
let vec_cross (arglist:const_t list) (ftype:type_t) =
   match arglist with
     | [CFloat(x1); CFloat(y1); CFloat(z1);
       CFloat(x2); CFloat(y2); CFloat(z2)] ->
       let v1 = [x1; y1; z1] in
       let v2 = [x2; y2; z2] in
       let res = calc_vec_cross(v1)(v2) in
       List.map (fun x -> CFloat(x)) res
     | [CInt(x1); CInt(y1); CInt(z1);
       CInt(x2); CInt(y2); CInt(z2)] ->
       let v1 = List.map float [x1; y1; z1] in
       let v2 = List.map float [x2; y2; z2] in
       let res = calc_vec_cross(v1)(v2) in
       List.map (fun x -> CInt(int_of_float(x))) res
      | _ -> invalid_args "vec_cross" arglist ftype
;; declare_std_function "vec_cross" vec_cross
(function 
  [TFloat; TFloat; TFloat;
    TFloat; TFloat; TFloat] ->
    [TFloat; TFloat; TFloat] | _-> inference_error ()) ;;
 *)
  
(**TODO*)
let hash (arglist:const_t list) (ftype:type_t) =
   match arglist with
      | [CInt(x1)] ->
        CInt(x1)
      | _ -> invalid_args "hash" arglist ftype
;; declare_std_function "hash" hash
(function [TInt] -> TInt | _-> inference_error ()) ;;


let dihedral_angle (arglist:const_t list) (ftype:type_t) =
   match arglist with
     | [CFloat(x1); CFloat(y1); CFloat(z1);
       CFloat(x2); CFloat(y2); CFloat(z2);
       CFloat(x3); CFloat(y3); CFloat(z3);
       CFloat(x4); CFloat(y4); CFloat(z4)
       ] ->
       let inputs = [x1; y1; z1;
         x2; y2; z2;
         x3; y3; z3;
         x4; y4; z4] in
       CFloat(calc_dihedral_angle(inputs))
     | [CInt(x1); CInt(y1); CInt(z1);
       CInt(x2); CInt(y2); CInt(z2);
       CInt(x3); CInt(y3); CInt(z3);
       CInt(x4); CInt(y4); CInt(z4)
       ] ->
       let int_inputs = [x1; y1; z1;
         x2; y2; z2;
         x3; y3; z3;
         x4; y4; z4] in
       let inputs = List.map float_of_int int_inputs in
       CFloat(calc_dihedral_angle(inputs))
      | _ -> invalid_args "dihedral_angle" arglist ftype
;; declare_std_function "dihedral_angle" dihedral_angle
(function [TFloat; TFloat; TFloat;
  TFloat; TFloat; TFloat;
  TFloat; TFloat; TFloat;
  TFloat; TFloat; TFloat;] -> TFloat | _-> inference_error ()) ;;


(** 
   Type casting -- cast to a particular type
*)
let cast (arglist:const_t list) (ftype:type_t) = 
   let arg = match arglist with 
      [a] -> a | _ -> invalid_args "cast" arglist ftype 
   in
   begin try 
      begin match ftype with
         | TInt -> CInt(Constants.int_of_const arg)
         | TFloat -> CFloat(Constants.float_of_const arg)
         | TDate -> 
            begin match arg with
               | CDate _ -> arg
               | CString(s) -> parse_date s
               | _ -> invalid_args "cast" arglist ftype
            end
         | TString -> CString(Constants.string_of_const arg)
         | _ -> invalid_args "cast" arglist ftype
      end
   with Failure msg -> 
      raise (InvalidFunctionArguments("Error while casting to "^
                  (string_of_type ftype)^": "^msg))
   end
;; List.iter (fun (t, valid_in) ->
      declare_std_function ("cast_"^(Type.string_of_type t))
                           cast
                           (function [a] when List.mem a valid_in -> t
                                   | _ -> inference_error ())
   ) [
      TInt,     [TFloat; TInt; TString]; 
      TFloat,   [TFloat; TInt; TString]; 
      TDate,    [TDate; TString]; 
      TString,  [TInt; TFloat; TDate; TString; TBool];
     ]
;;