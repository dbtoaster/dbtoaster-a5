let indent_width = 4
let stmt_delimiter = ";"
let tab = String.make indent_width ' '

type source_code_id_t = string
type source_code_op_t = string
type source_code_t = Lines of string list | Inline of string

let string_of_source_code bc =
  begin match bc with
  | Lines l -> String.concat "\n" l
  | Inline i -> i
  end

let concat_source_code a b =
  begin match a,b with
  | Lines l, Lines l2 -> Lines(l@l2)
  | Lines l, Inline i -> Lines(l@[i])
  | Inline i, Lines l -> Lines(i::l)
  | Inline i, Inline i2 -> Inline(i^i2)  
  end

let concat_many_source_code =
  List.fold_left concat_source_code 

let empty_source_code bc = bc = Lines([]) || bc = Inline("")

let delim_source_code ?(preserve=false) delim bc =
  begin match bc with
    | Lines (l) ->
      let x = List.rev l in
      Lines(List.rev (((List.hd x)^delim)::(List.tl x)))
    | Inline (i) ->
      if delim = stmt_delimiter && (not preserve) then Lines [i^delim]
      else Inline(i^delim)
  end

let concat_and_delim_source_code delim a b =
  concat_source_code (delim_source_code delim a) b

let concat_and_delim_source_code_list ?(final=false) ?(delim="") bcl =
  if bcl = [] then Inline("") else
    let r =
      let ne = List.filter (fun x -> not(empty_source_code x)) bcl in
      match ne with
      | [] -> Inline("") | [x] -> x
      | h::t -> List.fold_left (concat_and_delim_source_code delim) h t
    in if final then delim_source_code delim r else r

let indent_source_code s bc =
  if empty_source_code bc then bc 
  else begin match bc with
  | Lines l -> Lines(List.map (fun a -> s^a) l)
  | Inline i -> Inline(s^i)
  end

let unary_op_source_code op bc =
   match bc with
   | Lines([]) -> Inline(op)
   | Lines(l::rest) -> 
      if (String.length op) > (indent_width*2) then 
         concat_source_code (Inline(op)) (indent_source_code tab bc)
      else 
         concat_source_code 
            (Inline(op^l))
            (indent_source_code (String.make (String.length op) ' ')
                                (Lines(rest)))
   | Inline(i) -> Inline(op^i)

let binary_op_source_code op bc_left bc_right =
   match (bc_left,bc_right) with
   | ((Lines([])),_)             -> unary_op_source_code op bc_right
   | ((Lines(l)),(Lines([])))    -> Lines(l@[op])
   | ((Inline(l)),(Lines([])))   -> Inline(l^op)
   | ((Lines(ll)),(Lines(rl)))   -> Lines(ll@[tab^op]@rl)
   | ((Inline(li)),(Lines(rl)))  -> Lines([li;tab^op]@rl)
   | ((Lines(ll)),(Inline(ri)))  -> Lines(ll@[tab^op;ri])
   | ((Inline(li)),(Inline(ri))) -> Inline(li^op^ri)

let parenthesize_source_code lparen rparen bc =
   match (unary_op_source_code lparen bc) with 
   | Lines(l) -> Lines(l@[rparen])
   | Inline(i) -> Inline(i^rparen)

