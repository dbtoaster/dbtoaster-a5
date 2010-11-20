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

let empty_source_code bc = bc = Lines([]) || bc = Inline("")

let delim_source_code delim bc =
  begin match bc with
    | Lines (l) ->
      let x = List.rev l in
      Lines(List.rev (((List.hd x)^delim)::(List.tl x)))
    | Inline (i) ->
      if delim = stmt_delimiter then Lines [i^delim]
      else Inline(i^delim)
  end

let concat_and_delim_source_code delim a b =
  concat_source_code (delim_source_code delim a) b

let concat_and_delim_source_code_list ?(final=false) ?(delim="") bcl =
  if bcl = [] then Inline("") else
    let r = List.fold_left (concat_and_delim_source_code delim)
      (List.hd bcl)
      (List.filter (fun x -> not(empty_source_code x)) (List.tl bcl))
    in if final then delim_source_code delim r else r

let indent_source_code s bc =
  if empty_source_code bc then bc 
  else begin match bc with
  | Lines l -> Lines(List.map (fun a -> s^a) l)
  | Inline i -> Inline(s^i)
  end

