val indent_width : int
val stmt_delimiter : string
val tab : string

type source_code_id_t = string
type source_code_op_t = string
type source_code_t = Lines of string list | Inline of string

val string_of_source_code : source_code_t -> string
val concat_source_code : source_code_t -> source_code_t -> source_code_t
val concat_many_source_code :
  source_code_t -> source_code_t list -> source_code_t

val empty_source_code : source_code_t -> bool

val delim_source_code :
  ?preserve:bool -> string -> source_code_t -> source_code_t

val concat_and_delim_source_code :
  string -> source_code_t -> source_code_t -> source_code_t

val concat_and_delim_source_code_list :
  ?final:bool -> ?delim:string -> source_code_t list -> source_code_t

val indent_source_code : string -> source_code_t -> source_code_t
val unary_op_source_code : string -> source_code_t -> source_code_t
val binary_op_source_code : string -> source_code_t -> source_code_t -> source_code_t
val parenthesize_source_code : string -> string -> source_code_t -> source_code_t

