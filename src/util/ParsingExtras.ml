(**
   Supplemental functionality for building parsers.
*)

open Lexing

let format_error_at_position ({  pos_fname = err_file;
                                 pos_lnum  = err_line_no;
                                 pos_bol   = err_line_idx;
                                 pos_cnum  = err_col;
                              }:Lexing.position): string =
   if err_file = "" then
      "(at <stdin>:"^(string_of_int err_line_no)^")"
   else
      let f = open_in err_file in
      seek_in f err_line_idx;
         "(at "^(err_file)^":"^(string_of_int err_line_no)^")\n"^
         (input_line f)^"\n"^(String.make (err_col-err_line_idx) ' ')^"^"

let lexpos_set_fname fname ({  pos_lnum  = pos_lnum;
                               pos_bol   = pos_bol;
                               pos_cnum  = pos_cnum;
                            }:Lexing.position): Lexing.position = 
   {  pos_fname = fname;
      pos_lnum  = pos_lnum;
      pos_bol   = pos_bol;
      pos_cnum  = pos_cnum; }

let lexbuf_for_file fname = 
   let lexbuf = Lexing.from_channel (open_in fname) in
      lexbuf.lex_start_p <- lexpos_set_fname fname lexbuf.lex_start_p;
      lexbuf.lex_curr_p  <- lexpos_set_fname fname lexbuf.lex_curr_p;
      lexbuf

let lexbuf_for_file_or_stdin = (function
   | "-" -> Lexing.from_channel stdin
   | f   -> lexbuf_for_file f
)