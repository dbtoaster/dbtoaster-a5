{   
open PartitionMapParser   
open Lexing   

let init_line lexbuf =
    let pos = lexbuf.Lexing.lex_curr_p in
        lexbuf.Lexing.lex_curr_p <- { pos with
            Lexing.pos_lnum = 1;
            Lexing.pos_bol = 0;
        }

let advance_line lexbuf =
    let pos = lexbuf.Lexing.lex_curr_p in
        lexbuf.Lexing.lex_curr_p <- { pos with
            Lexing.pos_lnum = pos.Lexing.pos_lnum + 1;
            Lexing.pos_bol = pos.Lexing.pos_cnum;
        }

let hashtbl_of_pair_list h l =
    List.iter (fun (p1, p2) -> Hashtbl.add h p1 p2) l

let keyword_table = Hashtbl.create 30
let keywords =   
    [   
        "NODE", NODE;
        "MAP",  MAP
    ]
let _ = hashtbl_of_pair_list keyword_table keywords
    
}   
 
let char        = ['a'-'z' 'A'-'Z']   
let digit       = ['0'-'9']
let int         = digit+
let byte        = digit? digit? digit
let ip_addr     = byte "." byte "." byte "." byte
let identifier  = char(char|digit|['-' '_' '.'])*
let whitespace  = [' ' '\t']   
let newline     = "\n\r" | '\n' | '\r'  
let strconst    = '\''[^'\'']*'\''
let multicmst   = "/*"
let multicmend  = "*/"
let arrow       = "->"
 
rule tokenize = parse
| whitespace    { tokenize lexbuf }   
| newline       { advance_line lexbuf; tokenize lexbuf }
| int           { INT(int_of_string (lexeme lexbuf)) }
| strconst      { let s = lexeme lexbuf in 
                      STRING(String.sub s 1 ((String.length s)-2)) }
| ip_addr       { IP_ADDR((lexeme lexbuf)) }
| identifier    { 
                  let keyword_str = lexeme lexbuf in
                  let keyword_str_uc = String.uppercase keyword_str in
                      try Hashtbl.find keyword_table keyword_str_uc
                      with Not_found -> ID(keyword_str)
                }
| ','           { COMMA }
| ';'           { SEMICOLON }
| arrow         { ARROW }
| '['           { LBRACKET }
| ']'           { RBRACKET }
| '{'           { LBRACE }
| '}'           { RBRACE }
| multicmst     { comment 1 lexbuf }

and comment depth = parse
| multicmst     { raise (Failure ("nested comments are invalid")) }
| multicmend    { tokenize lexbuf }
| eof           { raise (Failure ("hit end of file in a comment")) }
| _             { comment depth lexbuf }
