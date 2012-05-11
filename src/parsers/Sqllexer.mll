{   
open Sqlparser   
open Lexing   
open Types

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

let keyword_table = Hashtbl.create 60
let keywords =   
    [   
        "CREATE", CREATE;
        "TABLE", TABLE;
        "FROM", FROM;
        "USING", USING;
        "DELIMITER", DELIMITER;
        "SELECT", SELECT;   
        "WHERE", WHERE;
        "GROUP", GROUP;
        "HAVING", HAVING;
        "ORDER", ORDER;
        "BY", BY;
        "JOIN", JOIN; 
        "INNER", INNER;
        "OUTER", OUTER;
        "LEFT", LEFT;
        "RIGHT", RIGHT;
        "ON", ON;
        "AS", AS;
        "AND", AND;
        "OR", OR;
        "NOT", NOT;
        "BETWEEN", BETWEEN;
        "ASC", ASC;
        "DESC", DESC;
        "SOURCE", SOURCE;
        "ARGS", ARGS;
        "INSTANCE", INSTANCE;
        "TUPLE", TUPLE;
        "ADAPTOR", ADAPTOR;
        "BINDINGS", BINDINGS;
        "SOCKET", SOCKET;
        "FILE", FILE;
        "PIPE", PIPE;
        "FIXEDWIDTH", FIXEDWIDTH;
        "DELIMITED", DELIMITED;
        "LINE", LINE;
        "VARSIZE", VARSIZE;
        "OFFSET", OFFSET;
        "ADJUSTBY", ADJUSTBY;
        "POSTGRES", POSTGRES;
        "RELATION", RELATION;
        "DATE", DATE;
        "NATURAL", NATURAL;
        "EXISTS", EXISTS;
        "TRUE", TRUE;
        "FALSE", FALSE;
        "INT",     TYPE(TInt);
        "INTEGER", TYPE(TInt);
        "FLOAT",   TYPE(TFloat);
        "DOUBLE",  TYPE(TFloat);
        "DECIMAL", TYPE(TFloat);
        "STRING",  TYPE(TString);
        "CHAR",    CHAR;
        "VARCHAR", VARCHAR;
        "STRING", VARCHAR;
        "STREAM", STREAM;
        "SUM", SUMAGG;
        "COUNT", COUNTAGG;
        "AVG", AVGAGG
    ]
let _ = hashtbl_of_pair_list keyword_table keywords

let ops_table = Hashtbl.create 10
let ops =
    [   
        "=",    EQ;
        "!=",   NE;
        "<>",   NE;
        "<",    LT;   
        "<=",   LE;  
        ">",    GT;   
        ">=",   GE;
        "+",    SUM;
        "-",    MINUS;
        "*",    PRODUCT;
        "/",    DIVIDE;
    ]
let _ = hashtbl_of_pair_list ops_table ops
    
}   
 
let char        = ['a'-'z' 'A'-'Z']   
let digit       = ['0'-'9']   
let bindigit    = [ '0' '1' ]
let binint      = '0'['B' 'b']bindigit+
let octdigit    = [ '0' - '7' ]
let octint      = '0'['O' 'o']octdigit+
let decint      = digit+
let hexdigit    = [ '0'-'9' 'A'-'F' 'a'-'f']
let hexint      = '0'['X' 'x']hexdigit+
let int         = binint|octint|decint|hexint
let decimal     = digit+ '.' digit+
let float       = (int|decimal)'E'('+'|'-')?digit+
let identifier  = char(char|digit|['-' '_'])*
let whitespace  = [' ' '\t']   
let newline     = "\n\r" | '\n' | '\r'  
let cmp_op      = ">" | ">=" | "<" | "<=" | "=" | "!=" | "<>"
let arith_op    = '+' | '-' | '*' | '/'
let strconst    = '\''[^'\'']*'\''

let singlecm    = "--"[^'\n' '\r']*
let multicmst   = "/*"
let multicmend  = "*/"
 
rule tokenize = parse
| whitespace    { tokenize lexbuf }   
| newline       { advance_line lexbuf; tokenize lexbuf }
| int           { INT(int_of_string (lexeme lexbuf)) }
| decimal       { FLOAT(float_of_string (lexeme lexbuf)) }
| float         { FLOAT(float_of_string (lexeme lexbuf)) }
| strconst      { let s = lexeme lexbuf in 
                      STRING(String.sub s 1 ((String.length s)-2)) }
| cmp_op
| arith_op      { let op_str = lexeme lexbuf in
                      try Hashtbl.find ops_table op_str
                      with Not_found -> raise (Failure ("unknown operator "^(op_str)))
                }
| identifier    { 
                  let keyword_str = lexeme lexbuf in
                  let keyword_str_uc = String.uppercase keyword_str in
                      try Hashtbl.find keyword_table keyword_str_uc
                      with Not_found -> ID(keyword_str)
                }
| ','           { COMMA }
| '('           { LPAREN }
| ')'           { RPAREN }
| singlecm      { tokenize lexbuf}
| multicmst     { comment 1 lexbuf }
| ';'           { EOSTMT }
| ":="          { SETVALUE }
| '.'           { PERIOD }
| eof           { EOF }

and comment depth = parse
| multicmst     { raise (Failure ("nested comments are invalid")) }
| multicmend    { tokenize lexbuf }
| eof           { raise (Failure ("hit end of file in a comment")) }
| _             { comment depth lexbuf }
