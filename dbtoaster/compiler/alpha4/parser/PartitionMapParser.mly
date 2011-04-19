
%{
(* 
program := node_list map_list

node_list := node ; node_list | node

node := NODE string ip_addr 

map_list := map ; map_list | map

map := MAP ID [ var_list ] { map_partition_list }

var_list := int ; var_list | int

map_partition_list := map_partition ; map_partition_list | map_partition

map_partition := [ var_list ] -> node_name_list

node_name_list := string , node_name_list | string
*)

%}

%token <string> ID STRING IP_ADDR
%token <int> INT 
%token NODE MAP
%token LBRACE RBRACE LBRACKET RBRACKET
%token SEMICOLON COMMA ARROW

// start   
%start cumulusPartitionMap

%type < (string * string) list * (string * int list * (int list * string list) list) list > cumulusPartitionMap

%%

cumulusPartitionMap: 
   |  node_list SEMICOLON map_list { ($1,$3) }
   |  map_list                     { ([],$1) }
   |  node_list                    { ($1,[]) }

node_list: 
   | node SEMICOLON node_list { $1 :: $3 }
   | node                     { [$1] }

node: 
   | NODE STRING IP_ADDR      { ($2,$3) }

map_list: 
   | map SEMICOLON map_list   { $1 :: $3 }
   | map                      { [$1] }

map:
   | MAP STRING var_list map_partition_list
                              { ($2,$3,$4) }

var_list:
   | LBRACKET RBRACKET                { [] }
   | LBRACKET var_list_entry RBRACKET { $2 }

var_list_entry:
   | INT SEMICOLON var_list_entry { $1 :: $3 }
   | INT                          { [$1] }

map_partition_list:
   | LBRACE RBRACE                          { [] }
   | LBRACE map_partition_list_entry RBRACE { $2 }

map_partition_list_entry:
   | map_partition SEMICOLON map_partition_list_entry { $1 :: $3 }
   | map_partition                                    { [$1] }

map_partition: 
   | var_list ARROW node_name_list { ($1,$3) }

node_name_list:
   | STRING SEMICOLON node_name_list { $1 :: $3 }
   | STRING                          { [$1] }

%%
