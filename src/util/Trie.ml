(* 
   A trie is a tree structure reprsenting a list of 'words' with common 
   prefixes.  Each node represents a single letter, and keeps track of how many
   words have been inserted into the trie that pass through that particular 
   node.
*)

type 'letter_t node_t = Node of int * 'letter_t t
and  'letter_t t = ('letter_t * 'letter_t node_t) list

let empty:'letter_t t = []

let rec add_word (trie:'l t) (word:'l list): 'l t
   if List.length word < 1 then trie else
   if List.mem_assoc hd trie then 
      (match List.assoc (List.hd word) trie with Node(cnt, children) ->
         ((List.hd word), Node(cnt+1, (add_word (List.tl word) children)))) :: 
      (List.remove_assoc (List.hd word) trie)
   else
      [(List.hd word), Node(1, (add_word (List.tl word) []))]

let size (trie:'l t): int =   
   List.fold_left (+) 0 (List.map (fun (_,x)->match x with Node(c,_)->c))

let rec delete_word (trie:'l t) (word:'l list): 'l t
   if List.length word < 1 then trie else
   let (old_cnt,old_children) = match List.assoc (List.hd word) trie with
      Node(cnt,child) -> (cnt, child)
   in
   let old_peers = List.remove_assoc (List.hd word) trie in
   if (size old_children) <= old_cnt then raise Not_found
   else
      (List.hd word,Node(old_cnt-1,(delete_word (List.tl word) old_children)))::
      old_peers
