require "#{File.dirname($0)}/util.rb"

class OcamlDB < Hash
  def initialize(db_string)
    tok = Tokenizer.new(
      db_string,
      /\[|\]|->|[a-zA-Z][a-zA-Z0-9_]*|[0-9]+|\.|<pat=[^>]*>|;/
    )
    raise "Not A Database (Got '#{tok.peek}')" unless tok.peek == "[";
    tree = TreeBuilder.new(tok) do |tree, t|
      case t
        when "[" then tree.push
        when "]" then tree.pop
        when "->" then 
          lhs = tree.backtrack;
          rhs = tree.next_leaf;
          tree.insert [lhs, rhs]
        when "." then 
          lhs = tree.backtrack;
          rhs = tree.next_leaf;
          tree.insert "#{lhs}.#{rhs}"
        when /<pat=.*/, ";" then #ignore
        else tree.insert t 
      end
    end
    OcamlDB.parse_named_forest(tree.pop.pop, self);
  end
  
  def OcamlDB.parse_named_forest(elements, into = Hash.new)
    elements.each do |k, contents|
      val = 
        case contents
          when Array then
            v = parse_named_forest(contents);
            v unless v.length == 0;
          when String then
            v = contents.to_f;
            v unless v == 0.0;
          else raise "Unknown value type"
        end
      into[k.map { |k_elem| k_elem.to_f }.reverse] = val unless val.nil?;
    end
    into;
  end
end