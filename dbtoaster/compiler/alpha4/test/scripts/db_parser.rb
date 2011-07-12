require "#{File.dirname($0)}/util.rb"

class OcamlDB < Hash
  def initialize(db_string)
    tok = Tokenizer.new(
      db_string,
      /\[|\]|->|[a-zA-Z][a-zA-Z0-9_]*|[0-9]+.[0-9]*|<pat=[^>]*>|;/
    )
    raise "Not A Database" unless tok.peek == "[";
    tree = TreeBuilder.new(tok) do |tree, t|
      case t
        when "[" then tree.push
        when "]" then tree.pop
        when "->" then 
          lhs = tree.backtrack;
          rhs = tree.next_leaf;
          tree.insert [lhs, rhs]
        when /<pat=.*/, ";" then #ignore
        else tree.insert t 
      end
    end
    OcamlDB.parse_named_forest(tree.pop.pop, self);
    p self;
  end
  
  def OcamlDB.parse_named_forest(elements, into = Hash.new)
    elements.each do |k, contents|
      into[k] = 
        case contents
          when Array then
            parse_named_forest(contents);
          when String then
            contents.to_f
          else raise "Unknown value type"
        end
    end
    into;
  end
end