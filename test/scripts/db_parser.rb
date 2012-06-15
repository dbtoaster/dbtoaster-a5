require "#{File.dirname($0)}/util.rb"

class String
  def extract_dbt_value
    case self.chomp
      when /([a-zA-Z][a-zA-Z0-9_# ]*)/ then $1;
      when / *([0-9]+)\-([0-9]+)\-([0-9]+)/ then ($1.to_i*10000+$2.to_i*100+$3.to_i).to_f;
      when / *([\-\+]?[0-9]+\.[0-9]*e?[\-\+]?[0-9]*)/ then $1.to_f;
      when / *([\-\+]?[0-9]+)/ then $1.to_f;
      else $1
    end
  end
end

class OcamlDB < Hash
  def initialize(db_string, reverse_key = true)
    tok = Tokenizer.new(
      db_string,
        /\[|\]|->|[0-9]+\-[0-9]+\-[0-9]+|[a-zA-Z][a-zA-Z0-9_#\s]*|[\-\+]?[0-9]+\.?[0-9]*e?[\-\+]?[0-9]*|<pat=[^>]*>|SingleMap|DoubleMap|TupleList|\(|\)|;/
    )
    tok.next while (tok.peek == "SingleMap") || tok.peek == "DoubleMap" || tok.peek == "TupleList" || (tok.peek == "(");
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
          if /[0-9]+/ =~ tree.peek 
            then tree.insert "#{lhs}.#{tree.next_leaf}"
            else tree.insert lhs
          end
          
        when /<pat=.*/, ";" then #ignore
        else tree.insert t 
      end
    end
    OcamlDB.parse_named_forest(tree.pop.pop, reverse_key, self);
  end
  
  def OcamlDB.parse_named_forest(elements, reverse, into = Hash.new)
    elements.each do |k, contents|
      val = 
        case contents
          when Array then
            v = parse_named_forest(contents);
            v unless v.length == 0;
          when /nan/ then
            v = 0;
          when /[\-\+]?[0-9]+\.[0-9]*e?[\-\+]?[0-9]*/ then 
            v = contents.to_f;
            v unless v == 0.0;
          when /[\-\+]?[0-9]+/ then
            v = contents.to_f;
            v unless v == 0;
          else raise "Unknown value type"
        end
      k = k.map { |k_elem| k_elem.extract_dbt_value }
      k = k.reverse if reverse;
      into[k] = val unless val.nil?;
    end
    into;
  end
end

class CppDB < Hash
  def initialize(db_string)
    tok = Tokenizer.new(db_string, /<\/?[^>]+>|[^<]+/);
    loop do 
      tok.tokens_up_to(/<item[^>]*>/);
      break unless /<item[^>]*>/ =~ tok.last;
      fields = Hash.new("");
      curr_field = nil;
      tok.tokens_up_to("</item>").each do |t|
        case t
          when /<\/.*>/ then curr_field = nil;
          when /<(.*)>/ then curr_field = $1;
          else 
            if curr_field then 
              fields[curr_field] = fields[curr_field] + t 
            end
        end
      end
      keys = fields.keys.clone;
      keys.delete("__av");
      self[
        keys.
          map { |k| k[3..-1].to_i }.
          sort.
          map { |k| fields["__a#{k}"].extract_dbt_value }
      ] = fields["__av"].to_f unless fields["__av"].extract_dbt_value == 0.0
    end
  end
end