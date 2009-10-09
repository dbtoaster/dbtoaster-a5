require 'spread_types';
require 'ok_mixins';

class NodeID
  def to_s
    host + ":" + port.to_s;
  end
end

class Entry 
  def to_s
    @source.to_s + "[" + @key.join(", ") + "]";
  end
  
  def has_wildcards?
    @key.includes? -1;
  end
  
  def Entry.make(source, key)
    entry = Entry.new
    entry.source, entry.key = source.to_i.freeze, key.collect do |k| k.to_i end.freeze;
    entry.freeze;
  end
end

class PutParams
  def PutParams.make(params)
    ret = PutParams.new;
    ret.params = 
      params.keys.collect do |key|
        field = PutField.new()
        field.name = key
        field.type = 
          case params[key]
            when Numeric, String then 
              field.value = params[key].to_f;
              #puts "Value: " + key + " = " + params[key].to_f.to_s;
              PutFieldType::VALUE;
            when Entry then
              field.entry = params[key];
              #puts "Entry: " + key + " = " + params[key].to_s;
              PutFieldType::ENTRY;
            else 
              raise SpreadException.new("Unknown parameter type: " + params[key].class.to_s);
          end;
        field;
      end
    ret;
  end
  
  def decipher
    @params.collect_hash do |field|
      [
        field.name,
        case field.type
          when PutFieldType::VALUE then field.value;
          when PutFieldType::ENTRY then field.entry;
        end
      ];
    end
  end
end

module MapNode
  class Client
    def close
      @iprot.trans.close;
    end
    
    def Client.connect(host, port)
      transport = Thrift::BufferedTransport.new(Thrift::Socket.new(host, port))
      protocol = Thrift::BinaryProtocol.new(transport)
      ret = MapNode::Client.new(protocol);
      transport.open;
      ret;
    end
  end
end