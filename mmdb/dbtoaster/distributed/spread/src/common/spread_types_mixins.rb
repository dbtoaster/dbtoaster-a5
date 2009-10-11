require 'spread_types';
require 'ok_mixins';

class NodeID
  def to_s
    host + ":" + port.to_s;
  end
  
  def NodeID.make(host, port)
    n = NodeID.new;
    n.host = host.to_s;
    n.port = port.to_i;
    n;
  end
end

class Entry 
  def to_s
    @source.to_s + "[" + @key.join(", ") + "]";
  end
  
  def has_wildcards?
    @key.include? -1;
  end
  
  def Entry.make(source, key)
    entry = Entry.new
    entry.source, entry.key = source.to_i.freeze, key.collect do |k| k.to_i end.freeze;
    entry.freeze;
  end
  
  def Entry.parse(string)
    parsed = / *(Map)? *([0-9]+) *\[([^\]]*)\]/.match(string);
    Entry.make(parsed[2], parsed[3].split(",")); # Entry.make does the to_i conversion
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
              Logger.debug "Value: " + key + " = " + params[key].to_f.to_s;
              PutFieldType::VALUE;
            when Entry then
              field.entry = params[key];
              Logger.debug "Entry: " + key + " = " + params[key].to_s;
              PutFieldType::ENTRY;
            else 
              raise SpreadException.new("Unknown parameter type: " + params[key].class.to_s);
          end;
        field;
      end
    ret;
  end
  
  def to_s
    "{ " + @params.collect do |field|
      field.name + "=>" + 
        case field.type
          when PutFieldType::VALUE then "VALUE:" + field.value.to_s;
          when PutFieldType::ENTRY then "ENTRY:" + field.entry.to_s;
        end
    end.join(", ") + " }"
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
      transport = Thrift::FramedTransport.new(Thrift::Socket.new(host, port))
      protocol = Thrift::BinaryProtocol.new(transport)
      ret = MapNode::Client.new(protocol);
      transport.open;
      ret;
    end
  end
  
  class Processor
    def Processor.listen(port, config = Array.new)
      handler = MapNodeHandler.new();
      config.each do |f|
        handler.setup(
          case f
            when File then f;
            when String then File.open(f);
            else raise SpreadException.new("Unusable processor configuration source: " + f.to_s);
          end
        );
      end
    
      processor = MapNode::Processor.new(handler)
      transport = Thrift::ServerSocket.new(port);
      [handler, Thrift::NonblockingServer.new(
        processor, 
        transport, 
        Thrift::FramedTransportFactory.new,
        Thrift::BinaryProtocolFactory.new,
        1,
        Logger.default
      )];
    end
  end
end