require 'spread_types';
require 'ok_mixins';
require 'switch_node';
require 'map_node';

class NodeID
  def to_s
    host + ":" + port.to_s;
  end
  
  def NodeID.make(host, port = 52982)
    n = NodeID.new;
    n.host = host.to_s;
    n.port = port.to_i;
    n;
  end
end

class Entry 
  def Entry.wildcard
    -1;
  end
  
  def to_s
    @source.to_s + "[" + @key.collect { |k| if k < 0 then "*" else k.to_s end }.join(", ") + "]";
  end
  
  def has_wildcards?
    @key.include? wildcard;
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
  
  def hash
    @source.hash + @key.hash;
  end
  
  def hashed_key
    # A key array with hashes and nil values for wildcards
    @key.collect do |k|
      k.hash.abs if k != wildcard;
    end
  end
  
  def partition(sizes)
    compute_partition(@key, sizes);
  end
  
  def Entry.compute_partition(keys, sizes)
    keys.zip(sizes).collect do |k, s|
      if s == 0 then 0
      elsif k < 0 then -1
      else (k.hash.abs.to_f * (size.to_f / Fixnum.max_fixnum.to_f))
      end
    end
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
              PutFieldType::VALUE;
            when Entry then
              field.entry = params[key];
              PutFieldType::ENTRY;
            else 
              raise SpreadException.new("Unknown parameter type: " + params[key].class.to_s + " (for key " + key.to_s + ")");
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

module AggregateType
  def AggregateType.aggregate(type, values)
    case type
      when SUM then sum = 0.0; values.each { |v| sum += v.to_f }; sum
      when AVG then sum = 0.0; values.each { |v| sum += v.to_f }; sum / values.size.to_f
      when MAX then Math.max(values);
    end
  end
end

module MapNode
  class Client
    def close
      @iprot.trans.close;
    end
    
    def Client.connect(host, port = 52982)
      transport = Thrift::FramedTransport.new(Thrift::Socket.new(host, port))
      protocol = Thrift::BinaryProtocol.new(transport)
      ret = MapNode::Client.new(protocol);
      transport.open;
      ret;
    end
  end
  
  class Processor
    def Processor.listen(port, name = "Unknown MapNode", config = Array.new)
      handler = MapNodeHandler.new(name);
      config.each do |f|
        handler.setup(
          case f
            when File then f;
            when String then File.open(f);
            else raise SpreadException.new("Unusable processor configuration source: " + f.to_s);
          end
        );
      end
      
      logger = Logger.new(STDOUT);
        logger.level = Logger::INFO;

      processor = MapNode::Processor.new(handler)
      transport = Thrift::ServerSocket.new(port);
      [handler, Thrift::NonblockingServer.new(
        processor, 
        transport, 
        Thrift::FramedTransportFactory.new,
        Thrift::BinaryProtocolFactory.new,
        1,
        logger
      )];
    end
  end
end

module SwitchNode
  class Client
    def close
      @iprot.trans.close;
    end
    
    def Client.connect(host, port)
      transport = Thrift::FramedTransport.new(Thrift::Socket.new(host, port))
      protocol = Thrift::BinaryProtocol.new(transport)
      ret = SwitchNode::Client.new(protocol);
      transport.open;
      ret;
    end
  end
  
  class Processor
    def Processor.listen(port)
      handler = SwitchNodeHandler.new();
      logger = Logger.new(STDOUT);
        logger.level = Logger::INFO;
      processor = SwitchNode::Processor.new(handler)
      transport = Thrift::ServerSocket.new(port);
      [handler, Thrift::NonblockingServer.new(
        processor, 
        transport, 
        Thrift::FramedTransportFactory.new,
        Thrift::BinaryProtocolFactory.new,
        1,
        logger
      )];
    end
  end
end

class SpreadException
  def to_s
    "SpreadException: " + why;
  end
end
