

require 'thrift_compat';
require 'ok_mixins';

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

class MapEntry 
  def MapEntry.wildcard
    -1;
  end
  
  def to_s
    @source.to_s + "[" + @key.collect { |k| if k < 0 then "*" else k.to_s end }.join(", ") + "]";
  end
  
  def has_wildcards?
    @key.include? MapEntry.wildcard;
  end
  
  def MapEntry.make(source, key)
    entry = MapEntry.new
    entry.source, entry.key = source.to_i.freeze, key.collect do |k| k.to_i end.freeze;
    entry.freeze;
  end
  
  def MapEntry.parse(string)
    parsed = / *(Map)? *([0-9]+) *\[([^\]]*)\]/.match(string);
    MapEntry.make(parsed[2], parsed[3].split(",")); # MapEntry.make does the to_i conversion
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
    MapEntry.compute_partition(@key, sizes);
  end
  
  def MapEntry.compute_partition(keys, sizes)
    ret = keys.zip(sizes).collect do |k, s|
      if s <= 1 then 0
      elsif k < 0 then -1
      else (k.to_i % s.to_i)
      end
    end
    ret;
  end
end

class PutRequest
  def PutRequest.make(template, id_offset, num_gets)
    put_request = PutRequest.new;
    put_request.template = template.to_i;
    put_request.id_offset = id_offset.to_i;
    put_request.num_gets = num_gets.to_i;
    put_request;
  end
end

class GetRequest
  attr_reader :replacements;
  attr_writer :replacements;

  def GetRequest.make(target, id_offset, entries)
    get_request = GetRequest.new;
    get_request.target = target;
    get_request.id_offset = id_offset.to_i;
    replacements = Array.new;
    get_request.entries = 
      entries.collect_index do |ei, e|
        e_new = MapEntry.new;
        e_new.source = e.source;
        e_new.key = e.key.collect_index do |i,k| 
          replacements.push([ei,i,k]) if (k.is_a? Numeric) && (k >= 0); 
          k.to_i;
        end
        e_new;
      end
    get_request.replacements = replacements;
    get_request;
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
            when MapEntry then
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
      if host.is_a? NodeID then
        port = host.port;
        host = host.host;
      end
      transport = Thrift::FramedTransport.new(Thrift::Socket.new(host, port))
      protocol = Thrift::BinaryProtocol.new(transport)
      ret = MapNode::Client.new(protocol);
      transport.open;
      ret;
    end
  end
  
  class Processor
    def Processor.listen(port, name = "Unknown MapNode")
      handler = MapNodeHandler.new(name);
      
      logger = Logger.new(STDOUT);
        logger.level = Logger::INFO;

      processor = MapNode::Processor.new(handler)
      transport = Thrift::ServerSocket.new(port);
      server = Thrift::NonblockingServer.new(
        processor, 
        transport, 
        Thrift::FramedTransportFactory.new,
        Thrift::BinaryProtocolFactory.new,
        1,
        logger
      );
      handler.designate_server(server);
      [handler, server];
    end
  end
end

module SwitchNode
  class Client
    def close
      @iprot.trans.close;
    end
    
    def Client.connect(host, port = nil)
      if host.is_a? NodeID then
        port = host.port;
        host = host.host;
      end
      raise "Invalid port" unless port;
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
    "SpreadException: " + why.to_s;
  end
  
  def SpreadException.make(why)
    e = SpreadException.new;
    e.why = why;
    e;
  end
  
  def SpreadException.backoff(why)
    ret = SpreadException.make(why);
    ret.retry = true;
    ret;
  end
end
