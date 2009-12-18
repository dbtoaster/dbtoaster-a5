include Java;

require_java 'spread_thrift.jar';
require 'ok_mixins';

########################### ENUMS #####################################

include_class Java::PutFieldType;
include_class Java::AggregateType;

#######################################################################

include_class Java::NodeID;
JavaUtilities.extend_proxy Java::NodeID do
  def to_s
    host + ":" + port.to_s;
  end
  
  def NodeID.make(host, port = 52982)
    NodeID.new(host, port);
  end
end

#######################################################################

include_class Java::MapEntry
JavaUtilities.extend_proxy Java::MapEntry do
  def MapEntry.wildcard
    -1;
  end
  
  def to_s
    #ArrayXX
    source.to_s + "[" + key.collect { |k| if k < 0 then "*" else k.to_s end }.join(", ") + "]";
  end
  
  def has_wildcards?
    key.include? MapEntry.wildcard;
  end
  
  def MapEntry.make(source, key)
    entry = MapEntry.new(source.to_i, key.collect do |k| k.to_i end.freeze.to_java(:Integer)).freeze
  end
  
  def MapEntry.parse(string)
    parsed = / *(Map)? *([0-9]+) *\[([^\]]*)\]/.match(string);
    MapEntry.make(parsed[2], parsed[3].split(",")); # MapEntry.make does the to_i conversion
  end
  
  def partition(sizes)
    MapEntry.compute_partition(key, sizes);
  end
  
  def MapEntry.compute_partition(keys, sizes)
    #ArrayXX
    ret = keys.zip(sizes).collect do |k, s|
      if s <= 1 then 0
      elsif k < 0 then -1
      else (k.to_i % s.to_i)
      end
    end
    ret;
  end
end

#######################################################################

include_class Java::SpreadException;
JavaUtilities.extend_proxy Java::SpreadException do
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

#######################################################################

include_class Java::PutField;
include_class Java::PutParams;
JavaUtilities.extend_proxy Java::PutParams do
  def PutParams.make(params)
    converted_params = 
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
    PutParams.new(converted_params);
  end
  
  def to_s
    "{ " + params.collect do |field|
      field.name + "=>" + 
        case field.type
          when PutFieldType::VALUE then "VALUE:" + field.value.to_s;
          when PutFieldType::ENTRY then "ENTRY:" + field.entry.to_s;
        end
    end.join(", ") + " }"
  end
  
  def decipher
    params.to_a.collect_hash do |field|
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

#######################################################################

include_class Java::PutRequest;
JavaUtilities.extend_proxy Java::PutRequest do
  def PutRequest.make(template, id_offset, num_gets)
    PutRequest.new(template.to_i, id_offset.to_i, num_gets.to_i)
  end
end

#######################################################################

include_class Java::GetRequest;
JavaUtilities.extend_proxy Java::GetRequest do
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
        end.to_java;
        e_new;
      end.to_java;
    get_request.replacements = replacements.to_java;
    get_request;
  end
end

#######################################################################

module MapNodeJavaHandlerInterface 
  include Java::MapNode::Iface;
  
  def push_get(result, cmdid)
    ruby_result = result.entrySet.toArray.to_a.collect_hash do |entry|
      [entry.key, entry.value]
    end
    super(ruby_result, cmdid)
  end
end
module MapNodeJavaHandlerInterface 
  include Java::MapNode::Iface;
  
  def push_get(result, cmdid)
    ruby_result = result.entrySet.toArray.to_a.collect_hash do |entry|
      [entry.key, entry.value]
    end
    super(ruby_result, cmdid)
  end
end

module ClientJavaInterfaceAdditions
  def close
    self.getOutputProtocol.getTransport.close;
  end
end

module MapNode
  class Client
    def Client.connect(host, port = 52982)
      if host.is_a? NodeID then
        port = host.port;
        host = host.host;
      end
      socket = org.apache.thrift.transport.TSocket.new(host, port)
      transport = Java::org.apache.thrift.transport.TFramedTransport.new(socket)
      protocol = org.apache.thrift.protocol.TBinaryProtocol.new(transport)
      ret = Java::MapNode.Client.new(protocol);
      transport.open;
      ret.extend(ClientJavaInterfaceAdditions);
      ret;
    end
  end
  
  class Processor
    def Processor.listen(port, name = "Unknown MapNode")
      handler = MapNodeHandler.new(name);
      handler.extend(MapNodeJavaHandlerInterface)

      processor = Java::MapNode.Processor.new(handler)
      transport = Java::org.apache.thrift.transport.TServerSocket.new(port);
      server = org.apache.thrift.server.TNonblockingServer(processor, transport);
      handler.designate_server(server);
      [handler, server];
    end
  end
end

#######################################################################

module SwitchNode
  class Client
    def Client.connect(host, port)
      if host.is_a? NodeID then
        port = host.port;
        host = host.host;
      end
      socket = org.apache.thrift.transport.TSocket.new(host, port)
      transport = Java::org.apache.thrift.transport.TFramedTransport.new(socket)
      protocol = org.apache.thrift.protocol.TBinaryProtocol.new(transport)
      ret = Java::SwitchNode.Client.new(protocol);
      transport.open;
      ret.extend(ClientJavaInterfaceAdditions);
      ret;
    end
  end
  
  class Processor
    def Processor.listen(port, name = "Unknown MapNode")
      handler = SwitchNodeHandler.new(name);

      processor = Java::SwitchNode.Processor.new(handler)
      transport = Java::org.apache.thrift.transport.TServerSocket.new(port);
      server = org.apache.thrift.server.TNonblockingServer(processor, transport);
      handler.designate_server(server);
      [handler, server];
    end
  end
end

