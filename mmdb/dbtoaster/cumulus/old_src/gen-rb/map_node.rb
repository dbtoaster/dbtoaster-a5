#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

require 'thrift'
require 'spread_types'

module MapNode
  class Client
    include ::Thrift::Client

    def put(id, template, params)
      send_put(id, template, params)
    end

    def send_put(id, template, params)
      send_message('put', Put_args, :id => id, :template => template, :params => params)
    end
    def mass_put(id, template, expected_gets, params)
      send_mass_put(id, template, expected_gets, params)
    end

    def send_mass_put(id, template, expected_gets, params)
      send_message('mass_put', Mass_put_args, :id => id, :template => template, :expected_gets => expected_gets, :params => params)
    end
    def get(target)
      send_get(target)
      return recv_get()
    end

    def send_get(target)
      send_message('get', Get_args, :target => target)
    end

    def recv_get()
      result = receive_message(Get_result)
      return result.success unless result.success.nil?
      raise result.error unless result.error.nil?
      raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'get failed: unknown result')
    end

    def fetch(target, destination, cmdid)
      send_fetch(target, destination, cmdid)
    end

    def send_fetch(target, destination, cmdid)
      send_message('fetch', Fetch_args, :target => target, :destination => destination, :cmdid => cmdid)
    end
    def push_get(result, cmdid)
      send_push_get(result, cmdid)
    end

    def send_push_get(result, cmdid)
      send_message('push_get', Push_get_args, :result => result, :cmdid => cmdid)
    end
    def meta_request(base_cmd, put_list, get_list, params)
      send_meta_request(base_cmd, put_list, get_list, params)
    end

    def send_meta_request(base_cmd, put_list, get_list, params)
      send_message('meta_request', Meta_request_args, :base_cmd => base_cmd, :put_list => put_list, :get_list => get_list, :params => params)
    end
    def aggreget(target, agg)
      send_aggreget(target, agg)
      return recv_aggreget()
    end

    def send_aggreget(target, agg)
      send_message('aggreget', Aggreget_args, :target => target, :agg => agg)
    end

    def recv_aggreget()
      result = receive_message(Aggreget_result)
      return result.success unless result.success.nil?
      raise result.error unless result.error.nil?
      raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'aggreget failed: unknown result')
    end

    def dump()
      send_dump()
      return recv_dump()
    end

    def send_dump()
      send_message('dump', Dump_args)
    end

    def recv_dump()
      result = receive_message(Dump_result)
      return result.success unless result.success.nil?
      raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'dump failed: unknown result')
    end

    def localdump()
      send_localdump()
    end

    def send_localdump()
      send_message('localdump', Localdump_args)
    end
  end

  class Processor
    include ::Thrift::Processor

    def process_put(seqid, iprot, oprot)
      args = read_args(iprot, Put_args)
      @handler.put(args.id, args.template, args.params)
      return
    end

    def process_mass_put(seqid, iprot, oprot)
      args = read_args(iprot, Mass_put_args)
      @handler.mass_put(args.id, args.template, args.expected_gets, args.params)
      return
    end

    def process_get(seqid, iprot, oprot)
      args = read_args(iprot, Get_args)
      result = Get_result.new()
      begin
        result.success = @handler.get(args.target)
      rescue SpreadException => error
        result.error = error
      end
      write_result(result, oprot, 'get', seqid)
    end

    def process_fetch(seqid, iprot, oprot)
      args = read_args(iprot, Fetch_args)
      @handler.fetch(args.target, args.destination, args.cmdid)
      return
    end

    def process_push_get(seqid, iprot, oprot)
      args = read_args(iprot, Push_get_args)
      @handler.push_get(args.result, args.cmdid)
      return
    end

    def process_meta_request(seqid, iprot, oprot)
      args = read_args(iprot, Meta_request_args)
      @handler.meta_request(args.base_cmd, args.put_list, args.get_list, args.params)
      return
    end

    def process_aggreget(seqid, iprot, oprot)
      args = read_args(iprot, Aggreget_args)
      result = Aggreget_result.new()
      begin
        result.success = @handler.aggreget(args.target, args.agg)
      rescue SpreadException => error
        result.error = error
      end
      write_result(result, oprot, 'aggreget', seqid)
    end

    def process_dump(seqid, iprot, oprot)
      args = read_args(iprot, Dump_args)
      result = Dump_result.new()
      result.success = @handler.dump()
      write_result(result, oprot, 'dump', seqid)
    end

    def process_localdump(seqid, iprot, oprot)
      args = read_args(iprot, Localdump_args)
      @handler.localdump()
      return
    end

  end

  # HELPER FUNCTIONS AND STRUCTURES

  class Put_args
    include ::Thrift::Struct
    ID = 1
    TEMPLATE = 2
    PARAMS = 3

    ::Thrift::Struct.field_accessor self, :id, :template, :params
    FIELDS = {
      ID => {:type => ::Thrift::Types::I64, :name => 'id'},
      TEMPLATE => {:type => ::Thrift::Types::I64, :name => 'template'},
      PARAMS => {:type => ::Thrift::Types::LIST, :name => 'params', :element => {:type => ::Thrift::Types::DOUBLE}}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Put_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Mass_put_args
    include ::Thrift::Struct
    ID = 1
    TEMPLATE = 2
    EXPECTED_GETS = 3
    PARAMS = 4

    ::Thrift::Struct.field_accessor self, :id, :template, :expected_gets, :params
    FIELDS = {
      ID => {:type => ::Thrift::Types::I64, :name => 'id'},
      TEMPLATE => {:type => ::Thrift::Types::I64, :name => 'template'},
      EXPECTED_GETS => {:type => ::Thrift::Types::I64, :name => 'expected_gets'},
      PARAMS => {:type => ::Thrift::Types::LIST, :name => 'params', :element => {:type => ::Thrift::Types::DOUBLE}}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Mass_put_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Get_args
    include ::Thrift::Struct
    TARGET = 1

    ::Thrift::Struct.field_accessor self, :target
    FIELDS = {
      TARGET => {:type => ::Thrift::Types::LIST, :name => 'target', :element => {:type => ::Thrift::Types::STRUCT, :class => MapEntry}}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Get_result
    include ::Thrift::Struct
    SUCCESS = 0
    ERROR = 1

    ::Thrift::Struct.field_accessor self, :success, :error
    FIELDS = {
      SUCCESS => {:type => ::Thrift::Types::MAP, :name => 'success', :key => {:type => ::Thrift::Types::STRUCT, :class => MapEntry}, :value => {:type => ::Thrift::Types::DOUBLE}},
      ERROR => {:type => ::Thrift::Types::STRUCT, :name => 'error', :class => SpreadException}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Fetch_args
    include ::Thrift::Struct
    TARGET = 1
    DESTINATION = 2
    CMDID = 3

    ::Thrift::Struct.field_accessor self, :target, :destination, :cmdid
    FIELDS = {
      TARGET => {:type => ::Thrift::Types::LIST, :name => 'target', :element => {:type => ::Thrift::Types::STRUCT, :class => MapEntry}},
      DESTINATION => {:type => ::Thrift::Types::STRUCT, :name => 'destination', :class => NodeID},
      CMDID => {:type => ::Thrift::Types::I64, :name => 'cmdid'}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Fetch_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Push_get_args
    include ::Thrift::Struct
    RESULT = 1
    CMDID = 2

    ::Thrift::Struct.field_accessor self, :result, :cmdid
    FIELDS = {
      RESULT => {:type => ::Thrift::Types::MAP, :name => 'result', :key => {:type => ::Thrift::Types::STRUCT, :class => MapEntry}, :value => {:type => ::Thrift::Types::DOUBLE}},
      CMDID => {:type => ::Thrift::Types::I64, :name => 'cmdid'}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Push_get_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Meta_request_args
    include ::Thrift::Struct
    BASE_CMD = 1
    PUT_LIST = 2
    GET_LIST = 3
    PARAMS = 4

    ::Thrift::Struct.field_accessor self, :base_cmd, :put_list, :get_list, :params
    FIELDS = {
      BASE_CMD => {:type => ::Thrift::Types::I64, :name => 'base_cmd'},
      PUT_LIST => {:type => ::Thrift::Types::LIST, :name => 'put_list', :element => {:type => ::Thrift::Types::STRUCT, :class => PutRequest}},
      GET_LIST => {:type => ::Thrift::Types::LIST, :name => 'get_list', :element => {:type => ::Thrift::Types::STRUCT, :class => GetRequest}},
      PARAMS => {:type => ::Thrift::Types::LIST, :name => 'params', :element => {:type => ::Thrift::Types::DOUBLE}}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Meta_request_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Aggreget_args
    include ::Thrift::Struct
    TARGET = 1
    AGG = 2

    ::Thrift::Struct.field_accessor self, :target, :agg
    FIELDS = {
      TARGET => {:type => ::Thrift::Types::LIST, :name => 'target', :element => {:type => ::Thrift::Types::STRUCT, :class => MapEntry}},
      AGG => {:type => ::Thrift::Types::I32, :name => 'agg', :enum_class => AggregateType}
    }

    def struct_fields; FIELDS; end

    def validate
      unless @agg.nil? || AggregateType::VALID_VALUES.include?(@agg)
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Invalid value of field agg!')
      end
    end

  end

  class Aggreget_result
    include ::Thrift::Struct
    SUCCESS = 0
    ERROR = 1

    ::Thrift::Struct.field_accessor self, :success, :error
    FIELDS = {
      SUCCESS => {:type => ::Thrift::Types::MAP, :name => 'success', :key => {:type => ::Thrift::Types::STRUCT, :class => MapEntry}, :value => {:type => ::Thrift::Types::DOUBLE}},
      ERROR => {:type => ::Thrift::Types::STRUCT, :name => 'error', :class => SpreadException}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Dump_args
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Dump_result
    include ::Thrift::Struct
    SUCCESS = 0

    ::Thrift::Struct.field_accessor self, :success
    FIELDS = {
      SUCCESS => {:type => ::Thrift::Types::STRING, :name => 'success'}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Localdump_args
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Localdump_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

end
