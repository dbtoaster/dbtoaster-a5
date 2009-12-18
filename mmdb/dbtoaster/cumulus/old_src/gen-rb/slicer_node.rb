#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

require 'thrift'
require 'spread_types'

module SlicerNode
  class Client
    include ::Thrift::Client

    def start_switch()
      send_start_switch()
    end

    def send_start_switch()
      send_message('start_switch', Start_switch_args)
    end
    def start_node(port)
      send_start_node(port)
    end

    def send_start_node(port)
      send_message('start_node', Start_node_args, :port => port)
    end
    def start_client()
      send_start_client()
    end

    def send_start_client()
      send_message('start_client', Start_client_args)
    end
    def shutdown()
      send_shutdown()
      recv_shutdown()
    end

    def send_shutdown()
      send_message('shutdown', Shutdown_args)
    end

    def recv_shutdown()
      result = receive_message(Shutdown_result)
      return
    end

    def start_logging(target)
      send_start_logging(target)
    end

    def send_start_logging(target)
      send_message('start_logging', Start_logging_args, :target => target)
    end
    def receive_log(log_message)
      send_receive_log(log_message)
    end

    def send_receive_log(log_message)
      send_message('receive_log', Receive_log_args, :log_message => log_message)
    end
    def poll_stats()
      send_poll_stats()
      return recv_poll_stats()
    end

    def send_poll_stats()
      send_message('poll_stats', Poll_stats_args)
    end

    def recv_poll_stats()
      result = receive_message(Poll_stats_result)
      return result.success unless result.success.nil?
      raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'poll_stats failed: unknown result')
    end

  end

  class Processor
    include ::Thrift::Processor

    def process_start_switch(seqid, iprot, oprot)
      args = read_args(iprot, Start_switch_args)
      @handler.start_switch()
      return
    end

    def process_start_node(seqid, iprot, oprot)
      args = read_args(iprot, Start_node_args)
      @handler.start_node(args.port)
      return
    end

    def process_start_client(seqid, iprot, oprot)
      args = read_args(iprot, Start_client_args)
      @handler.start_client()
      return
    end

    def process_shutdown(seqid, iprot, oprot)
      args = read_args(iprot, Shutdown_args)
      result = Shutdown_result.new()
      @handler.shutdown()
      write_result(result, oprot, 'shutdown', seqid)
    end

    def process_start_logging(seqid, iprot, oprot)
      args = read_args(iprot, Start_logging_args)
      @handler.start_logging(args.target)
      return
    end

    def process_receive_log(seqid, iprot, oprot)
      args = read_args(iprot, Receive_log_args)
      @handler.receive_log(args.log_message)
      return
    end

    def process_poll_stats(seqid, iprot, oprot)
      args = read_args(iprot, Poll_stats_args)
      result = Poll_stats_result.new()
      result.success = @handler.poll_stats()
      write_result(result, oprot, 'poll_stats', seqid)
    end

  end

  # HELPER FUNCTIONS AND STRUCTURES

  class Start_switch_args
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Start_switch_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Start_node_args
    include ::Thrift::Struct
    PORT = 1

    ::Thrift::Struct.field_accessor self, :port
    FIELDS = {
      PORT => {:type => ::Thrift::Types::I32, :name => 'port'}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Start_node_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Start_client_args
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Start_client_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Shutdown_args
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Shutdown_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Start_logging_args
    include ::Thrift::Struct
    TARGET = 1

    ::Thrift::Struct.field_accessor self, :target
    FIELDS = {
      TARGET => {:type => ::Thrift::Types::STRUCT, :name => 'target', :class => NodeID}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Start_logging_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Receive_log_args
    include ::Thrift::Struct
    LOG_MESSAGE = 1

    ::Thrift::Struct.field_accessor self, :log_message
    FIELDS = {
      LOG_MESSAGE => {:type => ::Thrift::Types::STRING, :name => 'log_message'}
    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Receive_log_result
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Poll_stats_args
    include ::Thrift::Struct

    FIELDS = {

    }

    def struct_fields; FIELDS; end

    def validate
    end

  end

  class Poll_stats_result
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

end
