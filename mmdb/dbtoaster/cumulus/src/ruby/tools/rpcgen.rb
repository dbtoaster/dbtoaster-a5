
raise "Error: RPC file argument required" if ARGV.empty?

messages = Array.new;

class RPCParam
  attr_reader :name;
  def initialize(param)
    param = param.chomp.split(" ");
    @name = param.pop;
    @type = 
      case param.join(" ")
        when /List *< *(\w+) *>/ then @wrapper = :list; $~[1]
        when /.+/ then @wrapper = :none; $~[0]
      end
  end
  
  def to_s
    "#{full_type} #{@name}"
  end
  
  def full_type
    case @wrapper
      when :list then "List<#{@type}>";
      when :none then "#{@type}";
    end
  end
  
  def RPCParam.put_type(type, wrapper=:none)
    case wrapper
      when :list then "List"
      when :none then
        case type
          when "Long"   then "Long"
          when "int"    then "Integer"
          when "Double" then "Double"
          when "String" then "Object"
          else raise "Unknown type: #{type}"
        end
      end
  end
  
  def export_encoder
    "oprot.put#{RPCParam.put_type(@type, @wrapper)}(#{@name});"
  end
  
  def generic_encoder?
    RPCParam.put_type(@type, @wrapper) == "Object"
  end
  
  def export_decoder
    "#{self} = #{if generic_encoder? then "("+full_type+")" else "" end}iprot." +
    case @wrapper
      when :list then "getList(#{@type}.class);"
      when :none then "get#{RPCParam.put_type(@type, @wrapper)}();"
    end
  end
end

class RPCMessage
  attr_reader :rpc_name;
  def initialize(ret_type, rpc_name, params)
    @ret_type, @rpc_name = ret_type, rpc_name;
    params = params.split(/, */) unless params.is_a? Array;
    @params = params.collect { |param| RPCParam.new(param) }
  end
  
  def to_s
    "#{@ret_type} #{@rpc_name}(#{@params.collect { |param| param.to_s }.join(", ")})";
  end
  
  def export_enum
    @rpc_name.upcase;
  end
  
  def export_iface(indent = "")
    "#{indent}public #{to_s}\n#{indent}  throws TException;"
  end
  
  def export_param_string
    @params.collect do |param|
      "+"+param.name
    end.join("+\", \"");
  end
  
  def export_encoder(indent)
    "#{indent}public #{to_s}\n" +
    "#{indent}  throws TException\n" +
    "#{indent}{\n" +
    "#{indent}  try {\n" +
    "#{indent}    #{$class_name}.logger.debug(\"Sending #{export_enum}(\"#{export_param_string}+\")\");\n" +
    if @params.size > 0 then
      "#{indent}    oprot.beginMessage();\n" +
      "#{indent}    oprot.putObject(#{$class_name}Method.#{export_enum});\n" +
      @params.collect { |param| "#{indent}    " + param.export_encoder + "\n" }.join("") +
      "#{indent}    oprot.endMessage();\n"
    else
      "#{indent}    oprot.putObject(#{$class_name}Method.#{export_enum});\n"
    end +
    if @ret_type != "void" then 
      "#{indent}    waitForFrame();\n" +
      "#{indent}    return (#{@ret_type})iprot.getObject();\n" +
      "#{indent}  } catch (TProtocolException e) { throw new TException(e.getMessage());\n" +
      "#{indent}  } catch (IOException e) { throw new TException(e.getMessage()); }\n"
    else
      "#{indent}  } catch (TProtocolException e) { throw new TException(e.getMessage()); }\n"
    end +
    "#{indent}}\n"
  end
  
  def export_decoder(indent)    
    "#{indent}private class #{@rpc_name} extends TProcessor.HandlerFunction\n" +
    "#{indent}{\n" +
    "#{indent}  public void process(TProtocol iprot, TProtocol oprot)\n" +
    "#{indent}    throws TException, TProtocolException\n" +
    "#{indent}  {\n" + 
    @params.collect { |param| "#{indent}    #{param.export_decoder}\n"}.join("") + 
    "#{indent}    #{$class_name}.logger.debug(\"Received #{export_enum}(\"#{export_param_string}+\")\");\n" +
    "#{indent}    #{if @ret_type != "void" then "oprot.putObject(" end }handler.#{@rpc_name}(#{@params.collect { |param| param.name }.join(", ")})#{if @ret_type != "void" then ")" end};\n" +
    "#{indent}  }\n" +
    "#{indent}}\n";
  end
end

$class_name = File.basename(ARGV[0], ".rpc")
$package_name = File.basename(File.dirname(ARGV[0]));
$out = $stdout;

$messages = 
  File.new(ARGV[0]).read.scan(/(\w+) (\w+)\(([^)]*)\)/).collect { |ret_type, rpc_name, params| RPCMessage.new(ret_type, rpc_name, params) };


$out.puts "package org.dbtoaster.cumulus.#{$package_name};";
$out.puts "";
[
  "java.util.HashMap",
  "java.util.List",
  "java.util.ArrayList",
  "java.util.Map",
  "java.io.IOException",
  "java.net.InetSocketAddress",
  "java.nio.channels.Selector",
  "org.apache.log4j.Logger",
  "org.dbtoaster.cumulus.net.NetTypes.*",
  "org.dbtoaster.cumulus.net.TProtocol.TProtocolException",
  "org.dbtoaster.cumulus.net.Server",
  "org.dbtoaster.cumulus.net.TException",
  "org.dbtoaster.cumulus.net.TProcessor",
  "org.dbtoaster.cumulus.net.TProtocol",
  "org.dbtoaster.cumulus.net.SpreadException",
  "org.dbtoaster.cumulus.net.Client",
  "org.dbtoaster.cumulus.config.CumulusConfig"
].each { |package| $out.puts "import #{package};" }
$out.puts "";
$out.puts "public class #{$class_name}";
$out.puts "{";
$out.puts "  protected static final Logger logger = Logger.getLogger(\"dbtoaster.#{$package_name.capitalize}.#{$class_name}\");";
$out.puts "";
$out.puts "  public interface #{$class_name}IFace";
$out.puts "  {";
$out.puts($messages.collect { |message| message.export_iface("    ") }.join("\n\n"));
$out.puts "  }";
$out.puts "";
$out.puts "  public static #{$class_name}Client getClient(InetSocketAddress addr) throws IOException {";
$out.puts "    return Client.get(addr, 1, #{$class_name}Client.class);";
$out.puts "  }";
$out.puts "";
$out.puts "  public static #{$class_name}Client getClient(InetSocketAddress addr, Integer sendFrameBatchSize)";
$out.puts "    throws IOException {";
$out.puts "    return Client.get(addr, sendFrameBatchSize, #{$class_name}Client.class);";
$out.puts "  }";
$out.puts "";
$out.puts "  public static class #{$class_name}Client extends Client implements #{$class_name}IFace";
$out.puts "  {";
$out.puts "    public #{$class_name}Client(InetSocketAddress s, Integer sendFrameBatchSize, Selector selector)";
$out.puts "      throws IOException";
$out.puts "    {";
$out.puts "      super(s, sendFrameBatchSize, selector);";
$out.puts "    }";
$out.puts $messages.collect { |m| m.export_encoder("    ") }.join("\n");
$out.puts "  }";
$out.puts "";
$out.puts "  public static enum #{$class_name}Method {"
$out.puts "    #{$messages.collect { |m| m.export_enum }.join(",")}"
$out.puts "  };";
$out.puts "";
$out.puts "  public static class Processor extends TProcessor<#{$class_name}Method>";
$out.puts "  {";
$out.puts "    private #{$class_name}IFace handler;";
$out.puts "";
$out.puts "    public Processor(#{$class_name}IFace h)";
$out.puts "    {";
$out.puts "      handler = h;";
$messages.each { |m|  $out.puts "      handlerMap.put(#{$class_name}Method.#{m.export_enum}, new #{m.rpc_name}());" };
$out.puts "    }";
$out.puts $messages.collect { |m| m.export_decoder("    ") }.join("\n");
$out.puts "  }";
$out.puts "";
$out.puts "  public static void main(String args[]) throws Exception";
$out.puts "  {";
$out.puts "    CumulusConfig conf = new CumulusConfig();";
$out.puts "    conf.configure(args);";
$out.puts "    logger.debug(\"Starting #{$class_name} server\");";
$out.puts "    #{$class_name}IFace handler = conf.loadRubyObject(\"#{$package_name}/#{$package_name}.rb\", #{$class_name}IFace.class);";
$out.puts "    Server s = new Server(new #{$class_name}.Processor(handler), 52981);";
$out.puts "    Thread t = new Thread(s);";
$out.puts "";
$out.puts "    t.start();";
$out.puts "    t.join();";
$out.puts "  }";
$out.puts "}";

