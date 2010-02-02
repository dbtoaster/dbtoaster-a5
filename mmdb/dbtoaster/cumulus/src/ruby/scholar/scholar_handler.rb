class ScholarNodeHandler
  include Java::org::dbtoaster::cumulus::scholar::ScholarNode::ScholarNodeIFace;
  include CLogMixins;
  self.logger_segment = "Scholar";

  def initialize(result_file)
    @output = open(result_file, "w+")
    @requests = Hash.new
    @request_counter = 0
  end

  def push_results(results, cmdid)
    unless @requests.key?(cmdid) then
      @requests[cmdid] = @request_counter;
      @request_counter += 1;
    end
    debug { "Got #{results.size.to_s} results for request #{cmdid.to_s}" }
    results.each do |entry_value|
      debug { "#{entry_value[0].to_s},#{entry_value[1].to_s}" }
      @output.write("#{@requests[cmdid].to_s},#{entry_value[0].to_s},#{entry_value[1].to_s}\n");
    end
    @output.flush
  end
end