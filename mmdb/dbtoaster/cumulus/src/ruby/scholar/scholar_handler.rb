class ScholarNodeHandler
  include Java::org::dbtoaster::cumulus::scholar::ScholarNode::ScholarNodeIFace;
  include CLogMixins;
  self.logger_segment = "Scholar";

  def push_results(results, cmdid)
    info { "Got #{results.size.to_s} results for request #{cmdid.to_s}" }
    results.each do |entry_value|
      puts "#{entry_value[0].to_s},#{entry_value[1].to_s}";
    end
  end
end