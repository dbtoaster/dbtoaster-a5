class ScholarNodeHandler
  include Java::org::dbtoaster::cumulus::scholar::ScholarNode::ScholarNodeIFace;
  include CLogMixins;
  self.logger_segment = "Scholar";

  def push_results(results, cmdid)
    results.each_pair do |entry,value|
      puts "#{entry.to_s},#{value.to_s}";
    end
  end
end

handler = ScholarNodeHandler.new();
return handler;