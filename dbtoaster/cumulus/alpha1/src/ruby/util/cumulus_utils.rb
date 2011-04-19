class CumulusUtils
  def CumulusEntry.parse_map_entry(string)
    parsed = / *(Map)? *([0-9]+) *\[([^\]]*)\]/.match(string);
    MapEntry.new(parsed[2], parsed[3].split(",").collect { |i| i.to_i });
  end
end