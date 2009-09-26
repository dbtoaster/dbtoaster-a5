require 'spread_types'

class NodeID
  def to_s
    host + ":" + port.to_s;
  end
end

class Entry 
  def to_s
    @source.to_s + "[" + @key.to_s + "(" + @version.to_s + ")]" +
      if @node != nil then "@" + @node.to_s else "" end;
  end
end