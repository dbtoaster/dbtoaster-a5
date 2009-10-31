
require 'getoptlong'
require 'ok_mixins'

data_dir = File.basename(__FILE__) + "../../data";

class TableInfo
  attr_reader :name, :data, :update, :relsize, :columns, :mode, :line, :data_file, :update_file, :key, :keycol, :active;
  attr_writer :mode, :active;
  
  def initialize(info, dep_sources)
    @name, @data, @update, @relsize, @key, @columns = info;
    @mode = :none;
    @keycol = @columns.index(@key);
    @active = false;

    @deps = dep_sources.collect do |source|
      if source.key && (@columns.include? source.key) then
        [ @columns.index(source.key), source ]
      else
        nil
      end
    end.compact;
  end
  
  def cmd_arg
    "--" + @name.upcase;
  end
  
  def open(data_dir)
    if @mode != :none then
      @data_file = File.open(data_dir + "/" + @data);
      @update_file = File.open(data_dir + "/" + @update) if @update && (@mode == :update);
      next_line;
    end
  end
  
  def start_updates
    @data_file = @update_file if @update && (@mode == :update);
    next_line;
  end
  
  def next_line
    ret = @line && @name + "(" + @line.join(",") + ")";
    @line = unless @data_file.eof? then @data_file.gets.chomp.split("|").collect { |col| col.gsub(",", "|") } end;
    ret;
  end
  
  def dump
    while @line
      puts next_line;
    end
  end
  
  def finished_key?(cmp)
    !@active || (@line == nil) || @line[@keycol].to_i > cmp;
  end
  
  def valid
    @line && @deps.assert { |d| d[1].finished_key? @line[d[0]].to_i }
  end
end

tables = Array.new;
[ 
  [ "REGIONS"  , "region.tbl"  , nil              , 5      , "rkey", ["rkey", "name", "comment"] ],
  [ "NATIONS"  , "nation.tbl"  , nil              , 25     , "nkey", ["nkey", "name", "rkey", "comment"] ],
  [ "CUSTOMERS", "customer.tbl", nil              , 150000 , "ckey", ["ckey", "name", "address", "nkey", "phone", "acctbal", "mktsegment", "comment"] ],
  [ "ORDERS"   , "orders.tbl"  , "order.tbl.u1"   , 1500000, "okey", ["okey", "ckey", "status", "totalprice", "orderdate", "opriority", "clerk", "spriority", "comment"] ],
  [ "PARTS"    , "part.tbl"    , nil              , 200000 , "pkey", ["pkey", "name", "mggr", "brand", "type", "size", "container", "retailprice", "comment"] ],
  [ "SUPP"     , "supplier.tbl", nil              , 10000  , "skey", ["skey", "name", "address", "nationkey", "phone", "acctbal", "comment"] ],
  [ "LINEITEMS", "lineitem.tbl", "lineitem.tbl.u1", 6000000, nil   , ["okey", "pkey", "skey", "linenumber", "quantity", "extendedprice", "discount", "tax", "returnflag", "linestatus", "shipdate", "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment"] ],
  [ "PARTSUPP" , "partsupp.tbl", nil              , 800000 , nil   , ["pkey", "skey", "availqty", "supplycost", "comment"] ],
].each do |t| 
  tables.push(TableInfo.new(t, tables));
end


GetoptLong.new(
  [ "--REGIONS"        , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--NATIONS"        , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--CUSTOMERS"      , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--ORDERS"         , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--LINEITEMS"      , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--PARTS"          , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--SUPP"           , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--PARTSUPP"       , GetoptLong::OPTIONAL_ARGUMENT],
  [ "-d", "--data"     , GetoptLong::REQUIRED_ARGUMENT]
).each do |opt, arg|
  case opt
    when "-d", "--data" then
      data_dir = arg;
    else
      table = tables.find { |t| t if opt == t.cmd_arg }
      if table then 
        table.mode = 
          case arg
            when "update"              then :update
            when "interleave", nil, "" then :interleave
            when "upfront"             then :upfront
            else                            :none
          end
        table.active = true;
      end
  end
end

# Prep the data files
tables.each { |t| t.open(data_dir) }

# First the upfront tables and the update basis. 
tables.each do |t| 
  if t.mode == :update || t.mode == :upfront then
    t.dump;
  end
end

# Now the interleaved tables
def interleave(tables)
  valid = [nil];
  until valid.empty?
    count = 0;
    valid = tables.collect do |t| 
      if t.valid then
        count += t.relsize
        t
      end
    end.compact
    unless valid.empty?
      count = rand(count);
      puts valid.find { |t| t.next_line if (count -= t.relsize) < 0 }
    end
  end
end

interleave(tables.collect { |t| t if t.mode == :interleave }.compact)

# Finally, the updates
interleave(tables.collect { |t| if t.mode == :update then t.start_updates; t end }.compact);
