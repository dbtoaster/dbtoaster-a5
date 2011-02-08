
require 'getoptlong'
require 'util/ok_mixins'

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
    (@line != nil) # && @deps.assert { |d| d[1].finished_key? @line[d[0]].to_i }
  end
end

tables = Array.new;
[ 
  ["LINEITEM"  , "lineitem.tbl", "lineitem.tbl.u1", 6000000, nil   ,        ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"] ],
  ["ORDERS"    , "orders.tbl"  , "order.tbl.u1"   , 1500000, "o_orderkey",  ["o_orderkey", "o_custkey", "o_status", "o_totalprice", "o_orderdate", "o_opriority", "o_clerk", "o_spriority", "o_comment"] ],
  ["PART"      , "part.tbl"    , nil              , 200000 , "p_partkey",   ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"] ],
  ["PARTSUPP"  , "partsupp.tbl", nil              , 800000 , nil   ,        ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"] ],
  ["CUSTOMER"  , "customer.tbl", nil              , 150000 , "c_custkey",   ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"] ],
  ["SUPPLIER"  , "supplier.tbl", nil              , 10000  , "s_suppkey",   ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"] ],
  ["REGION"    , "region.tbl"  , nil              , 5      , "r_regionkey", ["r_regionkey", "r_name", "r_comment"] ],
  ["NATION"    , "nation.tbl"  , nil              , 25     , "n_nationkey", ["n_nationkey", "n_name", "n_regionkey", "n_comment"] ]
].each do |t| 
  tables.push(TableInfo.new(t, tables));
end


GetoptLong.new(
  [ "--REGION"         , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--NATION"         , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--CUSTOMER"       , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--ORDERS"         , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--LINEITEM"       , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--PART"           , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--SUPPLIER"       , GetoptLong::OPTIONAL_ARGUMENT],
  [ "--PARTSUPP"       , GetoptLong::OPTIONAL_ARGUMENT],
  [ "-d", "--data"     , GetoptLong::REQUIRED_ARGUMENT]
).each do |opt, arg|
  #puts "Found OPT: #{opt}=#{arg}";
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
