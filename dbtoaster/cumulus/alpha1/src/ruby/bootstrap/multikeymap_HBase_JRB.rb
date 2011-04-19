require 'singleton'

include Java;

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter

# HBase options for creating secondary indexes:
# -- IndexedTable
# -- key duplication + filters:
#    Keys are multi-dimensional, values are dimensions of keys, and the map
#    value. Slicing can then be done by applying filters to the key dimensions
#    stored as values.
# Ideally we would use a filter that can compare parts of a complex key, but
# HBase doesn't seem to have one, and I don't want to write one for now...

# Note we don't keep any state for patterns with this approach, i.e. secondary
# indexes etc, so we can simply slice as needed, ad-hoc.

class HBaseHandle
  include Singleton
  
  attr_reader :hbconf, :hbadmin

  def initialize()
    @hbconf = HBaseConfiguration.new()
    @hbadmin = HBaseAdmin.new(@hbconf)
  end
end

class MultiKeyMap 

  def initialize(numkeys, patterns, name = "", default = nil, wildcard = -1)
    hbase_hndl = HBaseHandle.instance
    @numkeys = numkeys
    if not(hbase_hndl.hbadmin.tableExists(name)) then
      htd = HTableDescriptor.new(name)
      (0...@numkeys).each do |i|
        htd.addFamily(HColumnDescriptor.new(Bytes.toBytes("key#{i.to_s}")))
      end
      htd.addFamily(HColumnDescriptor.new(Bytes.toBytes("value")))
      hbase_hndl.hbadmin.createTable(htd)
    end

    @htable = HTable.new(hbase_hndl.hbconf, name)
    @wildcard = wildcard
    #@patterns = patterns
  end
  
  def add_pattern(pattern)
    #@patterns << pattern
  end
  
  def get_key_bytes(key)
    if (key.is_a? Array) then
      key_bb = java.nio.ByteBuffer.allocate(key.size*java.lang.Long::SIZE)
      key_bb.asLongBuffer.put(key.to_java(:long))
      key_bb = key_bb.array()
    else
      key_bb = Bytes.toBytes(key)
    end
    key_bb
  end

  def [](key)
    getd = Get.new(get_key_bytes(key))
    getd.addColumn(Bytes.toBytes("value"))
    result = @htable.get(getd)
    Bytes.toDouble(result.value())
  end
  
  def []=(key, val)
    putd = Put.new(get_key_bytes(key))
    key.each_index do |k_i| putd.add(Bytes.toBytes("key#{k_i.to_s}"), nil, Bytes.toBytes(key[k_i])) end
    putd.add(Bytes.toBytes("value"), nil, Bytes.toBytes(val))
    @htable.put(putd)
  end
  
  def has_key?(key)
    getd = Get.new(get_key_bytes(key))
    getd.addColumn(Bytes.toBytes("value"))
    @htable.exists(getd)
  end
  
  def values
    ret = Hash.new;
    result_scanner = @htable.getScanner(Scan.new()); 
    scanner = result_scanner.iterator();
    while (scanner.hasNext())
      r = scanner.next();
      ret[r.getRow()] = r.getValue(Bytes.toBytes("value"));
    end
    result_scanner.close();
    ret
  end
  
  def scan(partial_key)
    unless partial_key.include? @wildcard then
      yield(partial_key, self[partial_key]);
      return;
    end
    scand = Scan.new()
    (0...@numkeys).each do |i| scand.addColumn("key#{i.to_s}".to_java_bytes) end
    scand.addColumn("value".to_java_bytes)
    
    filter_list = FilterList.new()
    partial_key.each_index do |k_i|
      unless partial_key[k_i] == @wildcard then
        filter_list.addFilter(SingleColumnValueFilter.new(
          Bytes.toBytes("key#{k_i.to_s}"), nil,
          CompareFilter::CompareOp::EQUAL, Bytes.toBytes(partial_key[k_i])));
      end 
    end
    scand.setFilter(filter_list)
    
    result_scanner = @htable.getScanner(scand);
    scanner = result_scanner.iterator();
    while (scanner.hasNext())
      r = scanner.next();
      keys = (0...@numkeys).collect { |i| Bytes.toLong(r.getValue(Bytes.toBytes("key#{i.to_s}"))) }
      value = Bytes.toDouble(r.getValue(Bytes.toBytes("value")));
      yield keys, value;
    end
    result_scanner.close();
  end
  
  def replace(partial_key)
    scand = Scan.new()
    (0...@numkeys).each do |i| scand.addColumn("key#{i.to_s}".to_java_bytes) end
    scand.addColumn("value".to_java_bytes)

    filter_list = FilterList.new()
    partial_key.each_index do |k_i|
      unless partial_key[k_i] == @wildcard then
        filter_list.addFilter(SingleColumnValueFilter.new(
          Bytes.toBytes("key#{k_i.to_s}"), nil,
          CompareFilter::CompareOp::EQUAL, Bytes.toBytes(partial_key[k_i])));
      end 
    end
    scand.setFilterList(filter_list)

    result_scanner = @htable.getScanner(scand);
    scanner = result_scanner.iterator();
    while (scanner.hasNext())
      r = scanner.next();
      keys = (0...@numkeys).collect { |i| Bytes.toLong(r.getValue(Bytes.toBytes("key#{i.to_s}"))) }
      value = Bytes.toDouble(r.getValue(Bytes.toBytes("value")));
      new_value = yield keys, value;
      put(keys, new_value)
    end
    result_scanner.close();
  end
  
  def close
    @htable.close()
  end
  
  def sync
    @htable.flushCommits()
  end
  
  # Methods to return cursors (i.e. iterables) themselves.
  def cursor(partial_key)
    scand = Scan.new()
    (0...@numkeys).each do |i| scand.addColumn("key#{i.to_s}".to_java_bytes) end
    scand.addColumn("value".to_java_bytes)

    filter_list = FilterList.new()
    partial_key.each_index do |k_i|
      unless partial_key[k_i] == @wildcard then
        filter_list.addFilter(SingleColumnValueFilter.new(
          Bytes.toBytes("key#{k_i.to_s}"), nil,
          CompareFilter::CompareOp::EQUAL, Bytes.toBytes(partial_key[k_i])));
      end 
    end
    scand.setFilterList(filter_list)

    result_scanner = @htable.getScanner(scand);
    result_scanner.iterator()
  end

end

def test
  m = MultiKeyMap.new(2, [], "x")
  keys = [[1000000,1000000], [1000000,2000000], [2000000,2000000]]
  val = 1.0
  keys.each do |k|  m[k] = val; val += 1.0; end
  keys.each do |k|  x = m[k]; puts "Key #{k}: #{x}"; end
  slices = [[1000000,-1], [-1,2000000]]
  slices.each do |s| m.scan(s) { |k,v| puts "Key #{k}: #{v.to_s}" } end
end

# Unit test
#test
