include Java;

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan

# HBase options for creating secondary indexes:
# -- IndexedTable
# -- key duplication + filters:
#    Keys are multi-dimensional, values are dimensions of keys, and the map
#    value. Slicing can then be done by applying filters to the key dimensions
#    stored as values.
# Ideally we would use a filter that can compare parts of a complex key, but
# HBase doesn't seem to have one, and I don't want to write one for now...

class HBaseHandle
  include Singleton
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
      htd = HTableDesriptor.new(name)
      (0..@numkeys).each do |i|
        htd.addFamily(HColumnDescriptor.new("key#{i.to_s}:"))
      end
      htd.addFamily(HColumnDescriptor.new("value:"))
      hbase_hndl.hbadmin.createTable(htd)
    end
    @htable = HTable.new(hbase_hndl.hbconf, name)
    
    @wildcard = wildcard

    @patterns = []
    patterns.each do |pattern|
      @patterns << scand
    end
  end
  
  def add_pattern(pattern)
    @java_impl.add_pattern((pattern.is_a? Array) ? pattern.to_java(:Long) : pattern)
    # TODO: create a Scan object for the pattern.
    @patterns << pattern
  end
  
  def [](key)
    getd = Get.new((key.is_a? Array) ? key.to_java(:byte) : key)
    getd.addColumn(Bytes.toBytes("value:"))
    result = @htable.get(getd)
    Bytes.toDouble(result.getValue())
  end
  
  def []=(key, val)
    putd = Put.new((key.is_a? Array) ? key.to_java(:byte) : key)
    key.each_index do |k_i| putd.add(Bytes.toBytes("key{#k_i.to_s}:"), Bytes.toBytes(key[k_i])) end
    putd.add(Bytes.toBytes("value:"), Bytes.toBytes(val))
    @htable.put(putd)
  end
  
  def has_key?(key)
    getd = Get.new((key.is_a? Array) ? key.to_java(:byte) : key)
    getd.addColumn(Bytes.toBytes("value:"))
    @htable.exists(getd)
  end
  
  def values
    ret = Hash.new;
    result_scanner = @htable.getScanner(Scan.new()); 
    scanner = result_scanner.iterator();
    while (scanner.hasNext())
      r = scanner.next();
      ret[r.getRow()] = r.getValue(Bytes.toBytes("value:"));
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
    (0..@numkeys).each do |i| scand.addColumn("key#{i.to_s}:".to_java_bytes) end
    scand.addColumn("value:".to_java_bytes)
    
    filterlist = FilterList.new()
    partial_key.each_index do |k_i|
      unless partial_key[k_i] == @wildcard then
        filterList.addFilter(SingleColumnValueFilter.new(
          "key#{k_i.to_s}:", nil, CompareFilter.EQUAL, partial_key[k_i]));
      end 
    end
    scand.setFilter(filterList)
    
    result_scanner = @htable.getScanner(scand);
    scanner = result_scanner.iterator();
    while (scanner.hasNext())
      r = scanner.next();
      keys = (0..@numkeys).collect { |i| r.getValue(Bytes.toBytes("key#{i.to_s}:")) }
      value = r.getValue(Bytes.toBytes("value:"));
      yield keys, value.to_f;
    end
    result_scanner.close();
  end
  
  def replace(partial_key)
    scand = Scan.new()
    (0..@numkeys).each do |i| scand.addColumn("key#{i.to_s}:".to_java_bytes) end
    scand.addColumn("value:".to_java_bytes)

    filterList = FilterList.new()
    partial_key.each_index do |k_i|
      unless partial_key[k_i] == @wildcard then
        filterList.addFilter(SingleColumnValueFilter.new(
          "key#{k_i.to_s}:", nil, CompareFilter.EQUAL, partial_key[k_i]));
      end 
    end
    scand.setFilterList(filterList)

    result_scanner = @htable.getScanner(scand);
    scanner = result_scanner.iterator();
    while (scanner.hasNext())
      r = scanner.next();
      keys = (0..@numkeys).collect { |i| r.getValue(Bytes.toBytes("key#{i.to_s}:")) }
      value = r.getValue(Bytes.toBytes("value:"));
      new_value = yield keys, value.to_f;
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
    (0..@numkeys).each do |i| scand.addColumn("key#{i.to_s}:".to_java_bytes) end
    scand.addColumn("value:".to_java_bytes)

    filterList = FilterList.new()
    partial_key.each_index do |k_i|
      unless partial_key[k_i] == @wildcard then
        filterList.addFilter(SingleColumnValueFilter.new(
          "key#{k_i.to_s}:", nil, CompareFilter.EQUAL, partial_key[k_i]));
      end 
    end
    scand.setFilterList(filterList)

    result_scanner = @htable.getScanner(scand);
    result_scanner.iterator()
  end

end