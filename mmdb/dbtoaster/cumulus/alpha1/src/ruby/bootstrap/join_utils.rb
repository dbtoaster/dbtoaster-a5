def has_product_element(cursors)
  cursors.select { |c| c.done } != cursors.size
end

def next_product_element(cursors, last_keys_and_vals)
  read = true
  read_idx = lastvals.length == 0? 0 : -1

  # Find cursors where all successive cursors are done.
  l = cursors.length
  done_cursors = cursors.select do |c|
    c.done and (cursors[cursors.index(c)..-1].all? { |c2| c2.done })
  end

  # Reset successor cursors unless the first cursor is done
  # (in which case we're done with the cross product)
  unless done_cursors.nil? || done_cursors.length == 0 then
    reset_idx = cursors.index(done_cursors.first)
    if reset_idx > 0 then
      read_idx = reset_idx-1
      (reset_idx...cursors.length).each { |i| cursors[i].rewind }
    else read = false end
  end

  # Use prev values, and any new values for cursor reset
  # Note: cursor.next should return: [[keys], val]
  if read then
    last_keys_and_vals[0...read_idx].
      concat(cursors[read_idx..-1].map { |c| c.next })
  else [] end
end

def nljoin(cursors)
  # keys_and_vals = [[[keys], val], ...] , i.e. keys+vals per cursor
  keys_and_vals = []
  while (has_product_element(cursors))
    keys_and_vals = next_product_element(cursors, keys_and_vals)
    
    # mapkeys = [[keys], ... ] , i.e. keys per cursor
    # mapvals = [ val, ... ] , i.e. vals per cursor
    mapkeys = keys_and_vals.collect { |kv| keys = kv[0] }
    mapvals = keys_and_vals.collect { |kv| vals = kv[1] }
    yield mapkeys, mapvals
  end
end