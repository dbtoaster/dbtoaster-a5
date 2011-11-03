prefs = { 
  "servers" => 500,
  "tasks"   => 1000,
  "s_per_t" => 10,
  "p_down"  => 0.25
}

servers = (0...prefs["servers"]).map do |s| 
  [s, (rand < prefs["p_down"]) ? 0 : 1];
end

tasks = (0...prefs["tasks"]).map do |t|
  [t, rand(3)];
end

slot_cnt = (prefs["tasks"] * prefs["s_per_t"]);
slots_per_s = slot_cnt / servers.count;

slots = servers.map do |s,up| 
  Array.new(slots_per_s, s);
end.flatten(1);

assignments = tasks.map do |t,priority|
  (0...prefs["s_per_t"]).map do |i|
    [slots.delete_at(rand(slots.count)), t];
  end
end.flatten(1)

def dump_table(t, name)
  File.open("#{File.dirname $0}/../data/ca_#{name}.dat", "w+") do |f|
    f.write(t.map { |r| r.join(",") }.join("\n") + "\n");
    f.flush;
  end
end

dump_table(servers,"servers");
dump_table(tasks,"tasks");
dump_table(assignments,"assignments");

capacity = tasks.map { |t| { "assigned" => 0, "available" => 0 } };

assignments.each do |s,t|
  capacity[t]["assigned"] = capacity[t]["assigned"]+1;
  capacity[t]["available"] = capacity[t]["available"]+1 if (servers[s][1] == 1);
end

print "--- Correct Answer (Task,Assigned,Available) ---\n"
capacity.each_index do |c|
  if (capacity[c]["available"].to_f / capacity[c]["assigned"].to_f) < 0.5 then
    print "#{c},#{capacity[c]["assigned"]},#{capacity[c]["available"]}\n"
  end
end