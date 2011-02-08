require 'getoptlong';
require 'util/ok_mixins';
require 'config/config';
require 'config/template';
require 'fileutils';


###############################
# Bootstrap mapreduce code gen

class HadoopCompiler
  include CLogMixins

  attr_reader :stages;
  
  def initialize()
    @stages = []
    @tab = ' ' * 4;
  end

  def normalize_keys(keys)
    keys.collect do |k|
      case k when TemplateVariable then k.name;
        else k.to_s;
      end
    end
  end
  
  def translate_path_to_hdfs(path)
    hdfs_path = path;
    hdfs_path = File.expand_path(hdfs_path).gsub(File.expand_path("~"), "") unless (path =~ /~/).nil?;
    hdfs_path = File.join($config.hadoop_dfs_path, hdfs_path) unless (hdfs_path[0].chr == '/' || hdfs_path =~ /^hdfs:\/\//);
    hdfs_path  
  end

  def compile_template(template, patterns)
    # stage {
    #    jobname,
    #    in_file (in rel file), out_file (out dbt map file), 
    #    in_path (mapper input path), out_path (reducer out path),
    #    extra_files?,
    #    in_fields, out_fields,
    #    reduce_in_fields?, reduce_out_fields?,
    #    input_maps,
    #    builtin_map, map_init_code?, map_code, map_final_code?,
    #    builtin_reduce, reduce_init_code?,
    #    reduce_group_init_code, reduce_code, reduce_group_final_code,
    #    reduce_final_code?,
    # }

    # common:
    #    stage = {
    #        jobname => bootstrap_map<map id>
    #        in_file => source file
    #        out_file => jobname / map name / map
    #        in_path => jobname / rel name / chunks
    #        out_path => jobname / map name / chunks
    #        in_fields => relation schema
    #        out_fields => map keys, agg value
    #        builtin_map => false
    #        builtin_reduce => false
    #    }

    relname = template.relation
    relfile = "#{relname.downcase}.tbl"
    mapname = "map#{template.target.source.to_s}"
    mapfile = "#{mapname}.dump"

    stage = Hash.new
    stage["jobname"]         = "Bootstrap#{mapname.capitalize}";
    
    #  Relation input file
    stage["in_file"]         = File.join("$config.sources", relfile);
    
    #  Map output file
    stage["out_file"]        = File.join("$config.outputs", mapfile);
    
    #  MR input path for relation file
    source_rel_path          = translate_path_to_hdfs($config.client_debug["sourcedir"])
    stage["in_path"]         = File.join([source_rel_path, relname, "chunks"]); 
    
    #  MR output path for map
    map_out_path             = translate_path_to_hdfs($config.client_debug["targetdir"])
    stage["out_path"]        = File.join([map_out_path, stage["jobname"], mapname, "chunks"]);

    stage["in_fields"]       = template.paramlist;
    #stage["out_fields"]      = (0...template.target.keys.length).to_a.collect { |i| "kdim#{i.to_s}" }.concat(["value"]);
    stage["out_fields"]      = ["key", "value"];
    
    stage["reduce_in_fields"]       = ["key", "value"];
    stage["reduce_out_fields"]      = ["key", "value"];

    stage["input_maps"]      = template.entries.collect { |e| e.source };

    stage["builtin_map"]     = false;
    stage["builtin_reduce"]  = false;

    stage["extra_files"] = [
      "\"bin/bootstrap.jar\"",
      "\"lib/commons-logging-1.0.4.jar\"",
      "\"lib/hadoop-0.20.1-core.jar\"",
      "\"lib/hbase-0.20.2.jar\"",
      "\"lib/zookeeper-3.2.1.jar\"",
      "\"lib/log4j-1.2.15.jar\"" ]

    input_theta = Hash.new
    template.paramlist.each { |f| input_theta[f] = "input.#{f}.to_f" }
      
    debug { "Input theta: #{input_theta.to_s}" }

    if template.entries.nil? || template.entries.size == 0 then
      debug { "Target: #{template.target.to_s}" }
      output_keys_expr = normalize_keys(template.target.keys).collect { |k| input_theta[k] }.join(",");
      arith_expr = input_theta.inject(template.expression.to_s) do |expr, pr|
        e_s, v_s = pr; expr.gsub(e_s, v_s)
      end

      stage["map_in_separator"]    = "|";

      stage["map_code"] = [
        "rv = new_output;",
        #"rv[0...#{template.target.keys.length}] = [#{output_keys_expr}];",
        "rv.key = [#{output_keys_expr}].join(\",\");",
        "rv.value = #{arith_expr};",
        "rv"]

      target_map_patterns = patterns[template.target.source].collect { |ap| "["+ap.join(",")+"]" }
      stage["reduce_init_code"] = [
        "map#{template.target.source}_nk = #{template.target.keys.size}",
        "map#{template.target.source}_patterns = [#{target_map_patterns.join(",")}]",
        "@map#{template.target.source} = "+
          "MultiKeyMap.new("+
            "map#{template.target.source}_nk,"+
            "map#{template.target.source}_patterns,"+
            "\"Map#{template.target.source}\")",
        "nil" ]
      stage["reduce_group_init_code"] = [ "@acc = 0", "nil"]
      stage["reduce_code"] = ["@acc = @acc + input.value.to_f", "nil"]
      stage["reduce_group_final_code"] =
        ["output.key = @last",
          "output.value = @acc",
          "@map#{template.target.source}[output.key.split(\",\").collect { |k| k.to_i }] = output.value.to_f",
          "output" ]
      stage["reduce_final_code"] = [ "@map#{template.target.source}.close", "nil" ]
    
      #stage["reduce_code"] = ["UniqueSumReduce"];
      #stage["builtin_reduce"] = true;

    else

    #  TODO: clean up cursors
    #  TODO: design map naming and access, e.g. pass an input map config file as
    #  an extra file, which specifies whether we're accessing HBase or BDB files.
    #  Read this config file at each mapper as part of process_begin, and create
    #  handles to input maps.
    #    stage[extra_files] = job name / input map names
    #
    #    Substitutions:
    #    1. output keys: loop vars => any map key vars, and bound vars => input vars 
    #    2. arith expr: lookup key bound vars => input vars,
    #                   input map lookups => lookup vals,
    #                   map slices => map vals
    #    3. constraint: map key loop vars => map key vars

      loopvars = template.loopvarlist.collect do |e_ivl|
          lv_entry = template.entries[e_ivl[0]]
          normalize_keys(lv_entry.keys).select { |k| not(input_theta.key?(k)) }
        end.flatten.uniq

      debug { "Loopvars: #{loopvars.join(",")}" }

      loopvar_uses = Hash.new
      loopvars.each do |v|
          r = template.entries.collect do |e|
              e_keys = normalize_keys(e.keys) 
              (e_keys.include?(v) ? [e, e_keys.index(v)] : nil)
            end.compact;
          puts "LV #{v} uses: #{r.size}";
          loopvar_uses[v] = r;
        end
    
      slices, lookups = template.entries.partition do |e|
        normalize_keys(e.keys).any? { |k| loopvars.include?(k) }
      end

      lookup_theta = Hash.new
      lookup_idx = 0;
      lookup_bind_exprs = lookups.collect do |e|
          lookup_theta[e.to_s] = "vals[#{lookup_idx}]";
          params = normalize_keys(e.keys).collect{ |k| input_theta[k] }.join(",");
          emapname = "@map#{e.source.to_s}"
          map_lookup = "vals[#{lookup_idx}] = #{emapname}[#{params}]";
          lookup_idx += 1;
          map_lookup
        end;
    
    #    Cursors need:
    #    1. initialization
    #    2. key bindings, value bindings per tuple. The bindings are created by
    #       the join algorithm, but we must know how to use them here.
    #       Both are positional, i.e. correspond to cursor index of the slice.

      slice_theta = Hash.new
      slice_val_theta = Hash.new
      slice_idx = 0;  
      cursor_inits = slices.collect do |e|
          if not(slice_theta.key?(e.source)) then
            slice_val_theta[e.to_s] = "mapvals[#{slice_idx}]";
            slice_theta[e.source] = slice_idx;
            slice_access_pattern = normalize_keys(e.keys).collect do |k|
              loopvars.include?(k) ? "@wildcard" : input_theta[k]
            end.join(",");
            emapname = "@map#{e.source.to_s}"
            slice_init = "cursors << #{emapname}.scan([#{slice_access_pattern}])"
            slice_idx += 1;
            slice_init
          else nil end
        end.compact;

      loopvar_theta = Hash.new
      loopvar_uses.each_pair do |v,el|
          puts "El: #{el.to_s}"
          loopvar_entry_idx = slice_theta[el[0][0].source]
          loopvar_theta[v] = "mapkeys[#{loopvar_entry_idx}][#{el[0][1]}]"
        end;

      output_keys_expr = normalize_keys(template.target.keys).collect do |k|
          loopvars.include?(k) ? loopvar_theta[k] : input_theta[k]
        end.join(",");
    
      arith_expr = [slice_val_theta, lookup_theta, input_theta].inject(template.expression.to_s) do |expr, theta|
          theta.inject(expr) { |texpr,pr| e_s, v_s = pr; texpr.gsub(e_s,v_s) }
        end;
    
      equiv_classes = loopvar_uses.select { |v,el| el.size > 1 }
    
      constraints = equiv_classes.collect do |v,el|
        c, cidx = el.shift
        ckeys = "mapkeys[#{slice_theta[c.source]}]";
        el.select { |e, eidx| not(c.source == e.source) }.collect do |e, eidx|
          ekeys = "mapkeys[#{slice_theta[e.source]}]";
          "#{ckeys}[#{cidx}] == #{ekeys}[#{eidx}]"
        end
      end.flatten.join(" and ");
  
      predicate_and_output_block = [
        "do |mapkeys, mapvals| ",
        (constraints.nil? || constraints.empty?) ? nil : "  if #{constraints} then ",
        "    rv = new_output;",
        #"    rv[0...#{template.target.keys.length}] = [#{output_keys_expr}];",
        "    rv.key = [#{output_keys_expr}].join(\",\");",
        "    rv.value = #{arith_expr};",
        "    outputs << rv;",
        (constraints.nil? || constraints.empty?) ? nil : "  end",
        "end"].compact;
  
      stage["map_init_code"] = ["@wildcard = -1"].concat(
        template.entries.collect do |e|
          map_patterns = patterns[e.source].collect { |ap| "["+ap.join(",")+"]" }
          ["map#{e.source.to_s}_nk = #{e.keys.size}",
            "map#{e.source.to_s}_patterns = [#{map_patterns.join(",")}]",
            "@map#{e.source.to_s} = MultiKeyMap.new(map#{e.source.to_s}_nk, map#{e.source.to_s}_patterns, \"Map#{e.source.to_s}\")"]
        end.flatten.uniq)

      stage["map_code"] = [
          "outputs = []; cursors = [];",
          lookup_bind_exprs,
          cursor_inits,
          "nljoin(cursors) " + predicate_and_output_block[0],
          predicate_and_output_block[1..-1],
          "outputs"
        ].flatten;
    
      target_map_patterns = patterns[template.target.source].collect { |ap| "["+ap.join(",")+"]" }
      stage["reduce_init_code"] = [
        "map#{template.target.source}_nk = #{template.target.keys.size}",
        "map#{template.target.source}_patterns = [#{target_map_patterns.join(",")}]",
        "@map#{template.target.source} = "+
          "MultiKeyMap.new("+
            "map#{template.target.source}_nk,"+
            "map#{template.target.source}_patterns,"+
            "\"Map#{template.target.source}\")",
        "nil" ]
      stage["reduce_group_init_code"] = [ "@acc = 0", "nil"]
      stage["reduce_code"] = ["@acc = @acc + input.value.to_f", "nil"]
      stage["reduce_group_final_code"] =
        ["output.key = @last",
          "output.value = @acc",
          "@map#{template.target.source}[@last.split(\",\").collect { |k| k.to_i }] = output.value.to_f",
          "output" ]
      stage["reduce_final_code"] = [ "@map#{template.target.source}.close", "nil" ]

      #stage["reduce_code"] = ["UniqueSumReduce"];
      #stage["builtin_reduce"] = true;
    end
      
    return [template.target.source, stage]
  end

  def compile_program()
    # bottom-up compilation:
    # -- init: find maps with no dependent maps, find templates defining maps,
    #    push to pending queue
    # -- loop to empty queue, producing an MR stage described above. Track each
    #    map and set of MR stages that can generate it, along with score.
    #    Push unhandled parent maps/templates for each new map compiled.
    # -- traverse maps from result map based on score, tracking domains, outputting MR stages.
    
    # Map name => array of <stage,domains>
    map_stages = Hash.new
    map_deps = Hash.new
    $config.templates.each_value do |t|
      map_stages[t.target.source] = [];
      map_deps[t.target.source] = [];
    end

    compile_queue = $config.templates.values.select { |t| t.entries.nil? || t.entries.size == 0 }

    while not(compile_queue.empty?)
      template = compile_queue.shift;
      
      debug { "Compiling MR for: #{template.to_s}"; }

      patterns = Hash.new
      [template.target].concat(template.entries).each do |e|
        patterns[e.source] = [] unless patterns.key?(e.source)

        nps = []
        $config.templates.values.collect do |t|
            t.access_patterns(e.source)
        end.select { |p| p.size > 0 }.each { |aps| aps.each { |p| nps << p } }

        patterns[e.source] = ((patterns[e.source].concat(nps)).uniq)
      end

      mapid, mr_stage = compile_template(template, patterns);
      map_stages[mapid] = map_stages[mapid].push(mr_stage);

      # Track children to compute scores
      new_deps = template.entries.collect { |e| e.source } 
      map_deps[template.target.source] = map_deps[template.target.source].concat(new_deps).uniq;
      
      debug { "New deps: #{new_deps.join(",")}" }
      debug { "Deps: #{map_deps[template.target.source].join(",")}" }

      # Add new templates
      compile_queue = compile_queue.concat(
          $config.templates.values.select do |t|
            t.entries.any? {|e| e.source == template.target.source}
        end).uniq;
    end

    debug { "Map deps: "; }
    map_deps.each_pair { |k,v| debug { ("#{k}=>"+v.join(",")) } }

    # Compute scores
    stage_scores = Hash.new;
    map_stages.each_value { |stages| stages.each { |s| stage_scores[s] = -1; } }

    score_queue = [];
    map_deps.each_pair { |n,ch| score_queue.push(n) if ch.size == 0 }

    while not(score_queue.empty?)
      mapid = score_queue.shift;
      if map_deps[mapid].size == 0 then
        map_stages[mapid].each { |s| stage_scores[s] = 1 }
      else
        map_stages[mapid].each do |s|
          stage_scores[s] = map_deps[mapid].collect do |cid|
            (map_stages[cid].collect { |cs| stage_scores[cs] }.min)+1
          end.min
        end
      end
      parents = map_deps.select { |k,v| v.include?(mapid) }.collect { |k,v| k }.uniq
      score_queue = score_queue.concat(parents).uniq
    end
    
#    # Pick stages with best scores
#    map_mr_stages = []
#    rootid = map_deps.select { |k,v| map_deps.all? { |k2,v2| not(v2.include?(k)) } }[0][0]
#
#    traversal = [rootid]
#    while not(traversal.empty?)
#      node = traversal.shift
#      puts "Traversing #{node}"
#      new_stage = map_stages[node].min { |a,b| stage_scores[a] <=> stage_scores[b] }
#      map_mr_stages = [new_stage].concat(map_mr_stages)
#      traversal = traversal.concat(map_mr_stages[0]["input_maps"]).uniq;
#    end
      
    map_mr_stages = []
    rootid = map_deps.select { |k,v| map_deps.all? { |k2,v2| not(v2.include?(k)) } }[0][0]
      
    traversal = [rootid]
    done = []
    while not(traversal.empty?)
      node = traversal.shift
      done << node;
      debug { "Traversing #{node}" }
      new_stage = map_stages[node].min { |a,b| stage_scores[a] <=> stage_scores[b] }
      map_mr_stages = [new_stage].concat(map_mr_stages);
      next_nodes = map_stages[node].collect { |s| s["input_maps"] }.flatten.select { |n| not(done.include?(n)) }
      traversal = traversal.concat(next_nodes).uniq;
    end

    @stages = map_mr_stages

    # TODO: interleave stages for index construction for map lookup
    # -- need to decide indexing solution, e.g. BDB, HBase, file-based
    # -- first will test with MR jobs on base relations

    @stages.collect { |s| [s["jobname"], gc_mr_toolkit(s)] }

  end

  def gc_mrt_class(classname, superclass, methods)
    puts "Method size: #{methods.size}"
    r = ["class #{classname}" +
          ((superclass.nil? || superclass.size == 0) ? "" : " < #{superclass}"),
          methods.collect do |m|
            body = m[1..-1].flatten.map { |s| @tab+s }
            ["def #{m[0]}", body, "end"].flatten.map { |s| @tab+s }.join("\n")
          end,
        "end"].flatten.join("\n");
    return [classname, r]
  end

  def gc_mrt_mapper_class(jobname, methods)
    gc_mrt_class("#{jobname}Mapper", "MapBase", methods)
  end

  def gc_mrt_reducer_class(jobname, methods)
    gc_mrt_class("#{jobname}Reducer", "ReduceBase", methods)
  end
  
  def gc_mrt_job_class(jobname, methods)
    gc_mrt_class("#{jobname}Job", "JobBase", methods)
  end

  def gc_mr_toolkit(stage)
    require_str = [
      "require 'mrtoolkit';",
      "require 'join_utils';",
      "require 'multikeymap_HBase_JRB';"
      ].join("\n")

    mapper_methods = [
      ["declare", (stage.key?("in_fields") ?
          stage["in_fields"].collect {|f| "field :#{f}"} : []).concat(
        stage.key?("out_fields") ?
          stage["out_fields"].collect {|f| "emit :#{f}"} : []).concat(
        stage.key?("map_in_separator") ?
          ["field_separator \"#{stage["map_in_separator"]}\""] : []).concat(
        stage.key?("map_out_separator") ?
          ["emit_separator \"#{stage["map_in_separator"]}\""] : []) ],
      ["process_begin(dummy,output)", stage["map_init_code"]],
      ["process(input,output)", stage["map_code"]],
      ["process_end(dummy,output)", stage["map_final_code"]]].collect do |name, body|
        if body.nil? || body.size == 0 then nil else [name, body] end
      end.compact

    puts "Mapper methods #{mapper_methods.size}"

    mapper_class, mapper_str =
      if stage["builtin_map"] then [stage["map_code"], nil]
      else gc_mrt_mapper_class(stage["jobname"], mapper_methods)
      end

    reducer_methods = [
      ["declare", (stage.key?("reduce_in_fields") ?
          stage["reduce_in_fields"].collect {|f| "field :#{f}"} : []).concat(
        stage.key?("reduce_out_fields") ?
          stage["reduce_out_fields"].collect {|f| "emit :#{f}"} : []).concat(
        stage.key?("reduce_in_separator") ?
          ["field_separator \"#{stage["reduce_in_separator"]}\""] : []).concat(
        stage.key?("reduce_out_separator") ?
          ["emit_separator \"#{stage["reduce_in_separator"]}\""] : []) ],
      ["process_begin(dummy,output)", stage["reduce_init_code"]],
      ["process_init(input,output)", stage["reduce_group_init_code"]],
      ["process_each(input,output)", stage["reduce_code"]],
      ["process_term(input,output)", stage["reduce_group_final_code"]],
      ["process_end(dummy,output)", stage["reduce_final_code"]]].collect do |name, body|
        if body.nil? || body.size == 0 then nil else [name,body] end
      end.compact

    reducer_class, reducer_str =
      if stage["builtin_reduce"] then [stage["reduce_code"], nil]
      else gc_mrt_reducer_class(stage["jobname"], reducer_methods)
      end

    # TODO: partitioner and outputformat for HFileOutputFormat
    job_lines = [ "job",
      [ "mapper #{mapper_class}",
        "reducer #{reducer_class}",
        "indir \"#{stage["in_path"]}\"",
        "outdir \"#{stage["out_path"]}\""].concat(
      stage.key?("extra_files") ? stage["extra_files"].collect {|f| "extra #{f}"} : []) ]
    
    job_class, job_str = gc_mrt_job_class(stage["jobname"], [job_lines]);

    code = [require_str, "", mapper_str, "", reducer_str, "", job_str].compact.join("\n")
    code
  end
end

#######################
# 
# Compiler execution

$config_file = ""
$mr_job_path = "."
$source_path = nil
$output_path = "maps"

opts = GetoptLong.new(
  [ "-c", "--config-file",       GetoptLong::REQUIRED_ARGUMENT ],
  [ "-l", "--localprops",        GetoptLong::REQUIRED_ARGUMENT ],
  [ "-j", "--hadoop-job-path",   GetoptLong::REQUIRED_ARGUMENT ],
  [ "-i", "--source-path",       GetoptLong::REQUIRED_ARGUMENT ],
  [ "-o", "--output-path",       GetoptLong::REQUIRED_ARGUMENT ]
).each do |opt, arg| 
  case opt
    when "-c", "--config-file"     then $config_file = arg; 
    when "-l", "--localprops"      then $config.load_local_properties(arg)
    when "-j", "--hadoop-job-path" then $mr_job_path = arg;
    when "-i", "--source-path"     then $source_path = arg;
    when "-o", "--output-path"     then $output_path = arg;  
  end
end

$config.load($config_file)
$config.client_debug["sourcedir"] = $source_path unless $source_path.nil?
$config.client_debug["targetdir"] = $output_path

#########################
#
# Mapreduce job generation

$mrcompiler = HadoopCompiler.new();
hadoop_jobs = $mrcompiler.compile_program()

jobfilepath = $mr_job_path[0] == "/"[0] ?
  $mr_job_path : File.join($config.hadoop_job_path, $mr_job_path);
stagefile = File.open(File.join(jobfilepath, "stages"), "w+")
hadoop_jobs.each do |name,job|
  FileUtils.mkdir_p(jobfilepath) unless File.directory?(jobfilepath);
  jobfile = File.open(File.join(jobfilepath, name), "w+");
  jobfile.write(job);
  jobfile.close;
  
  stagefile.write(name+"\n")
end

stagefile.close;
