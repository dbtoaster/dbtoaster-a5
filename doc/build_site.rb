#!/usr/bin/env ruby

def copy_files(files,  tgt)
  Dir.mkdir(tgt) unless File.exists? tgt;
  raise "Invalid target directory: #{tgt}" unless File.directory? tgt
  files.each do |f|
    if File.directory? f 
      then copy_dir f,  tgt
      else copy_file f,  tgt
    end
  end
end

def copy_file(f,  tgt)
  system("cp #{f} #{tgt}/") or exit(-1);
end

def copy_dir(d,  tgt)
  entries =
    Dir.entries(d).
      delete_if { |e| e[0] == "."[0] }.
      map { |e| "#{d}/#{e}" }
  copy_files(entries,  "#{tgt}/#{File.basename d}")
end

def build_php(script,  target_dir) 
  old_dir    = Dir.getwd;
  script_dir = File.dirname script;
  
  Dir.chdir script_dir;
  
  # sources = Dir.glob("pages/*.php").select { |source| !source.include? "samples" }.map do |source|
  sources = Dir.glob("pages/*.php").map do |source|
      source = File.basename(source,  ".php");
      parts = source.split(/_/);
      page = parts.shift;
      subpage = if parts.length > 0 then parts.join("_") else "index" end;
      [ page,  subpage,  
        if source == "home" then "index.html"
                            else "#{source}.html" end
      ];
    end
    
  page_data = 
    sources.map do |page,  subpage,  dest_file|
      IO.popen("php",  "w+") do |php|
        php.puts "<?php ";
        php.puts "$_GET['page'] = '#{page}';";
        php.puts "$_GET['subpage'] = '#{subpage}';";
        php.puts "$now_building_distro = true";
        php.puts "?>";
        php.puts(File.open(File.basename script) { |f| f.readlines.join(""); });
        php.close_write;
        [ dest_file,  php.readlines.join("") ];
      end
    end

  sample_queries = { 
    "tpch" => [ 
        "tpch1",  "tpch2",  "tpch3",  "tpch4",  "tpch5",  "tpch6",  "tpch7",  "tpch8", "tpch9",  "tpch10",  "tpch11",  "tpch11a",  "tpch11c",  "tpch12",  "tpch13",  "tpch14",  "tpch15", "tpch16",  "tpch17",  "tpch17a",  "tpch18",  "tpch18a",  "tpch19",  "tpch20",  "tpch21",  "tpch22",  "tpch22a"#,  "ssb4"
      ], 
    "finance" => [ 
      "axfinder",  "brokerspread",  "brokervariance",  "pricespread",  "vwap"
    ], 
    "simple" => [ 
      "inequality_selfjoin", "invalid_schema_fn", "m3k3unable2escalate", "miraculous_minus", "miraculous_minus2", "r_aggcomparison", "r_aggofnested", "r_aggofnestedagg", "r_agtb", "r_agtbexists", "r_avg", "r_bigsumstar", "r_btimesa", "r_btimesacorrelated", "r_count", "r_count_of_one", "r_count_of_one_prime", "r_deepscoping", "r_divb", "r_existsnestedagg", "r_gbasumb", "r_gtealldynamic", "r_gtesomedynamic", "r_gtsomedynamic", "r_impossibleineq", "r_indynamic", "r_ineqandeq", "r_instatic", "r_lift_of_count", "r_ltallagg", "r_ltallavg", "r_ltallcorravg", "r_ltalldynamic", "r_multinest", "r_multiors", "r_natselfjoin", "r_nogroupby", "r_nonjoineq", "r_possibleineq", "r_possibleineqwitheq", "r_selectstar", "r_simplenest", "r_smallstar", "r_starofnested", "r_starofnestedagg", "r_sum_gb_all_out_of_aggregate", "r_sum_gb_out_of_aggregate", "r_sum_out_of_aggregate", "r_sumadivsumb", "r_sumdivgrp", "r_sumnestedintarget", "r_sumnestedintargetwitheq", "r_sumoutsideofagg", "r_sumstar", "r_union", "r_unique_counts_by_a", "rr_ormyself", "rs", "rs_cmpnest", "rs_column_mapping_1", "rs_column_mapping_2", "rs_column_mapping_3", "rs_eqineq", "rs_ineqonnestedagg", "rs_inequality", "rs_ineqwithnestedagg", "rs_joinon", "rs_joinwithnestedagg", "rs_natjoin", "rs_natjoinineq", "rs_natjoinpartstar", "rs_selectconstcmp", "rs_selectpartstar", "rs_selectstar", "rs_simple", "rs_stringjoin", "rst", "rstar", "rtt_or_with_stars", "singleton_renaming_conflict", "ss_math", "t_lifttype"
    ], 
    "employee" => [ 
      "query01", "query01a", "query02", "query02a", "query03", "query03a", "query04", "query04a", "query05", "query06", "query07", "query08", "query08a", "query09", "query09a", "query10", "query10a", 
      "query11b", #"query11", "query11a", "query12", "query15", "query35b", "query36b", 
      "query12a", "query13", "query14", "query16", "query16a", "query17a", "query22", "query23a", "query24a", "query35c", "query36c", "query37", "query38a", "query39", "query40", "query45", "query46", "query47", "query47a", "query48", "query49", "query50", "query51", 
      #"query52", "query53", "query56", "query57", "query58", "query62", "query63", "query64", "query65", "query66", "query66a", 
      "query52a", "query53a", "query54", "query55", "query56a", "query57a", "query58a", "query59", "query60", "query61", "query62a", "query63a", "query64a", "query65a"
    ], 
    "mddb" => [
      "mddb1", "mddb2"
    ],
    "zeus" => [ 
      "11564068", "12811747", "37494577", "39765730", "48183500", "52548748", "59977251", "75453299", "94384934", "95497049", "96434723"
    ]
  }

  sample_pages_data = 
    sample_queries.map do | query_group,  queries |
      queries.map do |q|
        dest_file = "samples/#{query_group}/#{q}.html"
        dest_file = "samples_#{query_group}_#{q}.html"
        IO.popen("php",  "w+") do |php|
          php.puts "<?php ";
          php.puts "$_GET['page'] = 'samples';";
          php.puts "$_GET['q'] = '#{q}';";
          php.puts "$now_building_distro = true";
          php.puts "?>";
          php.puts(File.open(File.basename script) { |f| f.readlines.join(""); });
          php.close_write;
          [ dest_file,  php.readlines.join("") ];
        end
      end
    end.flatten(1)

  Dir.chdir old_dir;

  page_data.each do | dest_file,  script_data |
    File.open("#{target_dir}/#{dest_file}",  "w") { |f| f.puts script_data }
  end

  # Dir.mkdir("#{target_dir}/samples") unless File.exists? "#{target_dir}/samples";
  # sample_queries.each do | query_group,  queries |
  #   Dir.mkdir("#{target_dir}/samples/#{query_group}") unless File.exists? "#{target_dir}/samples/#{query_group}";
  # end

  sample_pages_data.each do | dest_file,  script_data |
    File.open("#{target_dir}/#{dest_file}",  "w") { |f| f.puts script_data }
  end
end

copy_files(["site/favicon.ico", 
            "site/9.jpg", 
            "site/bakeoff.png", 
            "site/bluetab.gif", 
            "site/bluetabactive.gif", 
            "site/dbtoaster-logo.gif", 
            "site/schematic.png", 
            "site/internal_arch.png", 
            "site/perf.png", 
            "site/htmlcode", 
            "site/htmlsql"],  "site_html");
copy_files(["site/css/bootstrap-theme.min.css", 
            "site/css/bootstrap.min.css", 
            "site/css/style.css"],  "site_html/css");
copy_files(["site/js/bootstrap.min.js", 
            "site/js/jquery-2.0.3.min.js"],  "site_html/js");
copy_files(["site/data/bakeoff.csv"],  "site_html/data");
            
build_php("site/index.php",  "site_html");

