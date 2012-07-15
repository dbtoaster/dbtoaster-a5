#!/usr/bin/env ruby

def copy_files(files, tgt)
  Dir.mkdir(tgt) unless File.exists? tgt;
  raise "Invalid target directory: #{tgt}" unless File.directory? tgt
  files.each do |f|
    if File.directory? f 
      then copy_dir f, tgt
      else copy_file f, tgt
    end
  end
end

def copy_file(f, tgt)
  system("cp #{f} #{tgt}/") or exit(-1);
end

def copy_dir(d, tgt)
  entries =
    Dir.entries(d).
      delete_if { |e| e[0] == "."[0] }.
      map { |e| "#{d}/#{e}" }
  copy_files(entries, "#{tgt}/#{File.basename d}")
end

def fix_sql_file(sql_file, tgt_dir)
  File.open(sql_file) do |i|
    File.open("#{tgt_dir}/#{File.basename sql_file}", "w+") do |o|
      output_next_blank = false;
      o.puts(
        i.readlines.
          map { |l| l.gsub(/..\/..\/experiments/, "examples").
                      gsub(/test\/queries/, "examples/queries").
                      gsub(/standard\//, "").
                      gsub(/tiny\//, "").
                      gsub(/big\//, "").
                      gsub(/--.*/, "").
                      gsub(/data\/finance\/finance.csv/, 
                                      "data/finance.csv") }.
          delete_if { |l| 
            if l.chomp != "" then output_next_blank = true; false;
            elsif output_next_blank then output_next_blank = false; false;
            else true
            end
          }
      )
    end
  end
end

copy_files(["../doc/README", 
            "../doc/LICENSE"], "dbtoaster");

copy_files(["../bin/dbtoaster","../bin/dbtoaster_release"], "dbtoaster/bin");
copy_files([], "dbtoaster/lib");

copy_files(["../doc/site/9.jpg",
            "../doc/site/style.css",
            "../doc/site/bakeoff.png",
            "../doc/site/bluetab.gif",
            "../doc/site/bluetabactive.gif",
            "../doc/site/dropdowntabs.js",
            "../doc/site/dbtoaster-logo.gif"], "dbtoaster/doc");

copy_files(Dir.glob("../doc/site_html/*.html"), "dbtoaster/doc");

copy_files(Dir.glob("../lib/dbt_c++/*.a"), "dbtoaster/lib/dbt_c++");
copy_files(Dir.glob("../lib/dbt_c++/*.hpp"), "dbtoaster/lib/dbt_c++");
copy_files(Dir.glob("../lib/dbt_c++/*.cpp"), "dbtoaster/lib/dbt_c++");

copy_files(["../lib/dbt_scala/dbtlib.jar",
            "../lib/dbt_scala/makefile",
            "../lib/dbt_scala/src"], "dbtoaster/lib/dbt_scala");

copy_files([], "dbtoaster/examples");
copy_files(["../lib/dbt_c++/main.cpp",
            "../lib/dbt_scala/src/org/dbtoaster/RunQuery.scala"], 
                          "dbtoaster/examples/code");
copy_files([], "dbtoaster/examples/queries");
copy_files([], "dbtoaster/examples/data");
copy_files(Dir.glob("../../../experiments/data/simple/tiny/*.dat"), 
                                          "dbtoaster/examples/data/simple");
copy_files(Dir.glob("../../../experiments/data/tpch/tiny/*.csv"), 
                                          "dbtoaster/examples/data/tpch");
system("cp ../../../experiments/data/finance/tiny/finance.csv\
                                          dbtoaster/examples/data/finance.csv");

copy_files([], "dbtoaster/examples/queries/simple");
Dir.glob("../test/queries/simple/r*.sql").each { |f| 
      fix_sql_file(f, "dbtoaster/examples/queries/simple"); }

copy_files([], "dbtoaster/examples/queries/tpch");
Dir.glob("../test/queries/tpch/query*.sql").
  delete_if { |f| not /query[0-9]+.sql/ =~ f }.
  each { |f| 
      fix_sql_file(f, "dbtoaster/examples/queries/tpch"); }
fix_sql_file("../test/queries/tpch/schemas.sql", 
                          "dbtoaster/examples/queries/tpch");

copy_files([], "dbtoaster/examples/queries/finance");
Dir.glob("../test/queries/finance/*.sql").each { |f| 
      fix_sql_file(f, "dbtoaster/examples/queries/finance"); }

