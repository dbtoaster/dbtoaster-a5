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
def build_php(script, target_dir)
  
  old_dir    = Dir.getwd;
  script_dir = File.dirname script;
  
  Dir.chdir script_dir;
  
  sources = Dir.glob("pages/*.php").map do |source|
      source = File.basename(source, ".php");
      parts = source.split(/_/);
      page = parts.shift;
      subpage = if parts.length > 0 then parts.join("_") else "index" end;
      [ page, subpage, 
        if source == "home" then "index.html"
                            else "#{source}.html" end
      ];
    end
    
  page_data = 
    sources.map do |page, subpage, dest_file|
      IO.popen("php", "w+") do |php|
        php.puts "<?php ";
        php.puts "$_GET['page'] = '#{page}';";
        php.puts "$_GET['subpage'] = '#{subpage}';";
        php.puts "$now_building_distro = true";
        php.puts "?>";
        php.puts(File.open(File.basename script) { |f| f.readlines.join(""); });
        php.close_write;
        [ dest_file, php.readlines.join("") ];
      end
    end
  
  Dir.chdir old_dir;
  
  page_data.each do |dest_file, script_data|
    File.open("#{target_dir}/#{dest_file}", "w") { |f| f.puts script_data }
  end
end

copy_files(["site/9.jpg",
            "site/style.css",
            "site/bakeoff.png",
            "site/bluetab.gif",
            "site/bluetabactive.gif",
            "site/dbtoaster-logo.gif",
            "site/schematic.png"], "site_html");
copy_files(["site/css/bootstrap-theme.min.css",
            "site/css/bootstrap.min.css",
            "site/css/style.css"], "site_html/css");
copy_files(["site/js/bootstrap.min.js",
            "site/js/jquery-2.0.3.min.js"], "site_html/js");
copy_files(["site/data/bakeoff.csv"], "site_html/data");
            
build_php("site/index.php", "site_html");

