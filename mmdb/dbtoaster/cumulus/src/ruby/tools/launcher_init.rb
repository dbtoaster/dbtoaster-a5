require 'config/config';
conf_file = ARGV.shift;
hostname = `hostname`.chomp.sub(/^([^.]*)\.?.*/, "\\1");
site_conf_file = conf_file.sub(/local.properties/, "#{hostname}.properties");
$config.load_local_properties(conf_file);
$config.load_local_properties(site_conf_file) if File.exists? site_conf_file;
$config.parse_opt("object_file", $launcherClass);
Java::org::apache.log4j.MDC.put("hostname", hostname);
require 'util/cloud_logger';
