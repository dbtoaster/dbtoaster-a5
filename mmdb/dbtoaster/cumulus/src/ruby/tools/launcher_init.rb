require 'config/config';
$config.load_local_properties(ARGV.shift);
$config.parse_opt("object_file", $launcherClass);
require 'util/cloud_logger';
