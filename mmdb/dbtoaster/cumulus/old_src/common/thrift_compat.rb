
spread_compat_mode  = :JRuby;
#spread_compat_mode = :VanillaRuby

case spread_compat_mode
  when :JRuby
    require 'spread_java_compat';
  
  when :VanillaRuby
    require 'thrift';
    require 'spread_types';
    require 'map_node';
    require 'switch_node';
    require 'slicer_node';
    require 'spread_types_mixins';
end