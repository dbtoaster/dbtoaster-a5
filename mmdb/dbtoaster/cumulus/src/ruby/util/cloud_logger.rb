#Java::org::slf4j::Logger;
#Java::org::slf4j::LoggerFactory;

$logProjectName = "dbtoaster";
if $config["object_file"] then
  $logProcessName = File.basename($config["object_file"], ".rb").downcase
else
  $logProcessName = "";
end

class CLog
  def CLog.get(name = nil)
    name = "#{@@prefix}#{name ? ".#{name}" : ""}#{$logProcessName ? ".#{$logProcessName}" : ""}"
    Java::org::apache::log4j::Logger.getLogger(name)
  end
  
  @@logger_enabled = true;
  @@prefix = "#{$logProjectName}";
  @@default_logger = CLog.get();
  
  def CLog.enabled?
    @@logger_enabled;
  end
  
  def CLog.enable
    @@logger_enabled = true;
  end
  
  def CLog.disable
    @@logger_enabled = false;
  end
  
  def CLog.logger_target
    return @@default_logger.name;
  end
  
  def CLog.logger_target=(name)
    @@default_logger = get(name);
  end
  
  def CLog.default_logger
    @@default_logger;
  end
  
  def CLog.trace(target = @@default_logger, throwable = nil)
    return unless @@logger_enabled;
    if throwable then
      target.trace(yield, throwable);
    else
      target.trace(yield);
    end
  end
  
  def CLog.debug(target = @@default_logger, throwable = nil)
    return unless @@logger_enabled;
    if throwable then
      target.debug(yield, throwable);
    else
      target.debug(yield);
    end
  end
  
  def CLog.info(target = @@default_logger, throwable = nil)
    return unless @@logger_enabled;
    if throwable then
      target.info(yield, throwable);
    else
      target.info(yield);
    end
  end
  
  def CLog.warn(target = @@default_logger, throwable = nil)
    return unless @@logger_enabled;
    if throwable then
      target.warn(yield, throwable);
    else
      target.warn(yield);
    end
  end

  def CLog.error(target = @@default_logger, throwable = nil)
    return unless @@logger_enabled;
    if throwable then
      target.error(yield, throwable);
    else
      target.error(yield);
    end
  end
  
  def CLog.fatal(target = @@default_logger, throwable = nil)
    error(target, throwable);
    exit(-1)
  end
    
end

module CLogMixins
  def trace(throwable = nil)
    return unless CLog.enabled?
    if throwable then
      self.class.local_logger.trace(yield, throwable)
    else
      self.class.local_logger.trace(yield)
    end
  end
  
  def debug(throwable = nil)
    return unless CLog.enabled?
    if throwable then
      self.class.local_logger.debug(yield, throwable)
    else
      self.class.local_logger.debug(yield)
    end
  end
  
  def info(throwable = nil)
    return unless CLog.enabled?
    if throwable then
      self.class.local_logger.info(yield, throwable)
    else
      self.class.local_logger.info(yield)
    end
  end
  
  def warn(throwable = nil)
    return unless CLog.enabled?
    if throwable then
      self.class.local_logger.warn(yield, throwable)
    else
      self.class.local_logger.warn(yield)
    end
  end

  def error(throwable = nil)
    return unless CLog.enabled?
    if throwable then
      self.class.local_logger.error(yield, throwable)
    else
      self.class.local_logger.error(yield)
    end
  end
  
  module ClassMethods
    @@classLogger = nil;
  
    def logger_segment=(segment)
      @@classLogger = CLog.get("#{segment}#{segment ? "." : ""}#{self.name}");
    end
    
    def local_logger
      self.logger_segment=nil unless @@classLogger;
      @@classLogger;
    end
  end
  
  def self.included(klass)
    klass.extend(ClassMethods);
  end
end


CLog.disable if $config["logger.bypass"] == "enabled";
CLog.default_logger;
