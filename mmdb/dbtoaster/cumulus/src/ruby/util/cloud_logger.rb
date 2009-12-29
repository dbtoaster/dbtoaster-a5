#Java::org::slf4j::Logger;
#Java::org::slf4j::LoggerFactory;

$logProjectName = "dbtoaster.cumulus";
$logProcessName = File.basename($0, ".sh").to_lower;

class CLog
  def CLog.get(name = nil)
    Java::org::apache::log4j::Logger.getLogger("#{@@prefix}#{name ? "." : ""}#{name}")
  end
  
  @@loggerEnabled = false;
  @@prefix = "#{$logProjectName}.#{$logProcessName}";
  @@defaultLogger = CLog.get();
  
  def CLog.enabled?
    @@loggerEnabled;
  end
  
  def CLog.enable
    @@loggerEnabled = true;
  end
  
  def CLog.disable
    @@loggerEnabled = false;
  end
  
  def CLog.defaultLogger
    return @@defaultLogger.name;
  end
  
  def CLog.defaultLogger=(name)
    @@defaultLogger = get(name);
  end
  
  def CLog.trace(target = @@defaultLogger, throwable = nil)
    return unless @@loggerEnabled;
    if throwable then
      target.trace(yield, throwable);
    else
      target.trace(yield);
    end
  end
  
  def CLog.debug(target = @@defaultLogger, throwable = nil)
    return unless @@loggerEnabled;
    if throwable then
      target.debug(yield, throwable);
    else
      target.debug(yield);
    end
  end
  
  def CLog.info(target = @@defaultLogger, throwable = nil)
    return unless @@loggerEnabled;
    if throwable then
      target.info(yield, throwable);
    else
      target.info(yield);
    end
  end
  
  def CLog.warn(target = @@defaultLogger, throwable = nil)
    return unless @@loggerEnabled;
    if throwable then
      target.warn(yield, throwable);
    else
      target.warn(yield);
    end
  end

  def CLog.error(target = @@defaultLogger, throwable = nil)
    return unless @@loggerEnabled;
    if throwable then
      target.error(yield, throwable);
    else
      target.error(yield);
    end
  end
  
  def CLog.fatal(target = @@defaultLogger, throwable = nil)
    error(target, throwable);
    exit(-1)
  end
  
end

module CLogMixin
  @@classLogger = nil;
  
  def setup_logger(segment = nil)
    @@classLogger = CLog.get("#{segment}#{segment ? "." : ""}#{super.class.name}");
  end
  
  def trace(throwable = nil)
    return unless CLog.enabled?
    setup_logger unless @@classLogger;
    if throwable then
      @@classLogger.trace(yield, throwable)
    else
      @@classLogger.trace(yield)
    end
  end
  
  def debug(throwable = nil)
    return unless CLog.enabled?
    setup_logger unless @@classLogger;
    if throwable then
      @@classLogger.debug(yield, throwable)
    else
      @@classLogger.debug(yield)
    end
  end
  
  def info(throwable = nil)
    return unless CLog.enabled?
    setup_logger unless @@classLogger;
    if throwable then
      @@classLogger.info(yield, throwable)
    else
      @@classLogger.info(yield)
    end
  end
  
  def warn(throwable = nil)
    return unless CLog.enabled?
    setup_logger unless @@classLogger;
    if throwable then
      @@classLogger.warn(yield, throwable)
    else
      @@classLogger.warn(yield)
    end
  end

  def error(throwable = nil)
    return unless CLog.enabled?
    setup_logger unless @@classLogger;
    if throwable then
      @@classLogger.error(yield, throwable)
    else
      @@classLogger.error(yield)
    end
  end
end

CLog.disable if $config["logger.bypass"] == "enabled";

