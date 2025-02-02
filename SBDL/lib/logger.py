class Log4j(object):
'''
* spark._jvm gives access to the underlying JVM from PySpark.
* org.apache.log4j is the Log4j logging library in Java.
* log4j now holds a reference to this Java logging library.

* log4j.LogManager.getLogger("sbdl") creates a logger instance named "sbdl".
* This logger can now be used to log messages at different levels (warn, info, error, debug).
* self.logger stores this logger instance for use in the other methods of the class.
'''
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("sbdl")

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)