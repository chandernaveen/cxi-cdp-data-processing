#!/bin/bash
# https://kb.databricks.com/clusters/overwrite-log4j-logs.html
# https://forums.databricks.com/questions/17625/how-can-i-customize-log4j.html
#  TODO: what about customizing logging on executors as well ?

echo "" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "#RefinedFile" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.logger.RefinedLogger=INFO, RefinedFile" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.additivity.RefinedLogger=false" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile=com.databricks.logging.RedactionRollingFileAppender" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.layout=org.apache.log4j.PatternLayout" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.rollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.rollingPolicy.FileNamePattern=logs/log4j-refined-%d{yyyy-MM-dd-HH}.log.gz" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.rollingPolicy.ActiveFileName=logs/stdout.log4j-refined-active.log" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
