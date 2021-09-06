#!/bin/bash
# https://kb.databricks.com/clusters/overwrite-log4j-logs.html
# https://forums.databricks.com/questions/17625/how-can-i-customize-log4j.html
#  TODO: what about customizing logging on executors as well ?

echo "" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "#RawFile" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.logger.RawLogger=INFO, RawFile" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.additivity.RawLogger=false" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RawFile=com.databricks.logging.RedactionRollingFileAppender" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RawFile.layout=org.apache.log4j.PatternLayout" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RawFile.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RawFile.rollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RawFile.rollingPolicy.FileNamePattern=logs/log4j-raw-%d{yyyy-MM-dd-HH}.log.gz" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RawFile.rollingPolicy.ActiveFileName=logs/stdout.log4j-raw-active.log" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
