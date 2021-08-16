// https://kb.databricks.com/clusters/overwrite-log4j-logs.html
// https://forums.databricks.com/questions/17625/how-can-i-customize-log4j.html
// Databricks notebook source
dbutils.fs.put("dbfs:/databricks/config/log4j-logging-config/log4j-refined-config.sh", """

#!/bin/bash
echo "" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "#RefinedFile" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.logger.RefinedLogger=INFO, RefinedFile" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.additivity.RefinedLogger=false" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile=com.databricks.logging.RedactionRollingFileAppender" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.layout=org.apache.log4j.PatternLayout" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.rollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.rollingPolicy.FileNamePattern=logs/log4j-refined-%d{yyyy-MM-dd-HH}.log.gz" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.RefinedFile.rollingPolicy.ActiveFileName=logs/stdout.log4j-refined-active.log" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties""",true)

dbutils.fs.put("dbfs:/databricks/config/log4j-logging-config/log4j-raw-config.sh", """

#!/bin/bash
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
""",true)

dbutils.fs.put("dbfs:/databricks/config/log4j-logging-config/log4j-curated-config.sh", """
#!/bin/bash
echo "" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "#CuratedFile" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.logger.CuratedLogger=INFO, CuratedFile" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.additivity.CuratedLogger=false" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.CuratedFile=com.databricks.logging.RedactionRollingFileAppender" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.CuratedFile.layout=org.apache.log4j.PatternLayout" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.CuratedFile.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.CuratedFile.rollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.CuratedFile.rollingPolicy.FileNamePattern=logs/log4j-curated-%d{yyyy-MM-dd-HH}.log.gz" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
echo "log4j.appender.CuratedFile.rollingPolicy.ActiveFileName=logs/stdout.log4j-curated-active.log" >> /home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties
""",true)

