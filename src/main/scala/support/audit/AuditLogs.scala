package com.cxi.cdp.data_processing
package support.audit

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{lit, to_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object AuditLogs {

    def write(logContext: LogContext)(implicit spark: SparkSession): Unit = {
        val auditRecordDf = spark
            .createDataFrame(
                spark.sparkContext.parallelize(Seq(Row(logContext.processName))),
                StructType(List(StructField("processName", StringType, true)))
            )
            .withColumn("entity", lit(logContext.entity))
            .withColumn("subEntity", lit(logContext.subEntity))
            .withColumn("runID", lit(logContext.runID))
            .withColumn("dpYear", lit(logContext.dpYear))
            .withColumn("dpMonth", lit(logContext.dpMonth))
            .withColumn("dpDay", lit(logContext.dpDay))
            .withColumn("dpHour", lit(logContext.dpHour))
            .withColumn("dpPartition", lit(logContext.dpPartition))
            .withColumn("processStartTime", to_timestamp(lit(logContext.processStartTime)))
            .withColumn("processEndTime", to_timestamp(lit(logContext.processEndTime)))
            .withColumn("writeStatus", lit(logContext.writeStatus.toInt))
            .withColumn("errorMessage", lit(logContext.errorMessage))
            .withColumn("readRowCount", lit(-1))
        auditRecordDf.write.mode("append").saveAsTable(logContext.logTable)
    }

}
