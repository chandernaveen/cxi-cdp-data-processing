package com.cxi.cdp.data_processing
package support.functions

import io.delta.tables.DeltaTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{lit, to_timestamp}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/** @author - Name of Author
  * @createdOn - Date
  * @version - 1.0
  * @Ticket - Ticket tracking details
  * @App-Dependency - App that uses this
  * @function-desc - Description
  */
object UnifiedFrameworkFunctions {

    /** Generic
      *
      * @function-desc - Initiate the Logger process
      */
    def fnInitializeLogger(
        loggerName: String,
        logSystem: String,
        logLevel: String,
        logAppender: String,
        isRootLogEnabled: String
    ): Logger = {
        // Logger Configuration
        var logger: Logger = null

        if (logSystem == "App") {
            logger = Logger.getLogger(loggerName);
            if (isRootLogEnabled == "True") {
                val appender = logger.getAppender("logAppender")
                Logger.getRootLogger().addAppender(appender)
            }
        } else {
            logger = Logger.getRootLogger();
        }
        logLevel match {
            case "INFO" => logger.setLevel(Level.INFO)
            case "DEBUG" => logger.setLevel(Level.DEBUG)
            case "TRACE" => logger.setLevel(Level.TRACE)
            case _ => logger.info("invalid log level. using cluster default log level")
        }
        logger
    }

    // COMMAND ----------

    /** Generic
      *
      * @function-desc - Generates spark configuration based on contract configuration
      */
    def applySparkConfigOptions(spark: SparkSession, configOptions: Map[String, Any]): Unit = {
        configOptions.foreach {
            case (key, value: String) => spark.conf.set(key, value)
            case (key, value: Boolean) => spark.conf.set(key, value)
            case (key, value: Long) => spark.conf.set(key, value)
            case (key, value: Integer) => spark.conf.set(key, value.toLong)
            case _ =>
        }
    }

    // COMMAND ----------
    object Logs {

        def toTimestamp(x: String, fieldName: String): Column = {
            try {
                to_timestamp(lit(x))
            } catch {
                case x: java.lang.NumberFormatException => {
                    throw new IllegalArgumentException(
                        s"Value '${x}' in parameter '${fieldName}' can not be converted to TimestampType"
                    )
                }
                case x: java.lang.NullPointerException => {
                    lit(null)
                }
            }
        }

        def write(logContext: LogContext, spark: SparkSession, readRowCount: Integer): Unit = {
            val dummyDf = spark.createDataFrame(
                spark.sparkContext.parallelize(Seq(Row(1))),
                StructType(List(StructField("number", IntegerType, true)))
            )
            val values = scala.collection.Map(
                "processName" -> lit(logContext.processName),
                "entity" -> lit(logContext.entity),
                "runID" -> lit(logContext.runID),
                "dpYear" -> lit(logContext.dpYear),
                "dpMonth" -> lit(logContext.dpMonth),
                "dpDay" -> lit(logContext.dpDay),
                "dpHour" -> lit(logContext.dpHour),
                "dpPartition" -> lit(logContext.dpPartition),
                "processStartTime" -> toTimestamp(logContext.processStartTime, "processStartTime"),
                "processEndTime" -> toTimestamp(logContext.processEndTime, "processEndTime"),
                "writeStatus" -> lit(logContext.writeStatus),
                "errorMessage" -> lit(logContext.errorMessage),
                "readRowCount" -> lit(readRowCount)
            )
            // TODO: replace DeltaTable functionality with pure Spark SQL/DataFrame API, as it's not supported by databricks-connect
            val logTbl = DeltaTable.forName(logContext.logTable)
            logTbl
                .alias("t")
                .merge(dummyDf, "1 = 0")
                .whenNotMatched()
                .insert(values)
                .execute()
        }

    }

    def fnWriteAuditTable(
        logContext: LogContext,
        spark: SparkSession,
        importDF: DataFrame = null,
        logger: Logger = null
    ): Unit = {
        try {
            var readCount: Long = 0.toLong
            if (importDF != null) {
                readCount = importDF.count
            }
            Logs.write(logContext = logContext, readRowCount = readCount.toInt, spark = spark)
        } catch {
            case e: Throwable =>
                if (logger != null) {
                    logger.error("Function 'fn_writeAuditTable' failed with error:" + e.toString(), e)
                }
        }
    }

    type WriteOptionsFunction = (DataFrame, Map[String, String]) => Map[String, String]

    def replaceWhereForSingleColumnWriteOption(df: DataFrame, params: Map[String, String]): Map[String, String] = {
        val controlCol = params("controlCol")

        val controlColValuesProcessed = df
            .select(controlCol)
            .distinct
            .collect
            .map(_(0))
            .map(r => s"'$r'")
            .mkString(",")

        val replaceWhereCondition = s"$controlCol in ($controlColValuesProcessed)"

        Map[String, String]("replaceWhere" -> replaceWhereCondition)
    }

    /** Creates WriteOptionsFunction to only overwrite records with the provided feedDate.
      *
      * Without this ingesting data for a specific feedDate will overwrite data for previously imported feedDates.
      * Should be used together with SaveMode.Overwrite.
      *
      * Using this function instead of `replaceWhereForSingleColumnWriteOption` helps to avoid a full scan before write.
      *
      * NOTE: this function is not exposed in `writeOptionsFunctionsMap` as it should be configured with the feedDate.
      * See `FileIngestionFramework` for an example of how to expose this function.
      */
    def replaceWhereForFeedDate(feedDate: String): WriteOptionsFunction = (_, _) => {
        Map("replaceWhere" -> s"feed_date = '$feedDate'")
    }

    val writeOptionsFunctionsMap: Map[String, WriteOptionsFunction] =
        Map("replaceWhereForSingleColumn" -> replaceWhereForSingleColumnWriteOption)

    /** Generic
      *
      * @function-desc - Provide a list of all files including those in sub-directories
      */
    def getAllFiles(hadoopConf: Configuration, path: String): Seq[String] = {
        val fs: FileSystem = FileSystem.get(hadoopConf)

        def listRecursive(fs: FileSystem, path: String): Seq[String] = {
            val files = fs.listStatus(new Path(path))
            if (files.isEmpty) {
                List()
            } else {
                files
                    .map(file => {
                        if (file.isDirectory) listRecursive(fs, file.getPath.toString) else List(file.getPath.toString)
                    })
                    .reduce(_ ++ _)
            }
        }
        listRecursive(fs, path)
    }

}
