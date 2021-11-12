package com.cxi.cdp.data_processing
package support.functions

import com.cxi.cdp.data_processing.support.SparkSessionFactory
import com.databricks.service.DBUtils
import io.delta.tables.DeltaTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{lit, to_timestamp}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}


/**
 * @author - Name of Author
 * @createdOn - Date
 * @version - 1.0
 * @Ticket - Ticket tracking details
 * @App-Dependency - App that uses this
 * @function-desc - Description
 */
object UnifiedFrameworkFunctions {

    /**
     * Generic
     *
     * @function-desc - Initiate the Logger process
     */
    def fn_initializeLogger(loggerName: String, logSystem: String, logLevel: String, logAppender: String, isRootLogEnabled: String): Logger = {
        // Logger Configuration
        var logger: Logger = null
        try {

            if (logSystem == "App") {
                logger = Logger.getLogger(loggerName);
                if (isRootLogEnabled == "True") {
                    val appender = logger.getAppender("logAppender")
                    Logger.getRootLogger().addAppender(appender)
                }
            }
            else {
                logger = Logger.getRootLogger();
            }
            logLevel match {
                case "INFO" => logger.setLevel(Level.INFO)
                case "DEBUG" => logger.setLevel(Level.DEBUG)
                case "TRACE" => logger.setLevel(Level.TRACE)
                case _ => logger.info("invalid log level. using cluster default log level")
            }
        }
        catch {
            case e: Throwable =>
                println(e)
        }
        logger
    }

    // COMMAND ----------

    /**
     * Generic
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
    object logs {

        def toTimestamp(x: String, fieldName: String): Column = {
            try {
                to_timestamp(lit(x))
            }
            catch {
                case x: java.lang.NumberFormatException => {
                    throw new IllegalArgumentException(s"Value '${x}' in parameter '${fieldName}' can not be converted to TimestampType")
                }
                case x: java.lang.NullPointerException => {
                    lit(null)
                }
            }
        }

        def write(spark: SparkSession
                  , logTable: String
                  , processName: String
                  , entity: String
                  , runID: Integer
                  , dpYear: Integer
                  , dpMonth: Integer
                  , dpDay: Integer
                  , dpHour: Integer
                  , dpPartition: String
                  , subEntity: String
                  , processStartTime: String
                  , processEndTime: String
                  , writeStatus: Integer
                  , errorMessage: String
                  , readRowCount: Integer): Unit = {
            val dummyDf = spark.createDataFrame(
                spark.sparkContext.parallelize(Seq(Row(1))),
                StructType(List(StructField("number", IntegerType, true)))
            )
            val values = scala.collection.Map(
                "processName" -> lit(processName)
                , "entity" -> lit(entity)
                , "runID" -> lit(runID)
                , "dpYear" -> lit(dpYear)
                , "dpMonth" -> lit(dpMonth)
                , "dpDay" -> lit(dpMonth)
                , "dpHour" -> lit(dpHour)
                , "dpPartition" -> lit(dpPartition)
                , "processStartTime" -> toTimestamp(processStartTime, "processStartTime")
                , "processEndTime" -> toTimestamp(processEndTime, "processEndTime")
                , "writeStatus" -> lit(writeStatus)
                , "errorMessage" -> lit(errorMessage)
                , "readRowCount" -> lit(readRowCount)
            )
            // TODO: replace DeltaTable functionality with pure Spark SQL/DataFrame API, as it's not supported by databricks-connect
            val logTbl = DeltaTable.forName(logTable)
            logTbl.alias("t")
                .merge(dummyDf, "1 = 0")
                .whenNotMatched().insert(values)
                .execute()
        }

    }


    // COMMAND ----------

    /**
     * Generic
     *
     * @function-desc - Writes into Audit Table (Default Log)
     */


    def fn_writeAuditTable(spark: SparkSession, logTable: String = "testing.application_audit_logs", processName: String, entity: String, runID: Int,
                           dpYear: String, dpMonth: String, dpDay: String, dpHour: String, dpPartition: String = null, recordName: String = null,
                           processStartTime: String = null, processEndTime: String = null, writeStatus: String = null, errorMessage: String = "NA",
                           importDF: DataFrame = null, logger: Logger = null): Unit = {
        def toInt(x: String, fieldName: String): Integer = {
            try {
                if (!x.trim.isEmpty)
                    x.trim.toInt
                else 0
            }
            catch {
                case x: java.lang.NumberFormatException => {
                    throw new IllegalArgumentException(s"Value '${x}' in parameter '${fieldName}' can not be converted to Integer")
                }
                case x: java.lang.NullPointerException => {
                    null
                }
            }
        }

        try {
            var readCount: Long = 0.toLong
            if (importDF != null)
                readCount = importDF.count
            logs.write(logTable = logTable
                , processName = processName
                , entity = entity
                , runID = runID
                , dpYear = toInt(dpYear, "dpYear")
                , dpMonth = toInt(dpMonth, "dpMonth")
                , dpDay = toInt(dpDay, "dpDay")
                , dpHour = toInt(dpHour, "dpHour")
                , dpPartition = dpPartition
                , subEntity = null
                , processStartTime = processStartTime
                , processEndTime = processEndTime
                , writeStatus = toInt(writeStatus, "writeStatus")
                , errorMessage = errorMessage
                , readRowCount = readCount.toInt
                , spark = spark)
        }
        catch {
            case e: Throwable =>
                if (logger != null)
                    logger.error("Function 'fn_writeAuditTable' failed with error:" + e.toString(), e)
                println("Function 'fn_writeAuditTable' failed with error:" + e.toString(), e)
        }
    }

    // COMMAND ----------

    /**
     * Generic
     *
     * @function-desc - This Function is to obfuscate (hash/salt)
     */
    def sha256Hash(id: String, salt: String): String = {
        // Hash + Salt Configuration
        var hashValue: String = null
        try {

            val hashValue = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest((id + salt).getBytes("UTF-8"))))
            return hashValue

        }
        catch {
            case e: Throwable =>
                // logger.error("Function 'sha256Hash' failed with error:" + e.toString())
                println("Function 'sha256Hash' failed with error:" + e.toString())
                null
        }
    }

    // COMMAND ----------

    /**
     * Generic
     *
     * @function-desc - This Function is to obfuscate (hash/salt)
     */

    def getWorkspaceProperties(workspaceDetailsPath: String = "dbfs:/databricks/config/workspace_details.json"): Map[String, String] = {
        DBUtils.fs.head(workspaceDetailsPath)
            .replaceAll("^\\{|\\}$", "")
            .split(",")
            .map(e => e.split(":"))
            .map(arr => (arr(0).trim.replaceAll("^\"|\"$", "").trim, arr(1).trim.replaceAll("^\"|\"$", "").trim))
            .toMap
    }

    // COMMAND ----------

    /**
     * Generic
     *
     * @function-desc - This Function is to obfuscate (hash/salt)
     */

    def getKvScopeForCurrentEnv(): String = {
        val wsProps = getWorkspaceProperties()
        val env = wsProps("envType")
        val region = wsProps("region")
        s"$env-$region-keyvault-scope"
    }

    // COMMAND ----------

    /**
     * Generic
     *
     * @function-desc - This function identifies unique value for given Data Frame using the provided control columns and adds them to a where condition.
     */

    def replaceWhereForSingleColumnWriteOption(df: DataFrame, params: Map[String, String]): Map[String, String] = {
        val controlCol = params("controlCol")

        val controlColValuesProcessed = df.select(controlCol)
            .distinct
            .collect
            .map(_ (0))
            .map(r => s"'$r'")
            .mkString(",")

        val replaceWhereCondition = s"$controlCol in ($controlColValuesProcessed)"

        Map[String, String]("replaceWhere" -> replaceWhereCondition)
    }

    val writeOptionsFunctionsMap = Map[String, (DataFrame, Map[String, String]) => Map[String, String]](
        "replaceWhereForSingleColumn" -> replaceWhereForSingleColumnWriteOption
    )
    // COMMAND ----------

    /**
     * Generic
     *
     * @function-desc - Provide a list of all files including those in sub-directories
     */
    def getAllFiles(hadoopConf: Configuration, path: String): Seq[String] = {
        val fs: FileSystem = FileSystem.get(hadoopConf)

        def listRecursive(fs: FileSystem, path: String): Seq[String] = {
            val files = fs.listStatus(new Path(path))
            if (files.isEmpty)
                List()
            else {
                files.map(file => {
                    if (file.isDirectory) listRecursive(fs, file.getPath.toString)
                    else List(file.getPath.toString)
                }).reduce(_ ++ _)
            }
        }
        listRecursive(fs, path)
    }

}
