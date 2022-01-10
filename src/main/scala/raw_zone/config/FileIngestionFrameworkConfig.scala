package com.cxi.cdp.data_processing
package raw_zone.config

import support.packages.utils.ContractUtils
import support.packages.utils.ContractUtils.jobConfigPropName

import com.databricks.service.DBUtils
import org.apache.spark.sql.types.{DataType, StructType}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.Seq

case class FileIngestionFrameworkConfig
    (
        sourcePath: String,
        targetPath: String,
        targetPartitionColumns: Seq[String],
        processName: String,
        sourceEntity: String,
        logTable: String,
        pathParts: Int,
        fileFormat: String,
        fileFormatOptions: Map[String, String],
        transformationName: String,
        schema: Option[StructType],
        saveModeFromContract: String,
        configOptions: Map[String, Any],
        writeOptions: Map[String, String],
        writeOptionsFunctionName: Option[String],
        writeOptionsFunctionParams: Map[String, String],
        runID: Int,
        dpYear: Int,
        dpMonth: Int,
        dpDay: Int,
        dpHour: Int,
        notebookStartTime: String
    )

object FileIngestionFrameworkConfig {

    private final val PathDelimiter = "/"

    def apply
        (feedDate: LocalDate, sourceDateDirFormatter: DateTimeFormatter, contractUtils: ContractUtils, basePropName: String): FileIngestionFrameworkConfig = {

        val schemaPath = contractUtils.propOrElse[String](jobConfigPropName(basePropName, "read.schemaPath"), "")

        FileIngestionFrameworkConfig(
            sourcePath = getSourcePath("/mnt/"
                + contractUtils.prop(jobConfigPropName(basePropName, "read.path")), feedDate, sourceDateDirFormatter),
            targetPath = "/mnt/" + contractUtils.prop(jobConfigPropName(basePropName, "write.path")),
            targetPartitionColumns = try {
                contractUtils.prop(jobConfigPropName(basePropName, "write.partitionColumns"))
            } catch {
                case _: Throwable => Seq[String]("feed_date")
            },
            processName = contractUtils.prop(jobConfigPropName(basePropName, "audit.processName")),
            sourceEntity = contractUtils.prop(jobConfigPropName(basePropName, "audit.entity")),
            logTable = contractUtils.prop(jobConfigPropName(basePropName, "audit.logTable")),
            pathParts = contractUtils.propOrElse[Int](jobConfigPropName(basePropName, "read.pathParts"), 1),
            fileFormat = contractUtils.propOrElse[String](jobConfigPropName(basePropName, "read.fileFormat"), ""),
            fileFormatOptions = contractUtils.propOrElse[Map[String, String]](jobConfigPropName(basePropName, "read.fileFormatOptions"), Map[String, String]()),
            transformationName = contractUtils.propOrElse[String](jobConfigPropName(basePropName, "transform.transformationName"), "identity"),
            schema = if (schemaPath.nonEmpty) Some(DataType.fromJson(DBUtils.fs.head(s"/mnt/$schemaPath")).asInstanceOf[StructType]) else None,
            saveModeFromContract = contractUtils.propOrElse[String](jobConfigPropName(basePropName, "write.saveMode"), "append"),
            configOptions = contractUtils.propOrElse[Map[String, Any]](jobConfigPropName(basePropName, "spark.configOptions"), Map[String, Any]()),
            writeOptions = contractUtils.propOrElse[Map[String, String]](jobConfigPropName(basePropName, "write.writeOptions"), Map[String, String]()),
            writeOptionsFunctionName = contractUtils.propOrNone(jobConfigPropName(basePropName, "write.writeOptionsFunction")),
            writeOptionsFunctionParams =
                contractUtils.propOrElse[Map[String, String]](jobConfigPropName(basePropName, "write.writeOptionsFunctionParams"), Map[String, String]()),
            runID = 1,
            dpYear = feedDate.getYear,
            dpMonth = feedDate.getMonthValue,
            dpDay = feedDate.getDayOfMonth,
            dpHour = 0, // data is being processed on a daily basis
            notebookStartTime = java.time.LocalDateTime.now.toString
        )
    }

    /** Creates the final source path from the base path (which comes from a contract) and the processing date. */
    private def getSourcePath(basePath: String, date: LocalDate, sourceDateDirFormat: DateTimeFormatter): String = {
        concatPaths(basePath, sourceDateDirFormat.format(date))
    }

    private def concatPaths(parent: String, child: String) = {
        if (parent.endsWith(PathDelimiter)) {
            parent + child
        } else {
            parent + PathDelimiter + child
        }
    }
}
