package com.cxi.cdp.data_processing
package raw_zone.file_ingestion_framework

import support.utils.{ContractUtils, PathUtils}
import support.utils.ContractUtils.jobConfigPropName

import java.time.format.DateTimeFormatter
import java.time.LocalDate

case class FileIngestionFrameworkConfig(
    sourcePath: String,
    targetPath: String,
    targetPartitionColumns: Seq[String],
    processName: String,
    sourceEntity: String,
    subEntity: String,
    logTable: String,
    pathParts: Int,
    fileFormat: String,
    fileFormatOptions: Map[String, String],
    transformationName: String,
    saveModeFromContract: String,
    configOptions: Map[String, Any],
    writeOptions: Map[String, String],
    writeOptionsFunctionName: Option[String],
    writeOptionsFunctionParams: Map[String, String],
    auditRunId: Int,
    dpYear: Int,
    dpMonth: Int,
    dpDay: Int,
    dpHour: Int,
    notebookStartTime: String
)

object FileIngestionFrameworkConfig {

    def apply(
        feedDate: LocalDate,
        sourceDateDirFormatter: DateTimeFormatter,
        contractUtils: ContractUtils,
        basePropName: String
    ): FileIngestionFrameworkConfig = {
        val baseSourcePath = "/mnt/" + contractUtils.prop(jobConfigPropName(basePropName, "read.path"))
        val targetPath = "/mnt/" + contractUtils.prop(jobConfigPropName(basePropName, "write.path"))
        FileIngestionFrameworkConfig(
            sourcePath = getSourcePath(baseSourcePath, feedDate, sourceDateDirFormatter),
            targetPath = targetPath,
            targetPartitionColumns =
                contractUtils.propOrElse(jobConfigPropName(basePropName, "write.partitionColumns"), Seq("feed_date")),
            processName = contractUtils.propOrElse(jobConfigPropName(basePropName, "audit.processName"), ""),
            sourceEntity = contractUtils.prop(jobConfigPropName(basePropName, "audit.entity")),
            subEntity = contractUtils.prop(jobConfigPropName(basePropName, "audit.subEntity")),
            logTable = contractUtils.prop(jobConfigPropName(basePropName, "audit.logTable")),
            pathParts = contractUtils.propOrElse[Int](jobConfigPropName(basePropName, "read.pathParts"), 1),
            fileFormat = contractUtils.propOrElse[String](jobConfigPropName(basePropName, "read.fileFormat"), ""),
            fileFormatOptions = contractUtils.propOrElse[Map[String, String]](
                jobConfigPropName(basePropName, "read.fileFormatOptions"),
                Map[String, String]()
            ),
            transformationName = contractUtils
                .propOrElse[String](jobConfigPropName(basePropName, "transform.transformationName"), "identity"),
            saveModeFromContract =
                contractUtils.propOrElse[String](jobConfigPropName(basePropName, "write.saveMode"), "append"),
            configOptions = contractUtils.propOrElse[Map[String, Any]](
                jobConfigPropName(basePropName, "spark.configOptions"),
                Map[String, Any]()
            ),
            writeOptions = contractUtils.propOrElse[Map[String, String]](
                jobConfigPropName(basePropName, "write.writeOptions"),
                Map[String, String]()
            ),
            writeOptionsFunctionName =
                contractUtils.propOrNone(jobConfigPropName(basePropName, "write.writeOptionsFunction")),
            writeOptionsFunctionParams = contractUtils.propOrElse[Map[String, String]](
                jobConfigPropName(basePropName, "write.writeOptionsFunctionParams"),
                Map[String, String]()
            ),
            auditRunId = 1,
            dpYear = feedDate.getYear,
            dpMonth = feedDate.getMonthValue,
            dpDay = feedDate.getDayOfMonth,
            dpHour = 0, // data is being processed on a daily basis
            notebookStartTime = java.time.LocalDateTime.now.toString
        )
    }

    /** Creates the final source path from the base path (which comes from a contract) and the processing date. */
    private def getSourcePath(basePath: String, date: LocalDate, sourceDateDirFormat: DateTimeFormatter): String = {
        PathUtils.concatPaths(basePath, sourceDateDirFormat.format(date))
    }

}
