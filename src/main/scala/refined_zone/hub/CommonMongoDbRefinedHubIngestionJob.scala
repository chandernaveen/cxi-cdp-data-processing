package com.cxi.cdp.data_processing
package refined_zone.hub

import support.utils.ContractUtils
import support.{SparkSessionFactory, WorkspaceConfigReader}

import com.databricks.service.DBUtils
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

object CommonMongoDbRefinedHubIngestionJob {
    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        val spark = SparkSessionFactory.getSparkSession()
        run(spark, cliArgs)
    }

    def run(spark: SparkSession, cliArgs: CliArgs): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(cliArgs.contractPath))

        val mongoDbConfig = getMongoDbConfig(spark, contract)

        val mongoDbDatabase = contract.prop[String](getJobConfigProp("src_db"))
        val mongoDbCollection = contract.prop[String](getJobConfigProp("src_collection"))
        val destDb = contract.prop[String](getJobConfigProp("dest_db"))
        val destTable = contract.prop[String](getJobConfigProp("dest_table"))
        val columnsMapping = ColumnsMapping(contract.prop[List[Map[String, String]]](getJobConfigProp("column_mapping")))
        val destTableKeys = contract.prop[List[String]](getJobConfigProp("dest_table_keys"))

        val sourceCollectionDf = read(spark, mongoDbConfig.uri, mongoDbDatabase, mongoDbCollection)
        val transformedCollectionDf = transform(sourceCollectionDf, columnsMapping, destTableKeys, cliArgs.feedDate, cliArgs.dropDuplicates)
        write(transformedCollectionDf, s"$destDb.$destTable", destTableKeys)
    }

    def read(spark: SparkSession, mongoUri: String, mongoDbDatabase: String, mongoDbCollection: String): DataFrame = {
        val readConfig = ReadConfig(
            Map(
                "uri" -> mongoUri,
                "database" -> mongoDbDatabase,
                "collection" -> mongoDbCollection,
                "readPreference.name" -> "secondaryPreferred"
            ))
        MongoSpark.load(spark, readConfig)
    }

    def transform(sourceCollectionDf: DataFrame,
                  columnsMapping: ColumnsMapping,
                  destTableKeys: List[String],
                  date: String,
                  dropDuplicates: Boolean): DataFrame = {
        val selectedCollectionColumns = sourceCollectionDf.select(columnsMapping.sourceColumns.map(col): _*)

        val res = columnsMapping.mappings.foldLeft(selectedCollectionColumns) {
            case (acc, colMapping) =>
                val renamedColumn = acc
                    .withColumnRenamed(colMapping.sourceColName, colMapping.destColName)

                colMapping.castDataType
                    .map(dt => renamedColumn.withColumn(colMapping.destColName, col(colMapping.destColName).cast(dt)))
                    .getOrElse(renamedColumn)
        }

        if (dropDuplicates) {
            res.withColumn("feed_date", lit(date))
                .dropDuplicates(destTableKeys)
        } else {
            res.withColumn("feed_date", lit(date))
        }
    }

    def write(df: DataFrame, destTable: String, destTableKeys: List[String]): Unit = {
        val srcTable = s"newData"

        df.createOrReplaceTempView(srcTable)

        val joinCondition: String = constructJoinCondition(destTableKeys, srcTable, destTable)
        val columnsToUpdate: String = constructColumnsToUpdate(df.columns, srcTable)
        val columnsToInsert: String = constructColumnsToInsert(df.columns, srcTable)

        val resultingMergeQuery =
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $joinCondition
               |WHEN MATCHED
               |  THEN UPDATE SET $columnsToUpdate
               |WHEN NOT MATCHED
               |  THEN INSERT $columnsToInsert
               |""".stripMargin
        logger.info(s"Resulting merge query: $resultingMergeQuery")
        df.sqlContext.sql(resultingMergeQuery)
    }

    def constructColumnsToUpdate(columns: Array[String], srcTable: String): String = {
        columns.map(destCol => s"$destCol = $srcTable.$destCol").mkString(", ")
    }

    def constructColumnsToInsert(columns: Array[String], srcTable: String): String = {
        val columnsClausePart = columns.mkString("(", ", ", ")")
        val columnsValuesPart = columns.map(destCol => s"$srcTable.$destCol").mkString("VALUES (", ", ", ")")
        s"$columnsClausePart $columnsValuesPart"
    }

    def constructJoinCondition(destTableKeys: List[String], srcTable: String, destTable: String): String = {
        val commonDestKey = "feed_date"
        (commonDestKey +: destTableKeys).map(key => s"$destTable.$key <=> $srcTable.$key").mkString(" AND ")
    }

    case class MongoDbConfig(username: String, password: String, scheme: String, host: String) {
        def uri: String = s"$scheme$username:$password@$host"
    }

    case class ColumnsMapping(colConfigs: List[Map[String, String]]) {
        val mappings: List[ColumnMapping] = colConfigs.map(colConfig =>
            ColumnMapping(colConfig("source_col"), colConfig("dest_col"), colConfig.get("cast_data_type")))
        val sourceColumns: List[String] = mappings.map(_.sourceColName)
    }

    case class ColumnMapping(sourceColName: String, destColName: String, castDataType: Option[String])

    def getMongoDbConfig(spark: SparkSession, contract: ContractUtils): MongoDbConfig = {
        val workspaceConfigPath: String = contract.prop[String]("databricks_workspace_config")
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)

        val username = DBUtils.secrets.get(
            workspaceConfig.azureKeyVaultScopeName,
            contract.prop[String]("mongo.username_secret_key"))
        val password = DBUtils.secrets.get(
            workspaceConfig.azureKeyVaultScopeName,
            contract.prop[String]("mongo.password_secret_key"))
        val scheme = contract.prop[String]("mongo.scheme")
        val host = DBUtils.secrets.get(
            workspaceConfig.azureKeyVaultScopeName,
            contract.prop[String]("mongo.host_secret_key"))

        MongoDbConfig(username = username, password = password, scheme = scheme, host = host)
    }

    def getJobConfigProp(relativePath: String): String = s"jobs.databricks.refined_hub_mongo_ingestion_job.job_config.$relativePath"

    case class CliArgs(contractPath: String, feedDate: String, dropDuplicates: Boolean)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null, feedDate = null, dropDuplicates = false)

        private def optionsParser = new scopt.OptionParser[CliArgs](CommonMongoDbRefinedHubIngestionJob.getClass.getName) {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[String]("feed-date")
                .action((feedDate, c) => c.copy(feedDate = feedDate))
                .text("feed date to process (format: yyyy-MM-dd)")
                .required

            opt[Boolean]("drop-duplicates")
                .action((dropDuplicates, c) => c.copy(dropDuplicates = dropDuplicates))
                .text("whether to drop duplicates from the source or not (true/false)")
                .required
        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser.parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
