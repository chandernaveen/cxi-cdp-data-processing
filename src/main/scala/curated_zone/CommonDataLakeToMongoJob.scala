package com.cxi.cdp.data_processing
package curated_zone

import support.utils.{ContractUtils, TransformUtils}
import support.utils.mongodb.MongoDbConfigUtils
import support.utils.mongodb.MongoDbConfigUtils.{MongoDbConfig, MongoSparkConnectorClass}
import support.utils.JsonUtils.sparkSession
import support.utils.TransformUtils.ColumnsMapping
import support.SparkSessionFactory

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.mongodb.spark.config.ReadConfig.mongoURIProperty
import com.mongodb.spark.config.WriteConfig.{
    collectionNameProperty,
    databaseNameProperty,
    replaceDocumentProperty,
    shardKeyProperty
}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

object CommonDataLakeToMongoJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val spark = SparkSessionFactory.getSparkSession()

        val contractPath = "/mnt/" + args(0)
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        run(spark, contract)
    }

    def run(spark: SparkSession, contract: ContractUtils): Unit = {

        val datalakeSourceDb = contract.prop[String]("datalake.source_db_name")
        val datalakeSourceTable = contract.prop[String]("datalake.source_table")
        val columnsMappingOptions = contract.propOrNone[List[Map[String, String]]]("datalake.column_mapping")
        val includeAllSourceColumns = contract.propOrElse[Boolean]("datalake.include_all_source_columns", true)
        val saveMode = contract.prop[String]("mongo.save_mode")

        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)
        val mongoDestDbName = contract.prop[String]("mongo.dest_db_name")
        val mongoDestCollection = contract.prop[String]("mongo.dest_collection")

        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        val shardKey = mapper.writeValueAsString(contract.prop[Map[String, Int]]("mongo.shardKey"))
        val transformedDf = process(
            sparkSession,
            s"$datalakeSourceDb.$datalakeSourceTable",
            columnsMappingOptions,
            includeAllSourceColumns
        )

        write(transformedDf, mongoDestDbName, mongoDestCollection, mongoDbConfig, shardKey, saveMode)
    }

    def process(
        spark: SparkSession,
        sourceTable: String,
        columnsMappingOptions: Option[Seq[Map[String, String]]],
        includeAllSourceColumns: Boolean
    ): DataFrame = {

        val sourceDf = spark.table(sourceTable)
        maybeTransform(sourceDf, columnsMappingOptions, includeAllSourceColumns)
    }

    private def maybeTransform(
        sourceDf: DataFrame,
        columnsMappingOptions: Option[Seq[Map[String, String]]],
        includeAllSourceColumns: Boolean
    ): DataFrame = {
        columnsMappingOptions match {
            case Some(columnsMapping) =>
                TransformUtils.applyColumnMapping(sourceDf, ColumnsMapping(columnsMapping), includeAllSourceColumns)
            case None => sourceDf
        }
    }

    private def write(
        dataFrame: DataFrame,
        mongoDestDbName: String,
        mongoDestCollection: String,
        mongoDbConfig: MongoDbConfig,
        shardKey: String,
        saveMode: String
    ): Unit = {

        dataFrame.write
            .format(MongoSparkConnectorClass)
            .mode(saveMode)
            .option(databaseNameProperty, mongoDestDbName)
            .option(collectionNameProperty, mongoDestCollection)
            .option(replaceDocumentProperty, "true")
            .option(mongoURIProperty, mongoDbConfig.uri)
            .option(shardKeyProperty, shardKey)
            .save()
    }
}
