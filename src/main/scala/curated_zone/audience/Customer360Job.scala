package com.cxi.cdp.data_processing
package curated_zone.audience

import java.nio.file.Paths

import com.cxi.cdp.data_processing.curated_zone.audience.model.Customer360
import com.cxi.cdp.data_processing.curated_zone.audience.service.AudienceService
import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityId
import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Type}
import com.cxi.cdp.data_processing.support.SparkSessionFactory
import com.cxi.cdp.data_processing.support.utils.ContractUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Customer360Job {

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val spark = SparkSessionFactory.getSparkSession()

        // With GraphFrames 0.3.0 and later releases, the default Connected Components algorithm requires setting a Spark checkpoint directory.
        // https://graphframes.github.io/graphframes/docs/_site/user-guide.html#connected-components
        spark.sparkContext.setCheckpointDir("/tmp/checkpoint")

        val contractPath = "/mnt/" + args(0)
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        run(spark, contract)
    }

    def run(spark: SparkSession, contract: ContractUtils): Unit = {
        val refinedHubDb = contract.prop[String]("schema.refined_hub.db_name")
        val identityTable = contract.prop[String]("schema.refined_hub.identity_table")
        val posIdentityRelationshipTable = contract.prop[String]("schema.refined_hub.pos_identity_relationship_table")

        val curatedDb = contract.prop[String]("schema.curated.db_name")
        val customer360Table = contract.prop[String]("schema.curated.pos_customer_360_table")

        val connectedComponents = buildConnectedComponents(
            spark, s"$refinedHubDb.$identityTable", s"$refinedHubDb.$posIdentityRelationshipTable")

        val prevCustomer360 = readCustomer360(spark, s"$curatedDb.$customer360Table")
        val newCustomer360 = AudienceService.updateCustomer360(spark, connectedComponents, prevCustomer360)
        writeCustomer360(newCustomer360, s"$curatedDb.$customer360Table")
    }

    def buildConnectedComponents(spark: SparkSession, identityTable: String, identityRelationshipTable: String): DataFrame = {
        val qualifiedIdentityId = udf(IdentityId.qualifiedIdentityId _)

        val vertexDf = spark.table(identityTable)
            .withColumn(AudienceService.GraphFrames.VertexIdCol, qualifiedIdentityId(col(Type), col(CxiIdentityId)))
            .select(AudienceService.GraphFrames.VertexIdCol)

        val edgeDf = spark.table(identityRelationshipTable)
            .filter(col("active_flag") === true)
            .withColumn(AudienceService.GraphFrames.EdgeSrcCol, qualifiedIdentityId(col("source_type"), col("source")))
            .withColumn(AudienceService.GraphFrames.EdgeDstCol, qualifiedIdentityId(col("target_type"), col("target")))
            .select(AudienceService.GraphFrames.EdgeSrcCol, AudienceService.GraphFrames.EdgeDstCol)

        AudienceService.buildConnectedComponents(vertexDf, edgeDf)
    }

    def readCustomer360(spark: SparkSession, table: String): Dataset[Customer360] = {
        import spark.implicits._
        spark.table(table).as[Customer360]
    }

    def writeCustomer360(ds: Dataset[Customer360], destTable: String): Unit = {
        val df = ds.toDF

        val srcTable = "newCustomer360"
        df.createOrReplaceTempView(srcTable)

        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.customer_360_id <=> $srcTable.customer_360_id
               |WHEN MATCHED
               |  THEN UPDATE SET
               |    identities = $srcTable.identities,
               |    create_date = $srcTable.create_date,
               |    update_date = $srcTable.update_date,
               |    active_flag = $srcTable.active_flag
               |WHEN NOT MATCHED
               |  THEN INSERT (
               |    customer_360_id,
               |    identities,
               |    create_date,
               |    update_date,
               |    active_flag
               |  )
               |  VALUES (
               |    $srcTable.customer_360_id,
               |    $srcTable.identities,
               |    $srcTable.create_date,
               |    $srcTable.update_date,
               |    $srcTable.active_flag
               |  )
               |""".stripMargin)
    }
}
