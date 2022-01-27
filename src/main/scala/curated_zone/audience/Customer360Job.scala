package com.cxi.cdp.data_processing
package curated_zone.audience

import curated_zone.audience.service.AudienceService
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Metadata, Type, Weight}
import support.SparkSessionFactory
import support.packages.utils.ContractUtils

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}

import java.nio.file.Paths

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
        val pos_identityRelationshipTable = contract.prop[String]("schema.refined_hub.pos_identity_relationship_table")

        val vertexDf = spark.table(s"$refinedHubDb.$identityTable")
            .withColumn(AudienceService.GraphFrames.VertexIdCol, concat_ws(":", col(CxiIdentityId), col(Type)))
            .select(AudienceService.GraphFrames.VertexIdCol)

        val edgeDf = spark.table(s"$refinedHubDb.$pos_identityRelationshipTable")
            .withColumn(AudienceService.GraphFrames.EdgeSrcCol, concat_ws(":", col("source"), col("source_type")))
            .withColumn(AudienceService.GraphFrames.EdgeDstCol, concat_ws(":", col("target"), col("target_type")))
            .select(AudienceService.GraphFrames.EdgeSrcCol, AudienceService.GraphFrames.EdgeDstCol)

        val connectedComponents = AudienceService.buildConnectedComponents(vertexDf, edgeDf)

        // TODO: customer 360 logic, DP-1397
        connectedComponents.show()
    }

}
