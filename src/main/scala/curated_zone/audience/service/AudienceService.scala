package com.cxi.cdp.data_processing
package curated_zone.audience.service

import refined_zone.hub.model.CxiIdentity

import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame

object AudienceService {

    /**
      * GraphFrames framework pre-defined column names
      * https://graphframes.github.io/graphframes/docs/_site/user-guide.html#creating-graphframes
      */
    object GraphFrames {
        val VertexIdCol = "id"
        val EdgeSrcCol = "src"
        val EdgeDstCol = "dst"
        val ComponentCol = "component"
    }

    val ComponentIdCol = "component_id"

    def buildConnectedComponents(vertexDf: DataFrame, edgeDf: DataFrame): DataFrame = {
        val identityGraph = GraphFrame(vertexDf, edgeDf)
        identityGraph.connectedComponents.run()
            .withColumnRenamed(GraphFrames.VertexIdCol, CxiIdentity.CxiIdentityId)
            .withColumnRenamed(GraphFrames.ComponentCol, ComponentIdCol)
    }

}
