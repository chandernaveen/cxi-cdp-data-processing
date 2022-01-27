package com.cxi.cdp.data_processing
package curated_zone.audience.service

import refined_zone.hub.model.CxiIdentity
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.collect_list
import org.scalatest.Matchers

class AudienceServiceTest extends BaseSparkBatchJobTest with Matchers {

    import spark.implicits._

    case class TestCaseInput(vertices: Seq[String], edges: Seq[(String, String)], expectedClusters: Array[Seq[String]])

    test("test connected components") {
        // given
        val testCases = Seq(
            TestCaseInput(
                vertices = Seq("1", "2", "3", "4", "5"),
                edges = Seq(("1", "2"), ("3", "4"), ("2", "5")),
                expectedClusters = Array(Seq("1", "2", "5"), Seq("3", "4"))),
            TestCaseInput(
                vertices = Seq("1", "2", "3", "4", "5"),
                edges = Seq(),
                expectedClusters = Array(Seq("1"), Seq("2"), Seq("3"), Seq("4"), Seq("5"))),
            TestCaseInput(
                vertices = Seq("1", "2", "3", "4", "5"),
                edges = Seq(("1", "2"), ("2", "3"), ("3", "4"), ("4", "5")),
                expectedClusters = Array(Seq("1", "2", "3", "4", "5"))),
            TestCaseInput(
                vertices = Seq("1", "2", "3", "4", "5"),
                edges = Seq(("1", "2"), ("1", "2"), ("3", "4"), ("3", "4")),
                expectedClusters = Array(Seq("1", "2"), Seq("3", "4"), Seq("5")))
        )

        // when
        for (testCase <- testCases) {
            val connectedComponents = AudienceService
                .buildConnectedComponents(testCase.vertices.toDF("id"), testCase.edges.toDF("src", "dst"))
                .sort(CxiIdentity.CxiIdentityId)

            // then
            connectedComponents.count() shouldBe testCase.vertices.length
            val clusters = connectedComponents
                .groupBy("component_id")
                .agg(collect_list(CxiIdentity.CxiIdentityId) as "ids")
                .collect()
                .map(c => c.getAs[Seq[String]]("ids"))
            clusters shouldBe testCase.expectedClusters
        }
    }
}

