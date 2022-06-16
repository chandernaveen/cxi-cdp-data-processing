package com.cxi.cdp.data_processing
package support.change_data_feed

import com.cxi.cdp.data_processing.support.change_data_feed.ChangeDataQueryResult.TableMetadata
import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest

import org.apache.spark.sql.DataFrameReader
import org.mockito.Mockito._
import org.scalatest.{Matchers, OptionValues}

class SimpleUnionSourceTest extends BaseSparkBatchJobTest with Matchers with OptionValues {

    import spark.implicits._
    import SimpleUnionSourceTest._

    val consumerId = "testConsumer"
    val firstTable = "firstTable"
    val secondTable = "secondTable"

    test("SimpleUnion.queryChangeData when schemas are compatible") {
        val sparkSpy = spy(spark)
        val cdfService = mock(classOf[ChangeDataFeedService])

        val firstTableDF = spark.createDataset(Seq(Row("Alice", 25))).toDF
        val firstTableMetadata = TableMetadata("firstTable", 0L, 2L)
        val firstTableChangeData = ChangeDataQueryResult(consumerId, Seq(firstTableMetadata), Some(firstTableDF))

        val secondTableDF = spark.createDataset(Seq(Row("Bob", 33), Row("Chris", 45))).toDF
        val secondTableMetadata = TableMetadata("secondTable", 2L, 3L)
        val secondTableChangeData = ChangeDataQueryResult(consumerId, Seq(secondTableMetadata), Some(secondTableDF))

        doReturn(firstTableDF, Seq.empty: _*).when(sparkSpy).table("firstTable")
        doReturn(secondTableDF, Seq.empty: _*).when(sparkSpy).table("secondTable")

        doReturn(firstTableChangeData, Seq.empty: _*).when(cdfService).queryChangeData(consumerId, firstTable)(sparkSpy)
        doReturn(secondTableChangeData, Seq.empty: _*)
            .when(cdfService)
            .queryChangeData(consumerId, secondTable)(sparkSpy)

        val cdfSource = new ChangeDataFeedSource.SimpleUnion(cdfService, Seq(firstTable, secondTable))

        val expectedResult = ChangeDataQueryResult(
            consumerId,
            Seq(firstTableMetadata, secondTableMetadata),
            Some(firstTableDF.union(secondTableDF))
        )

        val actualResult = cdfSource.queryChangeData(consumerId)(sparkSpy)

        actualResult.consumerId shouldBe expectedResult.consumerId
        actualResult.tableMetadataSeq shouldBe expectedResult.tableMetadataSeq
        actualResult.data.value.collect should contain theSameElementsAs expectedResult.data.value.collect
    }

    test("SimpleUnion.queryChangeData when schemas are incompatible") {
        val sparkSpy = spy(spark)
        val cdfService = mock(classOf[ChangeDataFeedService])

        val firstTableDF = spark.createDataset(Seq(Row("Alice", 25))).toDF
        val firstTableMetadata = TableMetadata("firstTable", 0L, 2L)
        val firstTableChangeData = ChangeDataQueryResult(consumerId, Seq(firstTableMetadata), Some(firstTableDF))

        val secondTableDF = spark.createDataset(Seq(AnotherRow(33))).toDF
        val secondTableMetadata = TableMetadata("secondTable", 2L, 3L)
        val secondTableChangeData = ChangeDataQueryResult(consumerId, Seq(secondTableMetadata), Some(secondTableDF))

        doReturn(firstTableDF, Seq.empty: _*).when(sparkSpy).table("firstTable")
        doReturn(secondTableDF, Seq.empty: _*).when(sparkSpy).table("secondTable")

        doReturn(firstTableChangeData, Seq.empty: _*).when(cdfService).queryChangeData(consumerId, firstTable)(sparkSpy)
        doReturn(secondTableChangeData, Seq.empty: _*)
            .when(cdfService)
            .queryChangeData(consumerId, secondTable)(sparkSpy)

        val cdfSource = new ChangeDataFeedSource.SimpleUnion(cdfService, Seq(firstTable, secondTable))

        assertThrows[IllegalArgumentException] {
            cdfSource.queryChangeData(consumerId)(sparkSpy)
        }
    }

    test("SimpleUnion.queryAll when schemas are compatible") {
        val sparkSpy = spy(spark)

        val firstTableName = "firstTable"
        val firstTableDF = spark.createDataset(Seq(Row("Alice", 25))).toDF
        val firstTableLatestVersion = 2L

        val secondTableName = "secondTable"
        val secondTableDF = spark.createDataset(Seq(Row("Bob", 33), Row("Chris", 45))).toDF
        val secondTableLatestVersion = 5L

        val dataFrameReader = mock(classOf[DataFrameReader])
        doReturn(dataFrameReader, Seq.empty: _*).when(sparkSpy).read
        doReturn(dataFrameReader, Seq.empty: _*).when(dataFrameReader).format("delta")
        doReturn(dataFrameReader, Seq.empty: _*).when(dataFrameReader).option("versionAsOf", firstTableLatestVersion)
        doReturn(dataFrameReader, Seq.empty: _*).when(dataFrameReader).option("versionAsOf", secondTableLatestVersion)
        doReturn(firstTableDF, Seq.empty: _*).when(dataFrameReader).table(firstTableName)
        doReturn(secondTableDF, Seq.empty: _*).when(dataFrameReader).table(secondTableName)

        val cdfService = mock(classOf[ChangeDataFeedService])
        doReturn(firstTableLatestVersion, Seq.empty: _*)
            .when(cdfService)
            .getLatestAvailableVersion(firstTableName)(sparkSpy)
        doReturn(secondTableLatestVersion, Seq.empty: _*)
            .when(cdfService)
            .getLatestAvailableVersion(secondTableName)(sparkSpy)

        val cdfSource = new ChangeDataFeedSource.SimpleUnion(cdfService, Seq(firstTable, secondTable))

        val expectedResult = ChangeDataQueryResult(
            consumerId,
            Seq(
                TableMetadata(firstTableName, 0L, firstTableLatestVersion),
                TableMetadata(secondTableName, 0L, secondTableLatestVersion)
            ),
            Some(firstTableDF.union(secondTableDF))
        )

        val actualResult = cdfSource.queryAllData(consumerId)(sparkSpy)

        actualResult.consumerId shouldBe expectedResult.consumerId
        actualResult.tableMetadataSeq shouldBe expectedResult.tableMetadataSeq
        actualResult.data.value.collect should contain theSameElementsAs expectedResult.data.value.collect
    }
}

object SimpleUnionSourceTest {
    case class Row(name: String, age: Int)
    case class AnotherRow(size: Int)
}
