package com.cxi.cdp.data_processing
package support.change_data_feed

import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest
import org.mockito.Mockito._
import org.scalatest.{Matchers, OptionValues}

class SimpleUnionSourceTest extends BaseSparkBatchJobTest with Matchers with OptionValues {

    import SimpleUnionSourceTest._
    import spark.implicits._

    val consumerId = "testConsumer"
    val firstTable = "firstTable"
    val secondTable = "secondTable"

    test("SimpleUnion.queryChanges when schemas are compatible") {
        val sparkSpy = spy(spark)
        val cdfService = mock(classOf[ChangeDataFeedService])

        val firstTableDF = spark.createDataset(Seq(Row("Alice", 25))).toDF
        val firstTableMetadata = ChangeDataQueryResult.TableMetadata("firstTable", None, 0L, 2L)
        val firstTableChangeData = ChangeDataQueryResult(consumerId, Seq(firstTableMetadata), Some(firstTableDF))

        val secondTableDF = spark.createDataset(Seq(Row("Bob", 33), Row("Chris", 45))).toDF
        val secondTableMetadata = ChangeDataQueryResult.TableMetadata("secondTable", Some(1L), 2L, 3L)
        val secondTableChangeData = ChangeDataQueryResult(consumerId, Seq(secondTableMetadata), Some(secondTableDF))

        doReturn(firstTableDF, Seq.empty: _*).when(sparkSpy).table("firstTable")
        doReturn(secondTableDF, Seq.empty: _*).when(sparkSpy).table("secondTable")

        doReturn(firstTableChangeData, Seq.empty: _*).when(cdfService).queryChangeData(consumerId, firstTable)(sparkSpy)
        doReturn(secondTableChangeData, Seq.empty: _*).when(cdfService).queryChangeData(consumerId, secondTable)(sparkSpy)

        val cdfSource = new ChangeDataFeedSource.SimpleUnion(cdfService, Seq(firstTable, secondTable))

        val expectedResult = ChangeDataQueryResult(
            consumerId,
            Seq(firstTableMetadata, secondTableMetadata),
            Some(firstTableDF.union(secondTableDF)))

        val actualResult = cdfSource.queryChangeData(consumerId)(sparkSpy)

        actualResult.consumerId shouldBe expectedResult.consumerId
        actualResult.tableMetadataSeq shouldBe expectedResult.tableMetadataSeq
        actualResult.changeData.value.collect should contain theSameElementsAs expectedResult.changeData.value.collect
    }

    test("SimpleUnion.queryChanges when schemas are incompatible") {
        val sparkSpy = spy(spark)
        val cdfService = mock(classOf[ChangeDataFeedService])

        val firstTableDF = spark.createDataset(Seq(Row("Alice", 25))).toDF
        val firstTableMetadata = ChangeDataQueryResult.TableMetadata("firstTable", None, 0L, 2L)
        val firstTableChangeData = ChangeDataQueryResult(consumerId, Seq(firstTableMetadata), Some(firstTableDF))

        val secondTableDF = spark.createDataset(Seq(AnotherRow(33))).toDF
        val secondTableMetadata = ChangeDataQueryResult.TableMetadata("secondTable", Some(1L), 2L, 3L)
        val secondTableChangeData = ChangeDataQueryResult(consumerId, Seq(secondTableMetadata), Some(secondTableDF))

        doReturn(firstTableDF, Seq.empty: _*).when(sparkSpy).table("firstTable")
        doReturn(secondTableDF, Seq.empty: _*).when(sparkSpy).table("secondTable")

        doReturn(firstTableChangeData, Seq.empty: _*).when(cdfService).queryChangeData(consumerId, firstTable)(sparkSpy)
        doReturn(secondTableChangeData, Seq.empty: _*).when(cdfService).queryChangeData(consumerId, secondTable)(sparkSpy)

        val cdfSource = new ChangeDataFeedSource.SimpleUnion(cdfService, Seq(firstTable, secondTable))

        assertThrows[IllegalArgumentException] {
            cdfSource.queryChangeData(consumerId)(sparkSpy)
        }
    }

}

object SimpleUnionSourceTest {
    case class Row(name: String, age: Int)
    case class AnotherRow(size: Int)
}


