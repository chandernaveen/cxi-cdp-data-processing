package com.cxi.cdp.data_processing
package support.change_data_feed

import com.cxi.cdp.data_processing.support.change_data_feed.ChangeDataFeedService.{CdfColumnNames, ChangeType}
import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.Matchers

class CdfDiffFormatTest extends BaseSparkBatchJobTest with Matchers {

    import spark.implicits._
    import CdfDiffFormat._
    import CdfDiffFormatTest._

    test("transformChangeDataFeed when the provided DataFrame is not a ChangeDataFeed") {
        val df = Seq(Seq("Alice"), Seq("Bob")).toDF("name")
        assertThrows[IllegalArgumentException](transformChangeDataFeed(df, Seq("name"))(spark))
    }

    test("transformChangeDataFeed for newly added record") {
        val cdfData = Seq(
            // a simple insert
            BankAccountCdfRecord("Investment Bank", 1L, 0L, _change_type = ChangeType.Insert, _commit_version = 1L),

            // an insert with a further update
            BankAccountCdfRecord("Best Bank", 123L, 0L, _change_type = ChangeType.Insert, _commit_version = 1L),
            BankAccountCdfRecord("Best Bank", 123L, 0L, _change_type = ChangeType.UpdatePreImage, _commit_version = 2L),
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                1200L,
                _change_type = ChangeType.UpdatePostImage,
                _commit_version = 2L
            )
        )

        val expectedDiff = Seq(
            BankAccountDiff(
                previous_record = None,
                current_record = Some(BankAccount("Investment Bank", 1L, 0L))
            ),
            BankAccountDiff(
                previous_record = None,
                current_record = Some(BankAccount("Best Bank", 123L, 1200L))
            )
        )

        verifyTestCase(cdfData, expectedDiff)
    }

    test("transformChangeDataFeed for updated record") {
        val cdfData = Seq(
            // a simple update
            BankAccountCdfRecord(
                "Best Bank",
                111L,
                300L,
                _change_type = ChangeType.UpdatePreImage,
                _commit_version = 3L
            ),
            BankAccountCdfRecord(
                "Best Bank",
                111L,
                200L,
                _change_type = ChangeType.UpdatePostImage,
                _commit_version = 3L
            ),

            // several updates
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                100L,
                _change_type = ChangeType.UpdatePreImage,
                _commit_version = 2L
            ),
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                1200L,
                _change_type = ChangeType.UpdatePostImage,
                _commit_version = 2L
            ),
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                1200L,
                _change_type = ChangeType.UpdatePreImage,
                _commit_version = 5L
            ),
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                900L,
                _change_type = ChangeType.UpdatePostImage,
                _commit_version = 5L
            )
        )

        val expectedDiff = Seq(
            BankAccountDiff(
                previous_record = Some(BankAccount("Best Bank", 111L, 300L)),
                current_record = Some(BankAccount("Best Bank", 111L, 200L))
            ),
            BankAccountDiff(
                previous_record = Some(BankAccount("Best Bank", 123L, 100L)),
                current_record = Some(BankAccount("Best Bank", 123L, 900L))
            )
        )

        verifyTestCase(cdfData, expectedDiff)
    }

    test("transformChangeDataFeed for deleted record") {
        val cdfData = Seq(
            // a simple delete
            BankAccountCdfRecord("Best Bank", 111L, 300L, _change_type = ChangeType.Delete, _commit_version = 3L),

            // update and then delete
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                100L,
                _change_type = ChangeType.UpdatePreImage,
                _commit_version = 2L
            ),
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                1200L,
                _change_type = ChangeType.UpdatePostImage,
                _commit_version = 2L
            ),
            BankAccountCdfRecord("Best Bank", 123L, 1200L, _change_type = ChangeType.Delete, _commit_version = 3L)
        )

        val expectedDiff = Seq(
            BankAccountDiff(
                previous_record = Some(BankAccount("Best Bank", 111L, 300L)),
                current_record = None
            ),
            BankAccountDiff(
                previous_record = Some(BankAccount("Best Bank", 123L, 100L)),
                current_record = None
            )
        )

        verifyTestCase(cdfData, expectedDiff)
    }

    test("transformChangeDataFeed for inserted and then deleted records") {
        val cdfData = Seq(
            // a simple insert and delete
            BankAccountCdfRecord("Best Bank", 111L, 300L, _change_type = ChangeType.Insert, _commit_version = 2L),
            BankAccountCdfRecord("Best Bank", 111L, 300L, _change_type = ChangeType.Delete, _commit_version = 3L),

            // insert, update and then delete
            BankAccountCdfRecord("Best Bank", 123L, 100L, _change_type = ChangeType.Insert, _commit_version = 1L),
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                100L,
                _change_type = ChangeType.UpdatePreImage,
                _commit_version = 2L
            ),
            BankAccountCdfRecord(
                "Best Bank",
                123L,
                1200L,
                _change_type = ChangeType.UpdatePostImage,
                _commit_version = 2L
            ),
            BankAccountCdfRecord("Best Bank", 123L, 1200L, _change_type = ChangeType.Delete, _commit_version = 3L)
        )

        val expectedDiff = Seq.empty[BankAccountDiff]

        verifyTestCase(cdfData, expectedDiff)
    }

    test("transformRegularDataFrame") {
        val dataDF = Seq(
            BankAccount("Best Bank", 123L, 100L),
            BankAccount("Investment Bank", 111L, 123400L)
        ).toDF

        val expectedDiff = Seq(
            BankAccountDiff(
                previous_record = None,
                current_record = Some(BankAccount("Best Bank", 123L, 100L))
            ),
            BankAccountDiff(
                previous_record = None,
                current_record = Some(BankAccount("Investment Bank", 111L, 123400L))
            )
        )

        val actualDF = transformRegularDataFrame(dataDF)
        actualDF.schema shouldBe expectedDiffSchema(dataDF)
        actualDF.collect should contain theSameElementsAs expectedDiff.map(toRow)
    }

    private def expectedDiffSchema(cdfDataDF: DataFrame): StructType = {
        val recordSchema = StructType(cdfDataDF.schema.fields.filter(field => !CdfColumnNames.contains(field.name)))

        StructType(
            Seq(
                StructField(CdfDiffFormat.PreviousRecordColumnName, recordSchema),
                StructField(CdfDiffFormat.CurrentRecordColumnName, recordSchema)
            )
        )
    }

    private def verifyTestCase(cdfData: Seq[BankAccountCdfRecord], expectedDiff: Seq[BankAccountDiff]): Unit = {
        val cdfDataDF = cdfData.toDF
        val actualDF = transformChangeDataFeed(cdfDataDF, CompositeKey)(spark)

        actualDF.schema shouldBe expectedDiffSchema(cdfDataDF)
        actualDF.collect should contain theSameElementsAs expectedDiff.map(toRow)
    }

    private def toRow(bankAccountDiff: BankAccountDiff): Row = {
        Row(
            bankAccountDiff.previous_record.map(toRow).getOrElse(null),
            bankAccountDiff.current_record.map(toRow).getOrElse(null)
        )
    }

    private def toRow(bankAccount: BankAccount): Row = {
        Row(bankAccount.productIterator.toArray: _*)
    }

}

object CdfDiffFormatTest {

    val CompositeKey = Seq("bank", "accountId")

    case class BankAccountDiff(
        previous_record: Option[BankAccount],
        current_record: Option[BankAccount]
    )

    case class BankAccount(
        bank: String,
        accountId: Long,
        balance: Long
    )

    case class BankAccountCdfRecord(
        bank: String,
        accountId: Long,
        balance: Long,
        _change_type: String,
        _commit_version: Long,
        _commit_timestamp: String = ""
    )

}
