package com.cxi.cdp.data_processing
package support.normalization.udf

import refined_zone.hub.model.{OrderStateType, OrderTenderType}
import refined_zone.pos_square.model.PosSquareOrderStateTypes.PosSquareToCxiOrderStateType
import refined_zone.pos_square.model.PosSquareOrderTenderTypes.PosSquareToCxiTenderType
import support.normalization.udf.OrderStateNormalizationUdfs.normalizeOrderState
import support.normalization.udf.OrderTenderTypeNormalizationUdfs.normalizeOrderTenderType
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class CxiTaxonomyNormalizationUdfsTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("normalize order tender type") {

        // given
        val df = Seq(
            ("id_1", "cash"),
            ("id_2", "SQUARE_GIFT_CARD"),
            ("id_3", "NO_sale"),
            ("id_4", null),
            ("id_5", ""),
            ("id_6", "some string")
        ).toDF("id", "tender_type")

        // when
        val actual = df.select(
            col("id"),
            normalizeOrderTenderType(PosSquareToCxiTenderType)(col("tender_type")).as("tender_type")
        )

        // then
        withClue("order tender type is not correctly normalized") {
            val expected = Seq(
                ("id_1", OrderTenderType.Cash.code),
                ("id_2", OrderTenderType.GiftCard.code),
                ("id_3", OrderTenderType.Other.code),
                ("id_4", OrderTenderType.Unknown.code),
                ("id_5", OrderTenderType.Unknown.code),
                ("id_6", OrderTenderType.Other.code)
            ).toDF("id", "tender_type")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("normalize order state") {

        // given
        val df = Seq(
            ("id_1", "completed"),
            ("id_2", "OPEN"),
            ("id_3", "Cancelled"),
            ("id_4", null),
            ("id_5", ""),
            ("id_6", "some string")
        ).toDF("id", "ord_state")

        // when
        val actual = df.select(
            col("id"),
            normalizeOrderState(PosSquareToCxiOrderStateType)(col("ord_state")).as("ord_state_id")
        )

        // then
        withClue("order state is not correctly normalized") {
            val expected = Seq(
                ("id_1", OrderStateType.Completed.code),
                ("id_2", OrderStateType.Open.code),
                ("id_3", OrderStateType.Cancelled.code),
                ("id_4", OrderStateType.Unknown.code),
                ("id_5", OrderStateType.Unknown.code),
                ("id_6", OrderStateType.Other.code)
            ).toDF("id", "ord_state_id")

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}
