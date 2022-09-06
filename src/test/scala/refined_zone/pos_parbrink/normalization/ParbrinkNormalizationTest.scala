package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.normalization

import refined_zone.hub.model.{OrderChannelType, OrderStateType}
import refined_zone.pos_parbrink.model.PaymentStatusType
import refined_zone.pos_parbrink.normalization.OrderChannelTypeNormalization.{
    normalizeOriginateOrderChannelType,
    normalizeTargetOrderChannelType,
    ParbrinkDestinationNameToCxiOriginateChannel
}
import refined_zone.pos_parbrink.normalization.OrderStateNormalization.normalizeOrderState
import refined_zone.pos_parbrink.normalization.ParbrinkNormalizationTest.{
    ChannelTypeNormalizationTestCase,
    PaymentStatusNormalizationTestCase
}
import refined_zone.pos_parbrink.normalization.PaymentStatusNormalization.normalizePaymentStatus
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.scalatest.Matchers.convertToAnyShouldWrapper

class ParbrinkNormalizationTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("normalize payment status") {

        // given
        val testCases = Seq(
            PaymentStatusNormalizationTestCase(None, None, null),
            PaymentStatusNormalizationTestCase(None, Some(true), PaymentStatusType.Active.value),
            PaymentStatusNormalizationTestCase(None, Some(false), PaymentStatusType.Inactive.value),
            PaymentStatusNormalizationTestCase(Some(false), Some(true), PaymentStatusType.Active.value),
            PaymentStatusNormalizationTestCase(Some(false), Some(false), PaymentStatusType.Inactive.value),
            PaymentStatusNormalizationTestCase(Some(false), None, null),
            PaymentStatusNormalizationTestCase(Some(true), None, PaymentStatusType.Deleted.value)
        )

        for (testCase <- testCases) {
            // when
            PaymentStatusNormalization.normalizePaymentStatus(
                testCase.isDeletedPayment,
                testCase.isActiveTender
                // then
            ) shouldEqual testCase.status
        }
    }

    test("normalize Parbrink payment status udf") {

        // given
        val df = Seq(
            ("id_1", None, None),
            ("id_2", None, Some(true)),
            ("id_3", None, Some(false)),
            ("id_4", Some(false), Some(true)),
            ("id_5", Some(false), Some(false)),
            ("id_6", Some(false), None),
            ("id_7", Some(true), None)
        ).toDF("id", "id_deleted_payment", "is_active_tender")

        // when
        val actual = df
            .withColumn("status", normalizePaymentStatus(col("id_deleted_payment"), col("is_active_tender")))
            .select("id", "status")

        // then
        withClue("Parbrink payment status does not match") {
            val expected = Seq(
                ("id_1", null),
                ("id_2", PaymentStatusType.Active.value),
                ("id_3", PaymentStatusType.Inactive.value),
                ("id_4", PaymentStatusType.Active.value),
                ("id_5", PaymentStatusType.Inactive.value),
                ("id_6", null),
                ("id_7", PaymentStatusType.Deleted.value)
            ).toDF("id", "status")

            assertDataFrameEquals(expected, actual)
        }
    }

    test("normalize Parbrink order state udf") {
        // given
        val df = Seq(
            ("id_1", null),
            ("id_2", ""),
            ("id_3", "abc"),
            ("id_4", "true"),
            ("id_5", "True"),
            ("id_6", "TRUE"),
            ("id_7", "false"),
            ("id_8", "False"),
            ("id_9", "FALSE")
        ).toDF("id", "is_closed")

        // when
        val actual = df
            .withColumn("ord_state_id", normalizeOrderState(col("is_closed")))
            .select("id", "ord_state_id")

        // then
        withClue("Parbrink order state does not match") {
            val expected = Seq(
                ("id_1", OrderStateType.Unknown.code),
                ("id_2", OrderStateType.Unknown.code),
                ("id_3", OrderStateType.Unknown.code),
                ("id_4", OrderStateType.Completed.code),
                ("id_5", OrderStateType.Completed.code),
                ("id_6", OrderStateType.Completed.code),
                ("id_7", OrderStateType.Open.code),
                ("id_8", OrderStateType.Open.code),
                ("id_9", OrderStateType.Open.code)
            ).toDF("id", "ord_state_id")

            assertDataFrameEquals(expected, actual)
        }
    }

    test("normalize Parbrink order channel type") {
        // given
        val testCases = Seq(
            ChannelTypeNormalizationTestCase("For Here", OrderChannelType.PhysicalLane),
            ChannelTypeNormalizationTestCase("FORHERE", OrderChannelType.PhysicalLane),
            ChannelTypeNormalizationTestCase("Uber Eats", OrderChannelType.DigitalApp),
            ChannelTypeNormalizationTestCase("UberEats", OrderChannelType.DigitalApp),
            ChannelTypeNormalizationTestCase("Something", OrderChannelType.Other),
            ChannelTypeNormalizationTestCase("__@#$__", OrderChannelType.Unknown),
            ChannelTypeNormalizationTestCase("", OrderChannelType.Unknown),
            ChannelTypeNormalizationTestCase(" ", OrderChannelType.Unknown),
            ChannelTypeNormalizationTestCase(null, OrderChannelType.Unknown)
        )

        // when
        for (testCase <- testCases) {
            val actual = OrderChannelTypeNormalization.normalizeOrderChannelType(
                testCase.destinationName,
                ParbrinkDestinationNameToCxiOriginateChannel
            )
            // then
            actual shouldEqual testCase.expected
        }

    }

    test("normalize Parbrink order channel type udfs") {
        // given
        val df = Seq(
            ("id_1", null),
            ("id_2", ""),
            ("id_3", "something"),
            ("id_4", " "),
            ("id_5", "___@#%___"),
            ("id_6", "For Here"),
            ("id_7", "Uber Eats")
        ).toDF("id", "destination_name")

        // when
        val actual = df
            .withColumn("ord_originate_channel_id", normalizeOriginateOrderChannelType(col("destination_name")))
            .withColumn("ord_target_channel_id", normalizeTargetOrderChannelType(col("destination_name")))
            .select("id", "ord_originate_channel_id", "ord_target_channel_id")

        // then
        withClue("Parbrink order channel type not match") {
            val expected = Seq(
                ("id_1", OrderChannelType.Unknown.code, OrderChannelType.Unknown.code),
                ("id_2", OrderChannelType.Unknown.code, OrderChannelType.Unknown.code),
                ("id_3", OrderChannelType.Other.code, OrderChannelType.Other.code),
                ("id_4", OrderChannelType.Unknown.code, OrderChannelType.Unknown.code),
                ("id_5", OrderChannelType.Unknown.code, OrderChannelType.Unknown.code),
                ("id_6", OrderChannelType.PhysicalLane.code, OrderChannelType.PhysicalLane.code),
                ("id_7", OrderChannelType.DigitalApp.code, OrderChannelType.PhysicalDelivery.code)
            ).toDF("id", "ord_originate_channel_id", "ord_target_channel_id")

            assertDataFrameEquals(expected, actual)
        }
    }
}

object ParbrinkNormalizationTest {

    case class PaymentStatusNormalizationTestCase(
        isDeletedPayment: Option[Boolean],
        isActiveTender: Option[Boolean],
        status: String
    )

    case class ChannelTypeNormalizationTestCase(destinationName: String, expected: OrderChannelType)
}
