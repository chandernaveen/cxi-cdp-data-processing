package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.{OrderChannelType, OrderStateType}
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, CxiIdentityIds}
import refined_zone.pos_toast.model.PosToastOrderChannelTypes.PosToastToCxiTargetChannelType
import refined_zone.pos_toast.OrderSummaryProcessorTest._
import support.normalization.DateNormalization.parseToSqlDate
import support.normalization.TimestampNormalization.parseToTimestamp
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest {
    import spark.implicits._

    test("test readOrderSummary") {
        // given
        import spark.implicits._
        val orders = List(
            (
                s"""
                   |{
                   |   "approvalStatus":"NEEDS_APPROVAL",
                   |   "businessDate":20220311,
                   |   "closedDate": "2019-08-24T14:15:22Z",
                   |   "guid": "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                   |   "diningOption": {
                   |      "guid": "d8858e8e-67bc-4bd5-9b48-be29682aa03d"
                   |    },
                   |   "checks":[
                   |      {
                   |         "amount":7248.64,
                   |         "entityType":"Check",
                   |         "guid":"2370f247-2ef9-4b2b-a074-6773c5a4b76a",
                   |         "closedDate":"2022-03-11T17:02:52.455+0000"
                   |      }
                   |   ]
                   |}
                   |""".stripMargin,
                "f987f24a-2ee9-4550-8e76-1fad471c1136",
                "orders",
                "2022-02-24"
            ),
            (
                s"""
                   {
                       "approvalStatus":"APPROVED",
                       "businessDate":20220428,
                       "closedDate": "2022-04-28T16:54:24.768+0000",
                       "guid": "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                       "diningOption": {
                           "guid": "fb2e2cc0-3788-43e7-8175-89044ae8bcbe"
                       },
                       "checks":[
                          {
                             "amount":14.74,
                             "closedDate":"2022-04-28T16:54:24.768+0000",
                             "entityType":"Check",
                             "guid":"1d820fc7-14ff-4c78-b293-b46d8d687c34"
                          }
                       ]
                    }
                   """,
                "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                "orders",
                "2022-02-24"
            ),
            (
                s"""
                   {
                       "approvalStatus":"NEEDS_APPROVAL",
                       "businessDate":20220311,
                       "checks":[],
                       "guid": "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9"
                   }
                   """,
                "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                "orders",
                "2021-10-10"
            ) // duplicate with diff feed date that gets filtered out
        ).toDF("record_value", "location_id", "record_type", "feed_date")

        val tableName = "orders"
        orders.createOrReplaceTempView(tableName)

        // when
        val actual = OrderSummaryProcessor.readOrderSummary(spark, "2022-02-24", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "ord_id",
                "ord_timestamp",
                "ord_state",
                "diningOption",
                "location_id",
                "check"
            )
        }
        val actualData = actual.collect()
        withClue("POS Toast orders data do not match") {
            val expected = List(
                (
                    "ac814db7-3d6a-44c7-8bf7-135c1ba78cf9",
                    "2019-08-24T14:15:22Z",
                    "NEEDS_APPROVAL",
                    Tuple1("d8858e8e-67bc-4bd5-9b48-be29682aa03d"),
                    "f987f24a-2ee9-4550-8e76-1fad471c1136",
                    (7248.64, "2022-03-11T17:02:52.455+0000", "Check", "2370f247-2ef9-4b2b-a074-6773c5a4b76a")
                ),
                (
                    "0af3975e-d9bd-49d5-bf1a-db36cf70258b",
                    "2022-04-28T16:54:24.768+0000",
                    "APPROVED",
                    Tuple1("fb2e2cc0-3788-43e7-8175-89044ae8bcbe"),
                    "d8858e8e-67bc-4bd5-9b48-be29682aa03d",
                    (14.74, "2022-04-28T16:54:24.768+0000", "Check", "1d820fc7-14ff-4c78-b293-b46d8d687c34")
                )
            ).toDF(
                "ord_id",
                "ord_timestamp",
                "ord_state",
                "diningOption",
                "location_id",
                "check"
            ).collect()
            actualData.length should equal(expected.length)
            actualData should contain theSameElementsAs expected
        }

    }

    test("test readDiningOptions") {
        // given
        val diningOptions = List(
            (
                """
                  |{"behavior":"TAKE_OUT","curbside":false,"entityType":"DiningOption","guid":"0c6a3c37-936d-446b-af03-d461a9d80c98","name":"Pickup"}
                  |""".stripMargin,
                "dining-options",
                "2022-02-24"
            ),
            (
                """
                  |{"behavior":"TAKE_OUT","curbside":false,"entityType":"DiningOption","guid":"0c6a3c37-936d-446b-af03-d461a9d80c98","name":"Pickup"}
                  |""".stripMargin,
                "dining-options",
                "2022-02-24"
            ), // duplicate, filtered out
            (
                """
                  |{"behavior":"DINE_IN","curbside":false,"entityType":"DiningOption","guid":"1bf29ca2-c3a3-42fc-8eb8-0129f7f83baa","name":"To Go - No Bag"}
                  |""".stripMargin,
                "dining-options",
                "2022-02-24"
            ),
            (
                """
                  |{"behavior":"TAKE_OUT","curbside":false,"entityType":"DiningOption","guid":"1c0e3c93-d9ff-4dfd-8a8f-40446c76dcd6","name":"Delivery by GrubHub"}
                  |""".stripMargin,
                "dining-options",
                "2022-02-20"
            ) // diff date, filtered out
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "dining_options"
        diningOptions.createOrReplaceTempView(tableName)

        // when
        val actual = OrderSummaryProcessor.readDiningOptions(spark, "2022-02-24", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("dining_option_guid", "dining_option_behavior")
        }
        val actualData = actual.collect()
        withClue("POS Toast dining options data do not match") {
            val expected = List(
                (
                    "0c6a3c37-936d-446b-af03-d461a9d80c98",
                    "TAKE_OUT"
                ),
                (
                    "1bf29ca2-c3a3-42fc-8eb8-0129f7f83baa",
                    "DINE_IN"
                )
            ).toDF("dining_option_guid", "dining_option_behavior").collect()
            actualData should contain theSameElementsAs expected
        }

    }

    test("test getOrdTargetChannelId") {

        val unknownType = "some unknown type"

        val inputDF = List[OrdTargetChannelIdTestCase](
            OrdTargetChannelIdTestCase(behavior = null, expectedOrdTargetChannelId = OrderChannelType.Unknown.code),
            OrdTargetChannelIdTestCase(
                behavior = "DINE_IN",
                expectedOrdTargetChannelId = OrderChannelType.PhysicalLane.code
            ),
            OrdTargetChannelIdTestCase(
                behavior = "TAKE_OUT",
                expectedOrdTargetChannelId = OrderChannelType.PhysicalPickup.code
            ),
            OrdTargetChannelIdTestCase(
                behavior = "DELIVERY",
                expectedOrdTargetChannelId = OrderChannelType.PhysicalDelivery.code
            ),
            OrdTargetChannelIdTestCase(behavior = unknownType, expectedOrdTargetChannelId = OrderChannelType.Other.code)
        ).toDS

        val results = inputDF
            .withColumn(
                "actualOrdTargetChannelId",
                OrderSummaryProcessor.getOrdTargetChannelId(PosToastToCxiTargetChannelType)(col("behavior"))
            )
            .collect

        results.foreach { result =>
            val toastChannelType = result.getAs[String]("behavior")
            val expectedOrdTargetChannelId = result.getAs[Int]("expectedOrdTargetChannelId")
            val actualOrdTargetChannelId = result.getAs[Int]("actualOrdTargetChannelId")
            withClue(s"actual channel type id does not match: $toastChannelType") {
                actualOrdTargetChannelId shouldBe expectedOrdTargetChannelId
            }
        }
    }

    test("test transformOrderSummary") {
        // given
        val date = "2022-02-24"
        val cxiPartnerId = "cxi-usa-some-partner"
        val orderSummary = List(
            OrderSummaryTransformInput(
                ord_id = "ord1",
                ord_timestamp = "2019-01-04T18:18:53.775+0000",
                ord_state = "APPROVED",
                diningOption = DiningOption(guid = "dining_option_1"),
                location_id = "loc1",
                check = Check(
                    totalAmount = Some(14.74),
                    selections = List(
                        Selection(
                            item = Item(guid = "guid_item1"),
                            quantity = Some(1),
                            tax = Some(1.5),
                            appliedTaxes = List(AppliedTax(guid = "guid_appl_tax1")),
                            guid = "guid_check1",
                            price = Some(10.5)
                        )
                    ),
                    taxAmount = Some(5.5),
                    appliedDiscounts =
                        List(AppliedDiscount(discountAmount = Some(8.1), discount = Discount(guid = "guid_disc1"))),
                    payments = List(Payment(guid = "guid_payment1", tipAmount = Some(40))),
                    appliedServiceCharges = List(AppliedServiceCharge(chargeAmount = Some(20)))
                )
            ),
            OrderSummaryTransformInput(
                ord_id = "ord2",
                ord_timestamp = "2019-01-04T18:48:53.780+0000",
                ord_state = "NEEDS_APPROVAL",
                diningOption = DiningOption(guid = "dining_option_2"),
                location_id = "loc1",
                check = Check(
                    totalAmount = Some(7248.64),
                    selections = List(
                        Selection(
                            item = Item(guid = "guid_item2"),
                            quantity = Some(2),
                            tax = Some(2.5),
                            appliedTaxes = List(AppliedTax(guid = "guid_appl_tax2")),
                            guid = "guid_check2",
                            price = Some(22.5)
                        )
                    ),
                    taxAmount = Some(15.5),
                    appliedDiscounts = List(
                        AppliedDiscount(discountAmount = Some(18.1), discount = Discount(guid = "guid_disc2"))
                    ),
                    payments = List(Payment(guid = "guid_payment2", tipAmount = Some(4))),
                    appliedServiceCharges = List(AppliedServiceCharge(chargeAmount = Some(10)))
                )
            )
        ).toDS().toDF()
        val posIdentitiesByOrderIdAndLocationId = List(
            ("ord1", "loc1", List(Map("identity_type" -> IdentityType.Email.code, CxiIdentityId -> "email_hash"))),
            ("ord2", "loc1", List(Map("identity_type" -> IdentityType.Phone.code, CxiIdentityId -> "phone_hash")))
        ).toDF("order_id", "location_id", CxiIdentityIds)
        val diningOptions = List(
            (
                "dining_option1",
                "TAKE_OUT"
            ),
            (
                "dining_option_2",
                "DINE_IN"
            )
        ).toDF("dining_option_guid", "dining_option_behavior")

        // when
        val actual = OrderSummaryProcessor.transformOrderSummary(
            orderSummary,
            date,
            cxiPartnerId,
            posIdentitiesByOrderIdAndLocationId,
            diningOptions
        )

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "ord_id",
                "ord_desc",
                "ord_total",
                "ord_date",
                "ord_timestamp",
                "discount_amount",
                "cxi_partner_id",
                "location_id",
                "ord_state_id",
                "ord_type",
                "ord_originate_channel_id",
                "ord_target_channel_id",
                "item_quantity",
                "item_total",
                "emp_id",
                "discount_id",
                "dsp_qty",
                "dsp_ttl",
                "guest_check_line_item_id",
                "line_id",
                "taxes_id",
                "taxes_amount",
                "item_id",
                "item_price_id",
                "reason_code_id",
                "service_charge_id",
                "service_charge_amount",
                "total_taxes_amount",
                "total_tip_amount",
                "tender_ids",
                "ord_pay_total",
                "ord_sub_total",
                "feed_date",
                "cxi_identity_ids"
            )
        }
        val actualData = actual.collect()
        withClue("POS Toast orders data do not match") {
            val expectedData = List(
                OrderSummaryTransformResult(
                    ord_id = "ord1",
                    ord_desc = null,
                    ord_total = Some(14.74),
                    ord_date = parseToSqlDate("2019-01-04").get,
                    ord_timestamp = parseToTimestamp("2019-01-04T18:18:53.775+00:00").get,
                    discount_amount = Some(8.10),
                    cxi_partner_id = "cxi-usa-some-partner",
                    location_id = "loc1",
                    ord_state_id = OrderStateType.Completed.code,
                    ord_type = null,
                    ord_originate_channel_id = OrderChannelType.Unknown.code,
                    ord_target_channel_id = OrderChannelType.Unknown.code,
                    item_quantity = Some(1),
                    item_total = Some(10.50),
                    emp_id = null,
                    discount_id = null,
                    dsp_qty = null,
                    dsp_ttl = null,
                    guest_check_line_item_id = "guid_check1",
                    line_id = null,
                    taxes_id = Array("guid_appl_tax1"),
                    taxes_amount = Some(1.50),
                    item_id = "guid_item1",
                    item_price_id = null,
                    reason_code_id = null,
                    service_charge_id = null,
                    service_charge_amount = Some(20.00),
                    total_taxes_amount = Some(5.50),
                    total_tip_amount = Some(40),
                    tender_ids = Array("guid_payment1"),
                    ord_pay_total = Some(14.74),
                    ord_sub_total = Some(-50.76),
                    feed_date = parseToSqlDate("2022-02-24").get,
                    cxi_identity_ids =
                        List(Map("identity_type" -> IdentityType.Email.code, CxiIdentityId -> "email_hash"))
                ),
                OrderSummaryTransformResult(
                    ord_id = "ord2",
                    ord_desc = null,
                    ord_total = Some(7248.64),
                    ord_date = parseToSqlDate("2019-01-04").get,
                    ord_timestamp = parseToTimestamp("2019-01-04T18:48:53.780+00:00").get,
                    discount_amount = Some(18.10),
                    cxi_partner_id = "cxi-usa-some-partner",
                    location_id = "loc1",
                    ord_state_id = OrderStateType.Open.code,
                    ord_type = null,
                    ord_originate_channel_id = OrderChannelType.PhysicalLane.code,
                    ord_target_channel_id = OrderChannelType.PhysicalLane.code,
                    item_quantity = Some(2),
                    item_total = Some(22.50),
                    emp_id = null,
                    discount_id = null,
                    dsp_qty = null,
                    dsp_ttl = null,
                    guest_check_line_item_id = "guid_check2",
                    line_id = null,
                    taxes_id = Array("guid_appl_tax2"),
                    taxes_amount = Some(2.50),
                    item_id = "guid_item2",
                    item_price_id = null,
                    reason_code_id = null,
                    service_charge_id = null,
                    service_charge_amount = Some(10.00),
                    total_taxes_amount = Some(15.50),
                    total_tip_amount = Some(4.00),
                    tender_ids = Array("guid_payment2"),
                    ord_pay_total = Some(7248.64),
                    ord_sub_total = Some(7219.14),
                    feed_date = parseToSqlDate("2022-02-24").get,
                    cxi_identity_ids =
                        List(Map("identity_type" -> IdentityType.Phone.code, CxiIdentityId -> "phone_hash"))
                )
            )

            var expected = expectedData.toDS.toDF
            // Need ensure issue of Spark  https://issues.apache.org/jira/browse/SPARK-18484
            // doesn't affect field comparison
            expected.schema.fields.foreach { field =>
                if (field.dataType.isInstanceOf[DecimalType]) {
                    expected = expected
                        .withColumn(field.name, expected(field.name).cast(DecimalType(9, 2)))
                }
            }
            actualData should contain theSameElementsAs expected.collect()
        }
    }

}

object OrderSummaryProcessorTest {

    case class OrdTargetChannelIdTestCase(behavior: String, expectedOrdTargetChannelId: Int)

    case class Item(
        guid: String
    )
    case class Selection(
        guid: String,
        item: Item,
        quantity: Option[Int],
        tax: Option[Double],
        price: Option[Double],
        appliedTaxes: List[AppliedTax]
    )
    case class Discount(guid: String)
    case class AppliedDiscount(
        discountAmount: Option[Double],
        discount: Discount
    )
    case class AppliedTax(
        guid: String
    )
    case class AppliedServiceCharge(
        chargeAmount: Option[Double]
    )
    case class Payment(
        guid: String,
        tipAmount: Option[Double]
    )
    case class Check(
        totalAmount: Option[Double],
        taxAmount: Option[Double],
        selections: List[Selection],
        appliedDiscounts: List[AppliedDiscount],
        appliedServiceCharges: List[AppliedServiceCharge],
        payments: List[Payment]
    )
    case class DiningOption(guid: String)
    case class OrderSummaryTransformInput(
        ord_id: String,
        ord_timestamp: String,
        ord_state: String,
        diningOption: DiningOption,
        location_id: String,
        check: Check
    )

    case class OrderSummaryTransformResult(
        ord_id: String,
        ord_desc: String,
        ord_total: Option[Double],
        ord_date: java.sql.Date,
        ord_timestamp: java.sql.Timestamp,
        discount_amount: Option[Double],
        cxi_partner_id: String,
        location_id: String,
        ord_state_id: Int,
        ord_type: String,
        ord_originate_channel_id: Int,
        ord_target_channel_id: Int,
        item_quantity: Option[Int],
        item_total: Option[Double],
        emp_id: String,
        discount_id: Array[String],
        dsp_qty: String,
        dsp_ttl: String,
        guest_check_line_item_id: String,
        line_id: String,
        taxes_id: Seq[String],
        taxes_amount: Option[Double],
        item_id: String,
        item_price_id: String,
        reason_code_id: String,
        service_charge_id: String,
        service_charge_amount: Option[Double],
        total_taxes_amount: Option[Double],
        total_tip_amount: Option[Double],
        tender_ids: Seq[String],
        ord_pay_total: Option[Double],
        ord_sub_total: Option[Double],
        feed_date: java.sql.Date,
        cxi_identity_ids: List[Map[String, String]]
    )
}
