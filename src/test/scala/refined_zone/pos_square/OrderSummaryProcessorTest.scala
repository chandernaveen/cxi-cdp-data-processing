package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, PickupDetails}
import refined_zone.hub.model.ChannelType
import support.BaseSparkBatchJobTest

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.scalatest.Matchers

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.Random

class OrderSummaryProcessorTest extends BaseSparkBatchJobTest with Matchers {
    private val logger = Logger.getLogger(classOf[OrderSummaryProcessorTest].getName)

    import OrderSummaryProcessorTest._

    test("getOrdTargetChannelId") {
        import Fulfillment.{State, Type}
        import spark.implicits._

        val protoFulfillment = Fulfillment(pickup_details = PickupDetails(note = "a note"), state = "FL")
        val unknownType = "some unknown type"

        val inputDF = List[OrdTargetChannelIdTestCase](
            OrdTargetChannelIdTestCase(fulfillments = null, expectedOrdTargetChannelId = ChannelType.Unknown.code),
            OrdTargetChannelIdTestCase(fulfillments = Seq(), expectedOrdTargetChannelId = ChannelType.Unknown.code),
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(protoFulfillment.copy(`type` = Type.Pickup)),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code
            ),

            // prefer COMPLETED fulfillment
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = unknownType, state = State.Proposed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Completed)
                ),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code
            ),

            // COMPLETED fulfillment has null type, so ignore it
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = null, state = State.Completed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Proposed)
                ),
                expectedOrdTargetChannelId = ChannelType.PhysicalPickup.code
            ),
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(
                    protoFulfillment.copy(`type` = unknownType, state = State.Completed),
                    protoFulfillment.copy(`type` = Type.Pickup, state = State.Proposed)
                ),
                expectedOrdTargetChannelId = ChannelType.Unknown.code
            ),
            OrdTargetChannelIdTestCase(
                fulfillments = Seq(protoFulfillment.copy(`type` = unknownType)),
                expectedOrdTargetChannelId = ChannelType.Unknown.code
            )
        ).toDS

        val results = inputDF
            .withColumn("actualOrdTargetChannelId", OrderSummaryProcessor.getOrdTargetChannelId(col("fulfillments")))
            .collect

        results.foreach { result =>
            val fulfillments = result.getAs[Seq[Fulfillment]]("fulfillments")
            val expectedOrdTargetChannelId = result.getAs[Int]("expectedOrdTargetChannelId")
            val actualOrdTargetChannelId = result.getAs[Int]("actualOrdTargetChannelId")
            withClue(s"actual channel type id does not match for fulfillments: $fulfillments") {
                actualOrdTargetChannelId shouldBe expectedOrdTargetChannelId
            }
        }
    }
    test("testRead") {
        import spark.implicits._
        val baseDate = Instant.now()
        val defaultTimeZone = ZoneId.systemDefault()

        val dtFormatShort = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(defaultTimeZone)
        val dtFormatLong = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'").withZone(defaultTimeZone)
        val dateNow = dtFormatShort.format(baseDate)

        val past15sec = dtFormatLong.format(baseDate.minusSeconds(15))
        val past20sec = dtFormatLong.format(baseDate.minusSeconds(20))

        val orderId1 = "dummyStr1"
        val orderId2 = "dummyStr2"

        val tableName = "raw_orders"
        val rawData = List[OrderSummaryFromRaw](
            // case for normal flow
            new OrderSummaryFromRaw(id = orderId1, opened_at = past15sec, closed_at = past20sec, 42),
            // case for BUG check
            new OrderSummaryFromRaw(id = orderId2, opened_at = past20sec, closed_at = null, tender_id = 17)
        )

        implicit val formats: DefaultFormats = DefaultFormats
        val listData = rawData.map(raw =>
            RawDataEmulator(
                dateNow,
                Serialization.write(raw)
            )
        )

        val inputDF = listData.toDF

        // GlobalTempView used since it allows place data
        // to context where SQL works as "SELECT * FROM global_temp.tableName"
        inputDF.createOrReplaceGlobalTempView(tableName)

        val orderSummary = OrderSummaryProcessor.readOrderSummary(spark, dateNow, "global_temp", tableName)

        // Helper map to distinguish fields with Money
        val moneyFieldsMap = Map[String, String](
            "total_tax_money" -> "total_taxes_amount",
            "total_discount_money" -> "discount_amount",
            "total_tip_money" -> "total_tip_amount",
            "total_money" -> "ord_total",
            "total_service_charge_money" -> "service_charge_amount"
        )
        // Helper map to deal with fields were renamed by `read`
        val renamedFieldsMap = Map[String, String](
            "id" -> "ord_id",
            "state" -> "ord_state",
            "tenders" -> "tender_array"
        )
        orderSummary.collect
            .zip(rawData)
            .foreach { case (result, origin) =>
                ccToMap(origin)
                    .foreach {
                        case (k, v) if result.schema.fieldNames.contains(k) =>
                            val a = result.getAs[AnyVal](k)
                            logger.debug(s"test '$k': $a <vs> $v")
                            withClue(s"Auto field comparison: $k") {
                                a shouldBe v
                            }
                        case (k, v) if renamedFieldsMap.keySet.contains(k) =>
                            // some fields are renamed
                            withClue(s"Auto field comparison: $k") {
                                result.getAs[String](renamedFieldsMap.apply(k)) shouldBe v.toString
                            }
                        case (k, v) if k == "closed_at" =>
                            // closed_at used multiple times in other fields
                            withClue(s"closed_at equality to ord_date") {
                                result.getAs[String]("ord_date") shouldBe v
                            }
                            withClue(s"ord_timestamp equality to ord_date") {
                                result.getAs[String]("ord_timestamp") shouldBe v
                            }
                        case (k, Money(amount)) if moneyFieldsMap.keySet.contains(k) => {
                            // check all money related fields
                            withClue(s"Money field comparison error") {
                                result.getAs[String](moneyFieldsMap.apply(k)) shouldBe amount.toString
                            }
                        }
                        case (k, Discount(uid)) => {
                            withClue(s"Field comparison discount_id") {
                                result.getAs[String]("discount_id") shouldBe uid
                            }
                        }
                        case other =>
                            logger.warn(s"Don't know how to test select-field $other")
                    }
            }
    }

    test("testTransform") {
        import spark.implicits._
        val baseDate = Instant.now()
        val defaultTimeZone = ZoneId.systemDefault()

        val dtFormatShort = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(defaultTimeZone)
        val dtFormatLong = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'").withZone(defaultTimeZone)
        val dateNow = dtFormatShort.format(baseDate)

        val past15sec = dtFormatLong.format(baseDate.minusSeconds(15))
        val past20sec = dtFormatLong.format(baseDate.minusSeconds(20))

        val orderId1 = "dummyStr1"
        val orderId2 = "dummyStr2"
        val testPartnerId = "AABBCC"
        val identityIndex = List[IdentityTestData](IdentityTestData(orderId1), IdentityTestData(orderId2)).toDF
        val tableName = "raw_orders"
        val tendersId = 31;
        val rawData = List[OrderSummaryFromRaw](
            // case for normal flow
            new OrderSummaryFromRaw(id = orderId1, opened_at = past15sec, closed_at = past20sec, tender_id = tendersId),
            // case for BUG check
            new OrderSummaryFromRaw(id = orderId2, opened_at = past20sec, closed_at = null, tender_id = tendersId)
        )

        implicit val formats: DefaultFormats = DefaultFormats
        val listData = rawData.map(raw =>
            RawDataEmulator(
                dateNow,
                Serialization.write(raw)
            )
        )

        val inputDF = listData.toDF

        // GlobalTempView used since it allows place data
        // to context where SQL works as "SELECT * FROM global_temp.tableName"
        inputDF.createOrReplaceGlobalTempView(tableName)

        val orderSummary = OrderSummaryProcessor.readOrderSummary(spark, dateNow, "global_temp", tableName)

        // Helper map to distinguish fields with Money
        val moneyFieldsMap = Map[String, String](
            "total_tax_money" -> "total_taxes_amount",
            "total_discount_money" -> "discount_amount",
            "total_tip_money" -> "total_tip_amount",
            "total_money" -> "ord_total",
            "total_service_charge_money" -> "service_charge_amount"
        )
        // Helper map to deal with fields were renamed by `read`
        val renamedFieldsMap = Map[String, String](
            "id" -> "ord_id",
            "state" -> "ord_state"
        )
        val processedOrderSummary =
            OrderSummaryProcessor.transformOrderSummary(orderSummary, dateNow, testPartnerId, identityIndex)

        processedOrderSummary.collect
            .zip(rawData)
            .foreach { case (result, origin) =>
                ccToMap(origin)
                    // .filter{ case (k, v) => result.schema.fieldNames.contains(k) }
                    .foreach {
                        case (k, v) if result.schema.fieldNames.contains(k) => {
                            val a = result.getAs[AnyVal](k)
                            logger.debug(s"test:$k: $a <vs> $v")
                            withClue(s"Auto field comparison: $k") {
                                a shouldBe v
                            }
                        }
                        case (k, v) if (k == "tenders") => {
                            val target = result.getAs[Seq[String]]("tender_ids")
                            withClue(s"tenders_ids comparison") {
                                target should contain(tendersId.toString)
                            }
                        }
                        case (k, v) if renamedFieldsMap.keySet.contains(k) => {
                            // some fields are renamed
                            withClue(s"Auto field comparison: $k") {
                                result.getAs[String](renamedFieldsMap.apply(k)) shouldBe v.toString
                            }
                        }
                        case (k, v) if k == "closed_at" => { // closed_at used multiple times in other fields
                            // per bug:2842 null on `closed_at` must be treated as `opened_at`
                            val compareWith = if (v == null) origin.opened_at else v
                            withClue(s"closed_at equality to ord_date") {
                                result.getAs[String]("ord_date") shouldBe compareWith
                            }
                            withClue(s"ord_timestamp equality to ord_date") {
                                result.getAs[String]("ord_timestamp") shouldBe compareWith
                            }
                        }
                        case (k, Money(amount)) if moneyFieldsMap.keySet.contains(k) => {
                            // check all money related fields
                            withClue(s"Money field comparison error") {
                                (result
                                    .getAs[java.math.BigDecimal](moneyFieldsMap.apply(k))
                                    .doubleValue() - amount / 100.0) shouldBe <(0.01)
                            }
                        }
                        case (k, Discount(uid)) => {
                            withClue(s"Field comparison discount_id") {
                                result.getAs[String]("discount_id") shouldBe uid
                            }
                        }
                        case (k, _) if List("fulfillments", "line_items", "customer_id", "opened_at").contains(k) => {
                            logger.debug(s"Omitting field check for $k")
                        }
                        case other =>
                            logger.warn(s"Don't know how to test select-field $other")
                    }
            }
    }

}

object OrderSummaryProcessorTest {
    val _rnd = new Random()

    /** Helper mapper convert case class to pair with field name
      * @param cc case class to create map
      * @return Map of pair field_name -> value
      */
    def ccToMap(cc: Any): Map[String, AnyRef] = {

        cc.getClass.getDeclaredFields
            .map(f => {
                f.setAccessible(true)
                f.getName -> f.get(cc)
            })
            .toMap
    }

    case class OrdTargetChannelIdTestCase(fulfillments: Seq[Fulfillment], expectedOrdTargetChannelId: Int)

    case class Money(amount: Int)

    case class Discount(uid: String)

    case class OrderSummaryFromRaw(
        id: String,
        opened_at: String,
        closed_at: String,
        total_money: Money = Money(amount = _rnd.nextInt(1000) + 1),
        fulfillments: String = "[{'pickup_details':{'note':'abc'}, 'state':'FL'}]",
        line_items: String = "[{'catalog_object_id':1, 'quantity':2}]",
        total_service_charge_money: Money = Money(amount = _rnd.nextInt(1000) + 1),
        total_tax_money: Money = Money(10),
        total_tip_money: Money = Money(5),
        total_discount_money: Money = Money(1),
        customer_id: String = _rnd.nextString(12),
        tenders: String,
        location_id: String = "dsdsd",
        state: String = "COMPLETED",
        discounts: Discount = Discount(uid = _rnd.nextString(5))
    ) {
        def this(id: String, opened_at: String, closed_at: String, tender_id: Int) = this(
            id = id,
            opened_at = opened_at,
            closed_at = closed_at,
            tenders = s"{'id':$tender_id}"
        )
    }

    case class RawDataEmulator(feed_date: String, record_value: String, record_type: String = "orders")

    case class IdentityTestData(ord_id: String)
}
