package com.cxi.cdp.data_processing
package curated_zone.audience

import curated_zone.audience.model.{SignalType, SignalUniverse}
import curated_zone.model.CustomerMetricsTimePeriod
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{DataTypes, StringType, StructField}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class Customer360SignalsJobSpecificPartnerLocationSignalsTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    def signalUniverseDf: Dataset[SignalUniverse] = {
        val signalConfigCreateDate = sqlDate(2022, 1, 1)
        // general customer signals
        val burgerAffinity = SignalUniverse("food_and_drink_preference", "Food and Drink Preference", "burger_affinity", "Burger Affinity",
            "food_and_drink_preference.burger_affinity_boolean", "boolean", "test description", "boolean",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        // another signal in the 'food_and_drink_preference' domain
        val colaAffinity = SignalUniverse("food_and_drink_preference", "Food and Drink Preference", "cola_affinity", "Cola Affinity",
            "food_and_drink_preference.cola_affinity_boolean", "boolean", "test description", "boolean",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        val deliveryPreferred = SignalUniverse("channel_preferences", "Channel Preferences", "delivery_preferred", "Delivery Preferred",
            "channel_preferences.delivery_preferred_boolean", "boolean", "test description", "boolean",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        val reachibilityEmail = SignalUniverse("campaign", "Campaign", "reachability_email", "Reachability - Email",
            "campaign.reachability_email_boolean", "boolean", "test description", "boolean",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        val ageCategory = SignalUniverse("profile", "Profile", "age_category", "Age Category",
            "profile.age_category_keyword", "keyword", "test description", "enumeration",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        // inactive signal, should be ignored
        val gender = SignalUniverse("profile", "Profile", "gender", "Gender",
            "profile.gender_keyword", "keyword", "test description", "enumeration",
            signalConfigCreateDate, signalConfigCreateDate, false, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        // partner and location specific signals
        val loyaltyType = SignalUniverse("loyalty", "Loyalty", "loyalty_type", "Loyalty Type",
            "loyalty.loyalty_type_keyword", "keyword", "test description", "enumeration",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.SpecificPartnerLocationSignal.code, "specific_partner_location_signals")
        val timeOfDayEarlyAfternoon = SignalUniverse("time_of_day_metrics", "Time of Day early afternoon", "early_afternoon", "Time of Day early afternoon",
            "time_of_day_metrics.early_afternoon_long", "long", "test description", "number",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.SpecificPartnerLocationSignal.code, "specific_partner_location_signals")
        val  physicalLaneChannelMetric = SignalUniverse("channel_metrics", "Physical Lane", "physical_lane", "Physical Lane",
            "channel_metrics.physical_lane_long", "long", "test description", "number",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.SpecificPartnerLocationSignal.code, "specific_partner_location_signals")
        val  digitalWebChannelMetric = SignalUniverse("channel_metrics", "Digital Web", "digital_web", "Digital Web",
            "channel_metrics.digital_web_long", "long", "test description", "number",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.SpecificPartnerLocationSignal.code, "specific_partner_location_signals")
        val  totalOrdersOrderMetric = SignalUniverse("order_metrics", "Total orders", "total_orders", "Total orders",
            "order_metrics.total_orders_long", "long", "test description", "number",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.SpecificPartnerLocationSignal.code, "specific_partner_location_signals")
        val  cashTenderTypeMetric = SignalUniverse("tender_type_metrics", "Cash Tender Type", "cash", "Cash Tender Type",
            "tender_type_metrics.cash_long", "long", "test description", "number",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.SpecificPartnerLocationSignal.code, "specific_partner_location_signals")
        Seq(
            // general customer signals
            burgerAffinity, colaAffinity, deliveryPreferred, reachibilityEmail, ageCategory, gender,
            // weekly partner location signals
            loyaltyType,
            // daily partner location signals
            timeOfDayEarlyAfternoon, physicalLaneChannelMetric, digitalWebChannelMetric, totalOrdersOrderMetric, cashTenderTypeMetric
        ).toDS()
    }

    test("test read locations") {
        // given
        import spark.implicits._
        val cxiPartnerId1 = "some-partner-i1"
        val cxiPartnerId2 = "some-partner-id2"
        val locations = List(
            (cxiPartnerId1, "L0P0DJ340FXF0", "PHYSICAL", "ACTIVE", "loc1", null, null, null, null, null, null, null, null, null, null),
            (cxiPartnerId1, "L0P0DJ340FXF0", "some_other_loc_type", "ACTIVE", null, null, null, null, null, null, null, null, null, null, null), // no location name
            (cxiPartnerId2, "ACA0DJ910FPO1", "MOBILE", "INACTIVE", "loc11", null, null, null, null, null, null, null, null, null, null),
            (cxiPartnerId2, "PQW0DJ340ALS0", "some_other_loc_type", "ACTIVE", "loc22", null, null, null, null, null, null, null, null, null, null)
        ).toDF("cxi_partner_id", "location_id", "location_type", "active_flg", "location_nm", "address_1", "zip_code", "lat", "long", "phone", "country_code", "timezone", "currency", "open_dt", "location_website")
        val locationsTable = "tempCustomer360SignalsTable"
        locations.createOrReplaceTempView(locationsTable)

        // when
        val actual = Customer360SignalsJob.readLocations(spark, locationsTable)
        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cxi_partner_id", "location_id", "location_nm")
        }
        val actualLocationsData = actual.collect()
        withClue("Locations data do not match") {
            val expected = List(
                (cxiPartnerId1, "L0P0DJ340FXF0", "loc1"),
                (cxiPartnerId1, "L0P0DJ340FXF0", null), // no location name
                (cxiPartnerId2, "ACA0DJ910FPO1", "loc11"),
                (cxiPartnerId2, "PQW0DJ340ALS0", "loc22")
            ).toDF("cxi_partner_id", "location_id", "location_nm").collect()
            actualLocationsData.length should equal(expected.length)
            actualLocationsData should contain theSameElementsAs expected
        }
    }

    test("test read daily partner location signals") {
        // given
        import spark.implicits._
        val feedDate = LocalDate.of(2022, 2, 24)
        val partnerLocationDailySignalsTable = "tempPartnerLocationDailySignalsTable"
        val partnerLocationDailySignals = Seq(
            // customer #1
            ("partner1", "locid1", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "channel_metrics", "physical_lane", 0L.toString, sqlDate(feedDate)),
            ("partner1", "locid2", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "channel_metrics", "physical_lane", 1L.toString, sqlDate(feedDate)),
            ("partner1", "locid3", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "channel_metrics", "digital_web", 2L.toString, sqlDate(feedDate)),
            // all locations for partner 1
            ("partner2", "locid22", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "order_metrics", "total_orders", 1L.toString, sqlDate(feedDate)),
            ("partner2", "locid22", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "order_metrics", "total_orders", 1L.toString, sqlDate(feedDate)),
            // all locations for partner 2
            ("partner2", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "tender_type_metrics", "cash", 1L.toString, sqlDate(feedDate)),
            ("partner2", "_ALL", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "tender_type_metrics", "cash", 2L.toString, sqlDate(feedDate)),
            // customer #2
            ("partner3", "locid333", CustomerMetricsTimePeriod.Period7days.value, "cust_id2", "time_of_day_metrics", "early_afternoon", 1L.toString, sqlDate(feedDate)),
            // customer #3 doesn't have any daily signals
            // only 1 excluded signal for previous day
            ("partner5", "locid55555", CustomerMetricsTimePeriod.Period90days.value, "cust_id3", "channel_metrics", "physical_lane", 2L.toString, sqlDate(feedDate.minusDays(1))),
        ).toDF("cxi_partner_id", "location_id", "date_option", "customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
        partnerLocationDailySignals.createOrReplaceTempView(partnerLocationDailySignalsTable)

        // when
        val actual = Customer360SignalsJob.readPartnerLocationDailySignals(spark, partnerLocationDailySignalsTable, feedDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cxi_partner_id", "location_id", "date_option", "customer_360_id", "signal_domain", "signal_name", "signal_value")
        }
        val actualLocationsData = actual.collect()
        withClue("Locations data do not match") {
            val expected = Seq(
                // customer #1
                ("partner1", "locid1", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "channel_metrics", "physical_lane", 0L.toString),
                ("partner1", "locid2", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "channel_metrics", "physical_lane", 1L.toString),
                ("partner1", "locid3", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "channel_metrics", "digital_web", 2L.toString),
                // all locations for partner 1
                ("partner2", "locid22", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "order_metrics", "total_orders", 1L.toString),
                ("partner2", "locid22", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "order_metrics", "total_orders", 1L.toString),
                // all locations for partner 2
                ("partner2", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "tender_type_metrics", "cash", 1L.toString),
                ("partner2", "_ALL", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "tender_type_metrics", "cash", 2L.toString),
                // customer #2
                ("partner3", "locid333", CustomerMetricsTimePeriod.Period7days.value, "cust_id2", "time_of_day_metrics", "early_afternoon", 1L.toString),
            ).toDF("cxi_partner_id", "location_id", "date_option", "customer_360_id", "signal_domain", "signal_name", "signal_value").collect()
            actualLocationsData.length should equal(expected.length)
            actualLocationsData should contain theSameElementsAs expected
        }
    }

    test("test transform locations") {
        // given
        import spark.implicits._
        val cxiPartnerId1 = "some-partner-i1"
        val cxiPartnerId2 = "some-partner-id2"
        val readLocations = List(
            (cxiPartnerId1, "L0P0DJ340FXF0", "loc1"),
            (cxiPartnerId1, "L0P0DJ340FXF0", null), // no location name
            (cxiPartnerId2, "ACA0DJ910FPO1", "loc11"),
            (cxiPartnerId2, "PQW0DJ340ALS0", "loc22")
        ).toDF("cxi_partner_id", "location_id", "location_nm")

        // when
        val actual = Customer360SignalsJob.transformLocations(spark, readLocations)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cxi_partner_id", "location_id", "location_nm")
        }
        val actualSquareLocationsData = actual.collect()
        withClue("POS Square refined categories data do not match") {
            val expected = List(
                (cxiPartnerId1, "L0P0DJ340FXF0", "loc1"),
                (cxiPartnerId1, "L0P0DJ340FXF0", "L0P0DJ340FXF0"),
                (cxiPartnerId2, "ACA0DJ910FPO1", "loc11"),
                (cxiPartnerId2, "PQW0DJ340ALS0", "loc22")
            ).toDF("cxi_partner_id", "location_id", "location_nm").collect()
            actualSquareLocationsData.length should equal(expected.length)
            actualSquareLocationsData should contain theSameElementsAs expected
        }
    }

    test("test read partner location weekly signals") {
        // given
        val partnerLocationWeeklySignalsTable = "tempPartnerLocationWeeklySignalsTable"
        val signalGenerationDate = LocalDate.of(2022, 2, 23)
        val signalGenerationDateSql = sqlDate(signalGenerationDate)
        val partnerLocationWeeklySignals = Seq(
            // customer #1
            ("partner1", "loc1", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "at-risk", signalGenerationDateSql),
            ("partner1", "loc1", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "loyalty", "loyalty_type", "loyal", signalGenerationDateSql),
            ("partner1", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "loyal", signalGenerationDateSql),
            ("partner1", "_ALL", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "loyalty", "loyalty_type", "loyal", signalGenerationDateSql),
            ("partner1", "loc2", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "regular", signalGenerationDateSql),
            ("partner2", "loc11", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "new", signalGenerationDateSql),
            ("partner2", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "new", signalGenerationDateSql),
            // customer #2, signal's generation date == feed_date => excluded
            ("partner2", "loc11", CustomerMetricsTimePeriod.Period7days.value, "cust_id2", "loyalty", "loyalty_type", "new", sqlDate(signalGenerationDate.plusDays(1))),
            // customer #3, one signal generation date included
            ("partner1", "loc11", CustomerMetricsTimePeriod.Period7days.value, "cust_id3", "loyalty", "loyalty_type", "new", signalGenerationDateSql),
            ("partner2", "loc11", CustomerMetricsTimePeriod.Period7days.value, "cust_id3", "loyalty", "loyalty_type", "new", sqlDate(signalGenerationDate.minusDays(1))),
        ).toDF("cxi_partner_id", "location_id", "date_option", "customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
        partnerLocationWeeklySignals.createOrReplaceTempView(partnerLocationWeeklySignalsTable)

        // when
        val actual = Customer360SignalsJob.readPartnerLocationWeeklySignals(spark, partnerLocationWeeklySignalsTable, signalGenerationDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))

        // then
        val actualData = actual.collect()
        withClue("Transform data does not match." + actualData.map(_.json).mkString("\nActual data:\n", "\n-----------------------\n", "\n\n")) {
            val expected = Seq(
                // customer #1
                ("partner1", "loc1", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "at-risk"),
                ("partner1", "loc1", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "loyalty", "loyalty_type", "loyal"),
                ("partner1", "loc2", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "regular"),
                ("partner1", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "loyal"),
                ("partner1", "_ALL", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "loyalty", "loyalty_type", "loyal"),
                ("partner2", "loc11", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "new"),
                ("partner2", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "new"),
                // customer #3, one signal generation date included
                ("partner1", "loc11", CustomerMetricsTimePeriod.Period7days.value, "cust_id3", "loyalty", "loyalty_type", "new")
            ).toDF("cxi_partner_id", "location_id", "date_option", "customer_360_id", "signal_domain", "signal_name", "signal_value")
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    test("test process specific partner location customer signals") {
        // given
        val signalUniverseTable = "tempSignalUniverseTable"

        signalUniverseDf.createOrReplaceTempView(signalUniverseTable)

        val partnerLocationWeeklySignalsTable = "tempPartnerLocationWeeklySignalsTable"
        val feedDate = LocalDate.of(2022, 2, 24)
        val weeklySignalGenerationDate = feedDate.minusDays(1)
        val partnerLocationWeeklySignals = Seq(
            // customer #1
            ("partner1", "locid1", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "at-risk", sqlDate(weeklySignalGenerationDate)),
            ("partner1", "locid2", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "loyalty", "loyalty_type", "loyal", sqlDate(weeklySignalGenerationDate)),
            ("partner1", "locid3", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "regular", sqlDate(weeklySignalGenerationDate)),
            // all locations for partner 1
            ("partner2", "locid22", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "new", sqlDate(weeklySignalGenerationDate)),
            // customer #2
            ("partner3", "locid333", CustomerMetricsTimePeriod.Period7days.value, "cust_id2", "loyalty", "loyalty_type", "new", sqlDate(weeklySignalGenerationDate)),
            // customer #3
            ("partner4", "locid4444", CustomerMetricsTimePeriod.Period7days.value, "cust_id3", "loyalty", "loyalty_type", "new", sqlDate(weeklySignalGenerationDate)),
            ("partner5", "locid55555", CustomerMetricsTimePeriod.Period90days.value, "cust_id3", "loyalty", "loyalty_type", "loyal", sqlDate(weeklySignalGenerationDate)),
            // excluded signal, not present in signal universe
            ("partner5", "locid55555", CustomerMetricsTimePeriod.Period90days.value, "cust_id3", "some_domain", "some_signal", "loyal", sqlDate(weeklySignalGenerationDate)),
        ).toDF("cxi_partner_id", "location_id", "date_option", "customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
        partnerLocationWeeklySignals.createOrReplaceTempView(partnerLocationWeeklySignalsTable)

        val locations = List(
            ("partner1", "locid1", "Location name 1"),
            ("partner1", "locid2", "Location name 2"),
            ("partner1", "locid3", "Location name 3"),
            ("partner2", "locid22", "Location name 22"),
            ("partner3", "locid333", "Location name 333"),
            ("partner4", "locid4444", "Location name 4444"),
            ("partner5", "locid55555", "Location name 55555"),
        ).toDF("cxi_partner_id", "location_id", "location_nm")
        val locationsTable = "tempLocationsTable"
        locations.createOrReplaceTempView(locationsTable)

        val partnerLocationDailySignalsTable = "tempPartnerLocationDailySignalsTable"
        val partnerLocationDailySignals = Seq(
            // customer #1
            ("partner1", "locid1", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "channel_metrics", "physical_lane", 0L.toString, sqlDate(feedDate)),
            ("partner1", "locid2", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "channel_metrics", "physical_lane", 1L.toString, sqlDate(feedDate)),
            ("partner1", "locid3", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "channel_metrics", "digital_web", 2L.toString, sqlDate(feedDate)),
            // all locations for partner 1
            ("partner2", "locid22", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "order_metrics", "total_orders", 1L.toString, sqlDate(feedDate)),
            ("partner2", "locid22", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "order_metrics", "total_orders", 1L.toString, sqlDate(feedDate)),
            // all locations for partner 2
            ("partner2", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "tender_type_metrics", "cash", 1L.toString, sqlDate(feedDate)),
            ("partner2", "_ALL", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "tender_type_metrics", "cash", 2L.toString, sqlDate(feedDate)),
            // customer #2
            ("partner3", "locid333", CustomerMetricsTimePeriod.Period7days.value, "cust_id2", "time_of_day_metrics", "early_afternoon", 1L.toString, sqlDate(feedDate)),
            // customer #3 doesn't have any daily signals
            // only 1 excluded signal, not present in signal universe
            ("partner5", "locid55555", CustomerMetricsTimePeriod.Period90days.value, "cust_id3", "some_domain123", "some_signal123", "some_signal_value123", sqlDate(feedDate)),
        ).toDF("cxi_partner_id", "location_id", "date_option", "customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
        partnerLocationDailySignals.createOrReplaceTempView(partnerLocationDailySignalsTable)

        // when
        val actual = Customer360SignalsJob.processPartnerLocationSignals(spark,
            locationsTable, partnerLocationWeeklySignalsTable, partnerLocationDailySignalsTable, spark.table(signalUniverseTable).as[SignalUniverse].filter(_.is_active), feedDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))

        // then
        val actualData = actual.collect()
        val expectedOutput = Seq(
            Row("cust_id1",
                Seq(
                    Row("partner1",
                        "locid1",
                        "Location name 1",
                        Map(
                            "channel_metrics" -> Map("physical_lane_long" -> "0"),
                            "loyalty" -> Map("loyalty_type_keyword" -> "at-risk")
                        ),
                        null,
                        null,
                        null),
                    Row(
                        "partner1",
                        "locid2",
                        "Location name 2",
                        null,
                        Map(
                            "channel_metrics" -> Map("physical_lane_long" -> "1"),
                            "loyalty" -> Map("loyalty_type_keyword" -> "loyal")
                        ),
                        null,
                        null
                    ),
                    Row(
                        "partner1",
                        "locid3",
                        "Location name 3",
                        Map(
                            "channel_metrics" -> Map("digital_web_long" -> "2"),
                            "loyalty" -> Map("loyalty_type_keyword" -> "regular"),
                        ),
                        null,
                        null,
                        null
                    ),
                    Row(
                        "partner2",
                        "locid22",
                        "Location name 22",
                        Map(
                            "loyalty" -> Map("loyalty_type_keyword" -> "new"),
                            "order_metrics" -> Map("total_orders_long" -> "1")
                        ),
                        Map(
                            "order_metrics" -> Map("total_orders_long" -> "1")
                        ),
                        null,
                        null
                    ),
                    Row(
                        "partner2",
                        "_ALL",
                        "_ALL",
                        Map(
                            "tender_type_metrics" -> Map("cash_long" -> "1")
                        ),
                        Map(
                            "tender_type_metrics" -> Map("cash_long" -> "2")
                        ),
                        null,
                        null
                    ))
            ),
            Row("cust_id2",
                Seq(
                    Row(
                        "partner3",
                        "locid333",
                        "Location name 333",
                        Map(
                            "loyalty" -> Map("loyalty_type_keyword" -> "new"),
                            "time_of_day_metrics" -> Map("early_afternoon_long" -> "1")
                        ),
                        null,
                        null,
                        null
                    ))
            ),
            Row("cust_id3",
                Seq(
                    Row(
                        "partner4",
                        "locid4444",
                        "Location name 4444",
                        Map(
                            "loyalty" -> Map("loyalty_type_keyword" -> "new")
                        ),
                        null,
                        null,
                        null
                    ),
                    Row(
                        "partner5",
                        "locid55555",
                        "Location name 55555",
                        null,
                        null,
                        null,
                        Map(
                            "loyalty" -> Map("loyalty_type_keyword" -> "loyal")
                        )
                    ))
            )
        )
        val partnerLocationEntrySchema = DataTypes.createStructType(Array(
            StructField("cxi_partner_id_keyword", StringType),
            StructField("location_id_keyword", StringType),
            StructField("location_name_keyword", StringType),
            StructField("time_period_7", DataTypes.createMapType(StringType, DataTypes.createMapType(StringType, StringType, true)), true),
            StructField("time_period_30", DataTypes.createMapType(StringType, DataTypes.createMapType(StringType, StringType, true)), true),
            StructField("time_period_60", DataTypes.createMapType(StringType, DataTypes.createMapType(StringType, StringType, true)), true),
            StructField("time_period_90", DataTypes.createMapType(StringType, DataTypes.createMapType(StringType, StringType, true)), true)
        ))
        val outputToEsSchema = DataTypes.createStructType(Array(
            StructField("customer_360_id", StringType),
            StructField("specific_partner_location_signals", DataTypes.createArrayType(partnerLocationEntrySchema, false), false)
        ))
        import collection.JavaConverters._
        val expected = spark.createDataFrame(expectedOutput.asJava, outputToEsSchema)
        val expectedData = expected.collect()

        withClue("Transform data does not match." +
            actualData.map(_.json).mkString("\nActual data:\n", "\n-----------------------\n", "\n-----------------------\n") +
            expectedData.map(_.json).mkString("\nExpected data:\n", "\n-----------------------\n", "\n-----------------------\n")
        ) {
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expectedData
        }
    }

    private def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
    }

    private def sqlDate(date: LocalDate): java.sql.Date = {
        java.sql.Date.valueOf(date)
    }

}
