package com.cxi.cdp.data_processing
package curated_zone.audience

import curated_zone.audience.model.{SignalType, SignalUniverse}
import support.BaseSparkBatchJobTest

import com.cxi.cdp.data_processing.curated_zone.audience.Customer360SignalsJob.DatalakeTablesConfig
import com.cxi.cdp.data_processing.curated_zone.model.CustomerMetricsTimePeriod
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.{DataTypes, StringType, StructField}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class Customer360SignalsJobCommonTest extends BaseSparkBatchJobTest {

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

    test("test read signal universe") {
        // given
        val signalConfigCreateDate = sqlDate(2022, 1, 1)
        val ageCategory = SignalUniverse("profile", "Profile", "age_category", "Age Category",
            "profile.age_category_keyword", "keyword", "test description", "enumeration",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signal")
        // inactive signal, should be ignored
        val gender = SignalUniverse("profile", "Profile", "gender", "Gender",
            "profile.gender_keyword", "keyword", "test description", "enumeration",
            signalConfigCreateDate, signalConfigCreateDate, false, true, SignalType.GeneralCustomerSignal.code, "general_customer_signal")
        val signalUniverseDf = Seq(ageCategory, gender).toDS()

        val signalUniverseTable = "tempSignalUniverseTable"
        signalUniverseDf.createOrReplaceTempView(signalUniverseTable)

        // when
        val actual = Customer360SignalsJob.readSignalUniverse(spark, signalUniverseTable)

        // then
        val actualData = actual.collect()
        withClue("Transform data does not match." + actualData.mkString("\nActual data:\n","\n-----------------------\n", "\n\n")) {
            val expected = Seq(ageCategory).toDS()
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    test("test read customer360") {
        // given
        val customer360Table = "tempCustomer360Table"
        val customer360createDate = sqlDate(2022, 2, 1)
        val customer360Df = Seq(
            ("cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), customer360createDate, customer360createDate, true),
            ("cust_id2", Map("email" -> Array("test@domain.com")), customer360createDate, customer360createDate, true),
            // inactive customer, should be ignored
            ("cust_id3", Map("email" -> Array("test@domain.com")), customer360createDate, customer360createDate, false),
        ).toDF("customer_360_id", "identities", "create_date", "update_date", "active_flag")
        customer360Df.createOrReplaceTempView(customer360Table)

        // when
        val actual = Customer360SignalsJob.readCustomer360(spark, customer360Table)

        // then
        val actualData = actual.collect()
        withClue("Transform data does not match." + actualData.mkString("\nActual data:\n","\n-----------------------\n", "\n\n")) {
            val expected = Seq(
                ("cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2"))),
                ("cust_id2", Map("email" -> Array("test@domain.com")))
            ).toDF("customer_360_id", "identities")
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    test("test process of customer & partnerlocation specific signals") {
        // given
        val signalUniverseTable = "tempSignalUniverseTable"

        signalUniverseDf.filter(_.is_active).createOrReplaceTempView(signalUniverseTable)

        val customer360Table = "tempCustomer360Table"
        val customer360createDate = sqlDate(2022, 2, 1)
        val customer360Df = Seq(
            ("cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), customer360createDate, customer360createDate, true),
            ("cust_id2", Map("email" -> Array("test@domain.com")), customer360createDate, customer360createDate, true),
            // inactive customer, should be ignored
            ("cust_id3", Map("email" -> Array("test@domain.com")), customer360createDate, customer360createDate, false),
        ).toDF("customer_360_id", "identities", "create_date", "update_date", "active_flag")
        customer360Df.createOrReplaceTempView(customer360Table)

        val customer360SignalsTable = "tempCustomer360SignalsTable"
        val feedDate = LocalDate.of(2022, 2, 25)
        val signalGenerationDate = feedDate.minusDays(1)
        val genericSignalGenerationDateSql = sqlDate(feedDate)
        val customer360SignalsDf = Seq(
            // customer #1
            ("cust_id1", "campaign", "reachability_email", "true", genericSignalGenerationDateSql),
            ("cust_id1", "food_and_drink_preference", "burger_affinity", "true", genericSignalGenerationDateSql),
            ("cust_id1", "food_and_drink_preference", "cola_affinity", "true", genericSignalGenerationDateSql),
            ("cust_id1", "channel_preferences", "delivery_preferred", "true", genericSignalGenerationDateSql),
            ("cust_id1", "profile", "age_category", "25-30", genericSignalGenerationDateSql),
            // signal doesn't exist in the signal universe table, should be ignored
            ("cust_id1", "some_domain", "some_signal", "true", genericSignalGenerationDateSql),
            // inactive signal, should be ignored
            ("cust_id1", "profile", "gender", "male", genericSignalGenerationDateSql),

            // customer #2
            ("cust_id2", "food_and_drink_preference", "burger_affinity", "true", genericSignalGenerationDateSql),
            // signal's generation date is different, should be ignored
            ("cust_id2", "channel_preferences", "delivery_preferred", "true", sqlDate(2021, 12, 10)),
            // customer #3, will be ignored
            ("cust_id3", "food_and_drink_preference", "burger_affinity", "true", genericSignalGenerationDateSql)
        ).toDF("customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
        customer360SignalsDf.createOrReplaceTempView(customer360SignalsTable)

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

        val loyaltyTypeLatestDate = sqlDate(signalGenerationDate)
        val partnerLocationWeeklySignalsTable = "tempPartnerLocationWeeklySignalsTable"
        val partnerLocationWeeklySignals = Seq(
            // customer #1
            ("partner1", "locid1", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "at-risk", loyaltyTypeLatestDate),
            ("partner1", "locid2", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "loyalty", "loyalty_type", "loyal", loyaltyTypeLatestDate),
            ("partner1", "locid3", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "regular", loyaltyTypeLatestDate),
            // all locations for partner 1
            ("partner2", "locid22", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "new", loyaltyTypeLatestDate),
            // customer #2
            ("partner3", "locid333", CustomerMetricsTimePeriod.Period7days.value, "cust_id2", "loyalty", "loyalty_type", "new", loyaltyTypeLatestDate),
            // customer #3
            ("partner4", "locid4444", CustomerMetricsTimePeriod.Period7days.value, "cust_id3", "loyalty", "loyalty_type", "new", loyaltyTypeLatestDate),
            ("partner5", "locid55555", CustomerMetricsTimePeriod.Period90days.value, "cust_id3", "loyalty", "loyalty_type", "loyal", loyaltyTypeLatestDate),
            // excluded signal, not present in signal universe
            ("partner5", "locid55555", CustomerMetricsTimePeriod.Period90days.value, "cust_id3", "some_domain", "some_signal", "loyal", sqlDate(signalGenerationDate.minusDays(2))),
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

        // when
        val actual = Customer360SignalsJob.process(spark,
            DatalakeTablesConfig(signalUniverseTable, customer360Table, customer360SignalsTable, partnerLocationWeeklySignalsTable, partnerLocationDailySignalsTable, locationsTable), feedDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))

        // then
        val actualData = actual.collect()

        val expectedOutput = Seq(
            Row("cust_id1",
                Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")),
                Map(
                    "campaign" ->
                        Map("reachability_email_boolean" -> "true"),
                    "channel_preferences" ->
                        Map("delivery_preferred_boolean" -> "true"),
                    "food_and_drink_preference" ->
                        Map("burger_affinity_boolean" -> "true", "cola_affinity_boolean" -> "true"),
                    "profile" ->
                        Map("age_category_keyword" -> "25-30")),
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
                Map("email" -> Array("test@domain.com")),
                Map("food_and_drink_preference" ->
                    Map("burger_affinity_boolean" -> "true")),
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
            StructField("customer_identities", DataTypes.createMapType(StringType, DataTypes.createArrayType(StringType, true), true), true),
            StructField("general_customer_signals", DataTypes.createMapType(StringType, DataTypes.createMapType(StringType, StringType, true), true), true),
            StructField("specific_partner_location_signals", DataTypes.createArrayType(partnerLocationEntrySchema, false), true)
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
