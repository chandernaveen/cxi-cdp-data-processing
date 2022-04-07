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
        Seq(burgerAffinity, colaAffinity, deliveryPreferred, reachibilityEmail, ageCategory, gender, loyaltyType, timeOfDayEarlyAfternoon).toDS()
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
        val actualSquareLocationsData = actual.collect()
        withClue("POS Square refined categories data do not match") {
            val expected = List(
                (cxiPartnerId1, "L0P0DJ340FXF0", "loc1"),
                (cxiPartnerId1, "L0P0DJ340FXF0", null), // no location name
                (cxiPartnerId2, "ACA0DJ910FPO1", "loc11"),
                (cxiPartnerId2, "PQW0DJ340ALS0", "loc22")
            ).toDF("cxi_partner_id", "location_id", "location_nm").collect()
            actualSquareLocationsData.length should equal(expected.length)
            actualSquareLocationsData should contain theSameElementsAs expected
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
        val signalGenerationDate = LocalDate.of(2022, 2, 23)
        val partnerLocationWeeklySignals = Seq(
            // customer #1
            ("partner1", "locid1", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "at-risk", sqlDate(signalGenerationDate)),
            ("partner1", "locid2", CustomerMetricsTimePeriod.Period30days.value, "cust_id1", "loyalty", "loyalty_type", "loyal", sqlDate(signalGenerationDate)),
            ("partner1", "locid3", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "regular", sqlDate(signalGenerationDate)),
            // all locations for partner 1
            ("partner1", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "time_of_day_metrics", "early_afternoon", "0", sqlDate(signalGenerationDate)),
            ("partner2", "locid22", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "loyalty", "loyalty_type", "new", sqlDate(signalGenerationDate)),
            ("partner2", "locid22", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "time_of_day_metrics", "early_afternoon", "0", sqlDate(signalGenerationDate)),
            // all locations for partner 2
            ("partner2", "_ALL", CustomerMetricsTimePeriod.Period7days.value, "cust_id1", "time_of_day_metrics", "early_afternoon", "0", sqlDate(signalGenerationDate)),
            // customer #2
            ("partner3", "locid333", CustomerMetricsTimePeriod.Period7days.value, "cust_id2", "loyalty", "loyalty_type", "new", sqlDate(signalGenerationDate)),
            // customer #3
            ("partner4", "locid4444", CustomerMetricsTimePeriod.Period7days.value, "cust_id3", "loyalty", "loyalty_type", "new", sqlDate(signalGenerationDate)),
            ("partner5", "locid55555", CustomerMetricsTimePeriod.Period90days.value, "cust_id3", "loyalty", "loyalty_type", "loyal", sqlDate(signalGenerationDate)),
            // excluded signal, not present in signal universe
            ("partner5", "locid55555", CustomerMetricsTimePeriod.Period90days.value, "cust_id3", "some_domain", "some_signal", "loyal", sqlDate(signalGenerationDate)),
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
        val actual = Customer360SignalsJob.processPartnerLocationSignals(spark,
            locationsTable, partnerLocationWeeklySignalsTable, spark.table(signalUniverseTable).as[SignalUniverse].filter(_.is_active), signalGenerationDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))

        // then
        val actualData = actual.collect()
        withClue("Transform data does not match." + actualData.map(_.json).mkString("\nActual data:\n", "\n-----------------------\n", "")) {
            val expectedPartnerLocationSignals = Seq(
                Row("cust_id1",
                    Seq(
                        Row("partner1",
                            "locid1",
                            "Location name 1",
                            Map(
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
                                "loyalty" -> Map("loyalty_type_keyword" -> "regular")
                            ),
                            null,
                            null,
                            null
                        ),
                        Row(
                            "partner1",
                            "_ALL",
                            "_ALL",
                            Map(
                                "time_of_day_metrics" -> Map("early_afternoon_long" -> "0")
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
                                "time_of_day_metrics" -> Map("early_afternoon_long" -> "0")
                            ),
                            null,
                            null,
                            null
                        ),
                        Row(
                            "partner2",
                            "_ALL",
                            "_ALL",
                            Map(
                                "time_of_day_metrics" -> Map("early_afternoon_long" -> "0")
                            ),
                            null,
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
                                "loyalty" -> Map("loyalty_type_keyword" -> "new")
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
            val partnerLocationSignalsOutput = DataTypes.createStructType(Array(
                StructField("customer_360_id", StringType),
                StructField("specific_partner_location_signals", DataTypes.createArrayType(partnerLocationEntrySchema, false), false)
            ))
            import collection.JavaConverters._
            val expected = spark.createDataFrame(expectedPartnerLocationSignals.asJava, partnerLocationSignalsOutput)
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    private def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
    }

    private def sqlDate(date: LocalDate): java.sql.Date = {
        java.sql.Date.valueOf(date)
    }

}
