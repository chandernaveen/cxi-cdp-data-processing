package com.cxi.cdp.data_processing
package refined_zone.hub.demographic

import refined_zone.hub.demographic.DemographicIdentityRelationshipsJob.RelationshipType
import refined_zone.hub.identity.model.IdentityType
import support.BaseSparkBatchJobTest
import support.SparkPrettyStringClueFormatter.toPrettyString

import com.cxi.cdp.data_processing.support.crypto_shredding.hashing.Hash
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

import scala.language.postfixOps

class DemographicIdentityRelationshipsJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    test("test read max feed date for raw table (happy path)") {
        // given
        val expectedMaxFeedDate = "2022-02-24"
        val df = Seq(
            ("2022-02-23", "id1"),
            (expectedMaxFeedDate, "id2"),
            (expectedMaxFeedDate, "id3")
        ).toDF("feed_date", "throtle_id")
        val tableName = "tid_email_temp"
        df.createOrReplaceTempView(tableName)

        // when
        val actualMaxFeedDate = DemographicIdentityRelationshipsJob.findMaxFeedDateInRawTable(tableName)(spark)

        // then
        withClue("Max feed date does not match") {
            actualMaxFeedDate should equal(expectedMaxFeedDate)
        }
    }

    test("test read max feed date for raw table (unhappy path)") {
        // given
        val df = Seq.empty[(String, String)].toDF("feed_date", "throtle_id")
        val tableName = "tid_email_temp2"
        df.createOrReplaceTempView(tableName)

        // when
        val caught = intercept[IllegalStateException] {
            DemographicIdentityRelationshipsJob.findMaxFeedDateInRawTable(tableName)(spark)
        }
        caught.getMessage should equal(s"There is no data in raw table $tableName")
    }

    test("test read aaid maid data") {
        // given
        val rawStruct = new StructType()
            .add("throtle_id", StringType)
            .add("throtle_hhid", StringType)
            .add("feed_date", StringType)
            .add("file_name", StringType)
            .add("cxi_id", StringType)
            .add("native_maid", StringType)

        val feedDate = "2022-03-31"
        val rawSourceData = Seq(
            Row("120087649", "314830913", "2022-03-20", "part-00035-4d621c35-8bb5-4f54-a07d-e00f8bf8a440-c000.csv.gz",
                "4da5d46e-114d-408e-95ac-d01dab3330c1", "03d1e62bc6cf4e7b1ffd4aa649eba1c4ba79728160c393309dd1e0982bb45d76"), // diff feed date, filtered out
            Row("120087649", "314830913", feedDate, "part-00035-4d621c35-8bb5-4f54-a07d-e00f8bf8a440-c000.csv.gz",
                "4da5d46e-114d-408e-95ac-d01dab3330c1", "03d1e62bc6cf4e7b1ffd4aa649eba1c4ba79728160c393309dd1e0982bb45d76"),
            Row("25004023191", null, feedDate, "part-00035-4d621c35-8bb5-4f54-a07d-e00f8bf8a440-c000.csv.gz",
                "d2845ac6-628b-428b-a597-767a072d0251", "1aaff06a947d87deac6ebe0de5d28b94c21b355297216001dd14acbb76fdfec8"),
            Row("87737670", "246824989", feedDate, "part-00035-4d621c35-8bb5-4f54-a07d-e00f8bf8a440-c000.csv.gz",
                "a69acbcf-d7f5-4638-940f-3d663c1e75f8", "1cafc88274eb249b19cdffdf4c0a247c0c0e6fcac63d8cf032fbfac52f6f7dac"),
            Row("35092699", "405203644", feedDate, "part-00035-4d621c35-8bb5-4f54-a07d-e00f8bf8a440-c000.csv.gz",
                "4ebe43b0-e92b-4938-bf3a-15d5ed3ec497", "214d817ddf2c119debc89c764a714ff64ba7368fa53a2ebbe803a8aef3288a7b"))

        import collection.JavaConverters._
        val rawSource = spark.createDataFrame(rawSourceData.asJava, rawStruct)
        val tempTableName = "testRawTidAaid"

        rawSource.createOrReplaceTempView(tempTableName)

        // when
        val actual = DemographicIdentityRelationshipsJob.readThrotleTidAaid(tempTableName, feedDate)(spark)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("throtle_id", "native_maid")
        }
        val actualRawData = actual.collect()
        withClue("Raw demographic data source do not match") {
            val expected = List(
                ("120087649", "03d1e62bc6cf4e7b1ffd4aa649eba1c4ba79728160c393309dd1e0982bb45d76"),
                ("25004023191", "1aaff06a947d87deac6ebe0de5d28b94c21b355297216001dd14acbb76fdfec8"),
                ("87737670", "1cafc88274eb249b19cdffdf4c0a247c0c0e6fcac63d8cf032fbfac52f6f7dac"),
                ("35092699", "214d817ddf2c119debc89c764a714ff64ba7368fa53a2ebbe803a8aef3288a7b")
            ).toDF("throtle_id", "native_maid").collect()
            actualRawData.length should equal(expected.length)
            actualRawData should contain theSameElementsAs expected
        }
    }

    test("test read privacy lookup data") {
        // given
        val cxiSource = "cxi-usa-goldbowl"
        val cxiSource2 = "cxi-usa-burgerking"
        val dateRaw = "2022-02-24"
        val df = Seq(
            (cxiSource, sqlDate(dateRaw), "ov_1", "hv_1", IdentityType.Email.code),
            (cxiSource2, sqlDate(dateRaw), "ov_1", "hv_1", IdentityType.Email.code), // duplicate, just diff partner
            (cxiSource, sqlDate(dateRaw), "ov_2", "hv_2", IdentityType.Phone.code)
        ).toDF("cxi_source", "feed_date", "original_value", "hashed_value", "identity_type")
        val lookupTable = "lookupTableTemp"
        df.createOrReplaceTempView(lookupTable)

        // when
        val actualLookupDf = DemographicIdentityRelationshipsJob.readPrivacyLookupTableEmails(lookupTable)(spark)

        // then
        val actualData = actualLookupDf.collect()
        withClue("Read lookup email data does not match" + toPrettyString(actualData)) {
            val expected = Seq(
                ("ov_1", Hash.sha256Hash("ov_1"), "hv_1", IdentityType.Email.code),
            ).toDF( "original_value", "lookup_email", "hashed_value", "identity_type")
            actualLookupDf.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    test("test read email data") {
        // given
        val rawStruct = new StructType()
            .add("throtle_id", StringType)
            .add("throtle_hhid", StringType)
            .add("feed_date", StringType)
            .add("file_name", StringType)
            .add("cxi_id", StringType)
            .add("sha256_lower_email", StringType)

        val feedDate = "2022-03-31"
        val rawSourceData = Seq(
            Row("120087649", "314830913", "2022-03-20", "part-00035-4d621c35-8bb5-4f54-a07d-e00f8bf8a440-c000.csv.gz",
                "4da5d46e-114d-408e-95ac-d01dab3330c1", "03d1e62bc6cf4e7b1ffd4aa649eba1c4ba79728160c393309dd1e0982bb45d76"), // diff feed date, filtered out
            Row("25004023191", null, feedDate, "part-00035-4d621c35-8bb5-4f54-a07d-e00f8bf8a440-c000.csv.gz",
                "d2845ac6-628b-428b-a597-767a072d0251", "5778fdfa1f81d67da22f1f1319350ad7560d1ce55787d687dab5e69777a1509d"),
            Row("35092699", "405203644", feedDate, "part-00035-4d621c35-8bb5-4f54-a07d-e00f8bf8a440-c000.csv.gz",
                "4ebe43b0-e92b-4938-bf3a-15d5ed3ec497", "e02b5a75553bc94932ad2196be1821941f5ef8922b7656f5102ee2aa6c115b50"))

        import collection.JavaConverters._
        val rawSource = spark.createDataFrame(rawSourceData.asJava, rawStruct)
        val tempTableName = "testRawTidEmail"

        rawSource.createOrReplaceTempView(tempTableName)

        // when
        val actual = DemographicIdentityRelationshipsJob.readThrotleTidEmail(tempTableName, feedDate)(spark)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("throtle_id", "sha256_lower_email")
        }
        val actualRawData = actual.collect()
        withClue("Raw demographic email data source do not match") {
            val expected = List(
                ("25004023191", "5778fdfa1f81d67da22f1f1319350ad7560d1ce55787d687dab5e69777a1509d"),
                ("35092699", "e02b5a75553bc94932ad2196be1821941f5ef8922b7656f5102ee2aa6c115b50")
            ).toDF("throtle_id", "sha256_lower_email").collect()
            actualRawData.length should equal(expected.length)
            actualRawData should contain theSameElementsAs expected
        }
    }

    test("test throtle transform maid") {
        // given
        val rawStruct = new StructType()
            .add("throtle_id", StringType)
            .add("native_maid", StringType)

        import collection.JavaConverters._
        val rawIDFA = spark.createDataFrame(Seq(
            Row("120087649", "4da5d46e-114d-408e-95ac-d01dab3330c1"),
            Row("25004023191", "d2845ac6-628b-428b-a597-767a072d0251"),
            Row("25004023191", "d2845ac6-628b-428b-a597-767a072d0251"), // duplicate
            Row("87737670", "a69acbcf-d7f5-4638-940f-3d663c1e75f8"),
            Row("11111111", null), // should be filtered out
            Row("35092699", "4ebe43b0-e92b-4938-bf3a-15d5ed3ec497")).asJava, rawStruct)

        val rawAAID = spark.createDataFrame(Seq(
            Row("650407144", "4da5d46e-114d-408e-95ac-d01dab3330c1"),
            Row("142601417", "d2845ac6-628b-428b-a597-767a072d0251"),
            Row("222222222", null), // should be filtered out
            Row("142601417", "d2845ac6-628b-428b-a597-767a072d0251"), // duplicate
            Row("28606737786", "a69acbcf-d7f5-4638-940f-3d663c1e75f8")).asJava, rawStruct)

        // when
        val actual = DemographicIdentityRelationshipsJob.transformThrotleMaids(rawIDFA, rawAAID)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("throtle_id", "native_maid", "maid_identity_type")
        }
        val actualTransformData = actual.collect()
        withClue("Transform demographic maid data do not match") {
            val expected = List(
                ("120087649", "4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidIDFA.code),
                ("25004023191", "d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidIDFA.code),
                ("87737670", "a69acbcf-d7f5-4638-940f-3d663c1e75f8", IdentityType.MaidIDFA.code),
                ("35092699", "4ebe43b0-e92b-4938-bf3a-15d5ed3ec497", IdentityType.MaidIDFA.code),

                ("650407144", "4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidAAID.code),
                ("142601417", "d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidAAID.code),
                ("28606737786", "a69acbcf-d7f5-4638-940f-3d663c1e75f8", IdentityType.MaidAAID.code)
            ).toDF("throtle_id", "native_maid", "maid_identity_type").collect()
            actualTransformData.length should equal(expected.length)
            actualTransformData should contain theSameElementsAs expected
        }
    }

    test("test throtle transform email") {
        // given
        val rawStructEmail = new StructType()
            .add("throtle_id", StringType)
            .add("sha256_lower_email", StringType)

        val rawStructPrivacy = new StructType()
            .add("country", StringType)
            .add("cxi_source", StringType)
            .add("original_value", StringType)
            .add("hashed_value", StringType)
            .add("process_name", StringType)
            .add("feed_date", StringType)
            .add("lookup_email", StringType)

        import collection.JavaConverters._
        val rawEmail = spark.createDataFrame(
            Seq(
                Row("565225184", "5778fdfa1f81d67da22f1f1319350ad7560d1ce55787d687dab5e69777a1509d"),
                Row("187407107", "7c53a3ac6e326f3b1570fded173d611ac1f1fb54249c5bc32728f5545401d7dd"),
                Row("187407107", "7c53a3ac6e326f3b1570fded173d611ac1f1fb54249c5bc32728f5545401d7dd"), // duplicate, filtered out
                Row("333333333", null), // should be filtered out
                Row("119748951", "acb2d85813d85ac0689f9d38b977f4197eb93c4a78ccf5b5224ead03f61f5b98"),
                Row("13227693185", "e02b5a75553bc94932ad2196be1821941f5ef8922b7656f5102ee2aa6c115b50"),
                Row("8500614449", null), // filtered_out, no such hash in privacy lookup
                Row("8500614449", "some_hash"), // filtered_out, no such hash in privacy lookup
            ).asJava, rawStructEmail)

        val rawPrivacy = spark.createDataFrame(
            Seq(
                Row("USA", "cxi-usa-goldbowl", "vfalosito@gmail.com", "001c553ca1b16d9fd1bc089b8c94efa57dbc67f9dc7f406e066786a999fd0b0c", "json-crypto-hash", "2022-01-27T15:41:59.149+0000", "5778fdfa1f81d67da22f1f1319350ad7560d1ce55787d687dab5e69777a1509d"),
                Row("USA", "cxi-usa-goldbowl", "mbateman5@gmail.com", "00bd58ab420d6ef94bb0cb8702b31f560653b3f81d1227d59eb894b066a9d653", "json-crypto-hash", "2022-01-27T15:41:59.149+0000", "acb2d85813d85ac0689f9d38b977f4197eb93c4a78ccf5b5224ead03f61f5b98"),
                Row("USA", "cxi-usa-goldbowl", "dan_lopez23@yahoo.com", "01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7", "json-crypto-hash", "2022-01-27T15:54:06.946+0000", "e02b5a75553bc94932ad2196be1821941f5ef8922b7656f5102ee2aa6c115b50")
            ).asJava, rawStructPrivacy)

        // when
        val actual = DemographicIdentityRelationshipsJob.transformThrotleEmail(rawEmail, rawPrivacy)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("throtle_id", "hash_salted_email", "email_metadata")
        }
        val actualTransformData = actual.collect()
        withClue("Transform demographic data source do not match" + toPrettyString(actualTransformData)) {
            val expected = List(
                ("565225184", "001c553ca1b16d9fd1bc089b8c94efa57dbc67f9dc7f406e066786a999fd0b0c", Map("email_domain" -> "gmail.com")),
                ("119748951", "00bd58ab420d6ef94bb0cb8702b31f560653b3f81d1227d59eb894b066a9d653", Map("email_domain" -> "gmail.com")),
                ("13227693185", "01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7", Map("email_domain" -> "yahoo.com"))
            ).toDF("throtle_id", "hash_salted_email", "email_metadata").collect()
            actualTransformData.length should equal(expected.length)
            actualTransformData should contain theSameElementsAs expected
        }
    }

    test("test transform demographic identity") {
        // given
        val transformedThrotleMaids = List(
            ("120087649", "4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidIDFA.code),
            ("25004023191", "d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidIDFA.code),

            ("650407144", "4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidAAID.code),
            ("142601417", "d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidAAID.code),
        ).toDF("throtle_id", "native_maid", "maid_identity_type")

        val transformedThrotleEmail = List(
            ("565225184", "001c553ca1b16d9fd1bc089b8c94efa57dbc67f9dc7f406e066786a999fd0b0c", Map("email_domain" -> "gmail.com")),
            ("13227693185", "01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7", Map("email_domain" -> "yahoo.com"))
        ).toDF("throtle_id", "hash_salted_email", "email_metadata")

        // when
        val actual = DemographicIdentityRelationshipsJob.transformDemographicIdentity(transformedThrotleMaids, transformedThrotleEmail)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cxi_identity_id", "type", "metadata")
        }
        val actualTransformData = actual.collect()
        withClue("Transform demographic data source do not match" + toPrettyString(actualTransformData)) {
            val expected = List(
                ("120087649", IdentityType.ThrotleId.code, Map.empty[String, String]),
                ("4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidIDFA.code, Map.empty[String, String]),
                ("25004023191", IdentityType.ThrotleId.code, Map.empty[String, String]),
                ("d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidIDFA.code, Map.empty[String, String]),

                ("650407144", IdentityType.ThrotleId.code, Map.empty[String, String]),
                ("4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidAAID.code, Map.empty[String, String]),
                ("142601417", IdentityType.ThrotleId.code, Map.empty[String, String]),
                ("d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidAAID.code, Map.empty[String, String]),

                ("565225184", IdentityType.ThrotleId.code, Map.empty[String, String]),
                ("001c553ca1b16d9fd1bc089b8c94efa57dbc67f9dc7f406e066786a999fd0b0c", IdentityType.Email.code, Map("email_domain" -> "gmail.com")),
                ("13227693185", IdentityType.ThrotleId.code, Map.empty[String, String]),
                ("01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7", IdentityType.Email.code, Map("email_domain" -> "yahoo.com"))
            ).toDF("cxi_identity_id", "type", "metadata").collect()
            actualTransformData.length should equal(expected.length)
            actualTransformData should contain theSameElementsAs expected
        }
    }

    test("test transform demographic identity relationship") {
        // given
        val runDate = "2022-02-24"
        val transformedThrotleMaids = List(
            ("120087649", "4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidIDFA.code),
            ("25004023191", "d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidIDFA.code),

            ("650407144", "4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidAAID.code),
            ("142601417", "d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidAAID.code),
        ).toDF("throtle_id", "native_maid", "maid_identity_type")

        val transformedThrotleEmail = List(
            ("565225184", "001c553ca1b16d9fd1bc089b8c94efa57dbc67f9dc7f406e066786a999fd0b0c", Map("email_domain" -> "gmail.com")),
            ("13227693185", "01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7", Map("email_domain" -> "yahoo.com"))
        ).toDF("throtle_id", "hash_salted_email", "email_metadata")

        // when
        val actual = DemographicIdentityRelationshipsJob.transformDemographicIdentityRelationship(transformedThrotleMaids, transformedThrotleEmail, runDate)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("source", "source_type", "target", "target_type", "relationship", "frequency", "created_date", "last_seen_date", "active_flag")
        }
        val actualTransformData = actual.collect()
        withClue("Transform demographic data source do not match" + toPrettyString(actualTransformData)) {
            val expected = List(
                ("120087649", IdentityType.ThrotleId.code, "4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidIDFA.code, RelationshipType, 1, runDate, runDate, true),
                ("25004023191", IdentityType.ThrotleId.code, "d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidIDFA.code, RelationshipType, 1, runDate, runDate, true),
                ("650407144", IdentityType.ThrotleId.code, "4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidAAID.code, RelationshipType, 1, runDate, runDate, true),
                ("142601417", IdentityType.ThrotleId.code, "d2845ac6-628b-428b-a597-767a072d0251", IdentityType.MaidAAID.code, RelationshipType, 1, runDate, runDate, true),
                ("565225184", IdentityType.ThrotleId.code, "001c553ca1b16d9fd1bc089b8c94efa57dbc67f9dc7f406e066786a999fd0b0c", IdentityType.Email.code, RelationshipType, 1, runDate, runDate, true),
                ("13227693185", IdentityType.ThrotleId.code, "01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7", IdentityType.Email.code, RelationshipType, 1, runDate, runDate, true)
            ).toDF("source", "source_type", "target", "target_type", "relationship", "frequency", "created_date", "last_seen_date", "active_flag").collect()
            actualTransformData.length should equal(expected.length)
            actualTransformData should contain theSameElementsAs expected
        }
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }
}
