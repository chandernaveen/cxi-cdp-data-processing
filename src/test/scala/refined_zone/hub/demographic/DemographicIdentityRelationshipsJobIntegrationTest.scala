package com.cxi.cdp.data_processing
package refined_zone.hub.demographic

import refined_zone.hub.demographic.DemographicIdentityRelationshipsJob.RelationshipType
import refined_zone.hub.identity.model.IdentityType
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import scala.language.postfixOps

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class DemographicIdentityRelationshipsJobIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {
    val demographicIdentityTempTable = "demographic_identity_temp"
    val demographicIdentityRelationshipTempTable = generateUniqueTableName("demographic_identity_relationship_temp")

    import spark.implicits._

    test("write final Demographic Identity") {
        // given
        val transformedDemographicIdentities = List(
            ("120087649", IdentityType.ThrotleId.code, Map.empty[String, String]),
            ("4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidIDFA.code, Map.empty[String, String])
        ).toDF("cxi_identity_id", "type", "metadata")

        // when
        DemographicIdentityRelationshipsJob.writeDemographicIdentity(
            transformedDemographicIdentities,
            demographicIdentityTempTable
        )

        // then
        withClue("Data from write demographic identity function does not match") {
            val actual_1 = spark
                .table(demographicIdentityTempTable)
                .select("cxi_identity_id", "type", "metadata")
            val expected_1 = transformedDemographicIdentities
            actual_1.schema.fields.map(_.name) shouldEqual expected_1.schema.fields.map(_.name)
            actual_1.collect() should contain theSameElementsAs expected_1.collect()
        }

        // given
        val transformedDemographicIdentities2 = List(
            ("120087649", IdentityType.ThrotleId.code, Map.empty[String, String]), // duplicate, not inserted
            (
                "4da5d46e-114d-408e-95ac-d01dab3330c1",
                IdentityType.MaidAAID.code,
                Map.empty[String, String]
            ) // diff identity type, inserted
        ).toDF("cxi_identity_id", "type", "metadata")

        // when
        DemographicIdentityRelationshipsJob.writeDemographicIdentity(
            transformedDemographicIdentities2,
            demographicIdentityTempTable
        )

        // then
        withClue("Written demographic identity data do not match with expected data") {
            val actual_2 = spark
                .table(demographicIdentityTempTable)
                .select("cxi_identity_id", "type", "metadata")
            val expected_2 = transformedDemographicIdentities
                .unionByName(
                    List(
                        ("4da5d46e-114d-408e-95ac-d01dab3330c1", IdentityType.MaidAAID.code, Map.empty[String, String])
                    )
                        .toDF("cxi_identity_id", "type", "metadata")
                )
            actual_2.schema.fields.map(_.name) shouldEqual expected_2.schema.fields.map(_.name)
            actual_2.collect() should contain theSameElementsAs expected_2.collect()
        }
    }

    test("write Demographic Identity Relationship") {
        // given
        val runDate = "2022-02-24"
        val transformedDemographicIdentityRelationship = List(
            (
                "120087649",
                IdentityType.ThrotleId.code,
                "4da5d46e-114d-408e-95ac-d01dab3330c1",
                IdentityType.MaidIDFA.code,
                RelationshipType,
                1,
                runDate,
                runDate,
                true
            ),
            (
                "650407144",
                IdentityType.ThrotleId.code,
                "4da5d46e-114d-408e-95ac-d01dab3330c1",
                IdentityType.MaidAAID.code,
                RelationshipType,
                1,
                runDate,
                runDate,
                true
            ),
            (
                "13227693185",
                IdentityType.ThrotleId.code,
                "01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7",
                IdentityType.Email.code,
                RelationshipType,
                1,
                runDate,
                runDate,
                true
            )
        ).toDF(
            "source",
            "source_type",
            "target",
            "target_type",
            "relationship",
            "frequency",
            "created_date",
            "last_seen_date",
            "active_flag"
        )

        // when
        DemographicIdentityRelationshipsJob.writeDemographicIdentityRelationship(
            transformedDemographicIdentityRelationship,
            demographicIdentityRelationshipTempTable
        )

        // then
        withClue("Written demographic identity relationship data do not match with expected data") {
            val actual_1 = spark
                .table(demographicIdentityRelationshipTempTable)
                .select(
                    "source",
                    "source_type",
                    "target",
                    "target_type",
                    "relationship",
                    "frequency",
                    "created_date",
                    "last_seen_date",
                    "active_flag"
                )
            val expected_1 = transformedDemographicIdentityRelationship
                .withColumn("created_date", col("created_date").cast(DateType))
                .withColumn("last_seen_date", col("last_seen_date").cast(DateType))
            actual_1.schema.fields.map(_.name) shouldEqual expected_1.schema.fields.map(_.name)
            actual_1.collect() should contain theSameElementsAs expected_1.collect()
        }

        // given
        val runDate2 = "2022-02-25"
        val runDate3 = "2022-02-23"
        val transformedDemographicIdentityRelationship2 = List(
            (
                "120087649",
                IdentityType.ThrotleId.code,
                "4da5d46e-114d-408e-95ac-d01dab3330c1",
                IdentityType.MaidIDFA.code,
                RelationshipType,
                1,
                runDate,
                runDate,
                true
            ), // duplicate, no update
            (
                "650407144",
                IdentityType.ThrotleId.code,
                "4da5d46e-114d-408e-95ac-d01dab3330c1",
                IdentityType.MaidAAID.code,
                RelationshipType,
                1,
                runDate2,
                runDate2,
                true
            ), // duplicate, updated last seen date
            (
                "13227693185",
                IdentityType.ThrotleId.code,
                "01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7",
                IdentityType.Email.code,
                RelationshipType,
                1,
                runDate3,
                runDate3,
                true
            ) // duplicate, updated create date
        ).toDF(
            "source",
            "source_type",
            "target",
            "target_type",
            "relationship",
            "frequency",
            "created_date",
            "last_seen_date",
            "active_flag"
        )

        // when
        DemographicIdentityRelationshipsJob.writeDemographicIdentityRelationship(
            transformedDemographicIdentityRelationship2,
            demographicIdentityRelationshipTempTable
        )

        // then
        withClue("Written demographic identity relationship data do not match with expected data") {
            val actual_2 = spark
                .table(demographicIdentityRelationshipTempTable)
                .select(
                    "source",
                    "source_type",
                    "target",
                    "target_type",
                    "relationship",
                    "frequency",
                    "created_date",
                    "last_seen_date",
                    "active_flag"
                )
            val expected_2 = List(
                (
                    "120087649",
                    IdentityType.ThrotleId.code,
                    "4da5d46e-114d-408e-95ac-d01dab3330c1",
                    IdentityType.MaidIDFA.code,
                    RelationshipType,
                    1,
                    sqlDate(runDate),
                    sqlDate(runDate),
                    true
                ), // from first write
                (
                    "650407144",
                    IdentityType.ThrotleId.code,
                    "4da5d46e-114d-408e-95ac-d01dab3330c1",
                    IdentityType.MaidAAID.code,
                    RelationshipType,
                    1,
                    sqlDate(runDate),
                    sqlDate(runDate2),
                    true
                ), // duplicate, updated last seen date
                (
                    "13227693185",
                    IdentityType.ThrotleId.code,
                    "01209b23560558fcc6c8b40850f59d0253264eb10a0b5116b66604f2555744f7",
                    IdentityType.Email.code,
                    RelationshipType,
                    1,
                    sqlDate(runDate3),
                    sqlDate(runDate),
                    true
                ) // duplicate, updated create date
            ).toDF(
                "source",
                "source_type",
                "target",
                "target_type",
                "relationship",
                "frequency",
                "created_date",
                "last_seen_date",
                "active_flag"
            )

            actual_2.schema.fields.map(_.name) shouldEqual expected_2.schema.fields.map(_.name)
            actual_2.collect() should contain theSameElementsAs expected_2.collect()
        }
    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createDemographicIdentityTable(demographicIdentityTempTable)
        createDemographicIdentityRelationshipTable(demographicIdentityRelationshipTempTable)
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTempTable(demographicIdentityTempTable)
        dropTempTable(demographicIdentityRelationshipTempTable)
    }

    def createDemographicIdentityTable(tableName: String): Unit = {
        spark.sql(s"""
               |CREATE TABLE IF NOT EXISTS $tableName
               (
               |    `cxi_identity_id`   string not null,
               |    `type`              string not null,
               |    `metadata`          map<string,string> not null
               |) USING delta
               |PARTITIONED BY (type);
               |""".stripMargin)
    }

    def createDemographicIdentityRelationshipTable(tableName: String): Unit = {
        spark.sql(s"""
               |CREATE TABLE IF NOT EXISTS $tableName
               (
               |    `source`         string not null,
               |    `source_type`    string not null,
               |    `target`         string not null,
               |    `target_type`    string not null,
               |    `relationship`   string not null,
               |    `frequency`      int not null,
               |    `created_date`   date not null,
               |    `last_seen_date` date not null,
               |    `active_flag`    boolean not null
               |) USING delta
               |PARTITIONED BY (relationship);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }
}
