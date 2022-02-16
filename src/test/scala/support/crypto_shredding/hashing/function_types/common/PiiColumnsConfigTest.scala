package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.common

import support.BaseSparkBatchJobTest

import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class PiiColumnsConfigTest extends BaseSparkBatchJobTest {

    test("test config parse") {
        val mapper = new ObjectMapper() with ScalaObjectMapper
        mapper.registerModule(DefaultScalaModule)
        val rawConfigJson =
            """
              |{
              |   "pii_columns": [
              |       {
              |           "column": "MAID_AAID",
              |           "identity_type": "MAID-AAID"
              |       },
              |       {
              |           "column": "ipv_4",
              |           "identity_type": "ipv4"
              |       },
              |       {
              |           "column": "ipv_6",
              |           "identity_type": "ipv6"
              |       }
              |   ]
              |}
              |""".stripMargin
        val conf = mapper.readValue[Map[String, Object]](rawConfigJson)
        val rawConfig = conf("pii_columns").asInstanceOf[Seq[Map[String, Any]]]

        // when
        val piiColumnsConfig = PiiColumnsConfig.parse(rawConfig)

        // then
        piiColumnsConfig.columns.map(columnsConfig => (columnsConfig._1, columnsConfig._3.map(_.code).orNull)) should contain theSameElementsAs
            List(
                ("MAID_AAID", IdentityType.MaidAAID.code),
                ("ipv_4", IdentityType.IPv4.code),
                ("ipv_6", IdentityType.IPv6.code))

    }

}
