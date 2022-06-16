package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.function_types.json

import com.cxi.cdp.data_processing.support.crypto_shredding.hashing.function_types.CryptoHashingResult
import support.crypto_shredding.hashing.Hash

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider

import scala.collection.mutable

class JsonNodeHasher(
    private val config: PiiColumnsConfig,
    private val salt: String,
    private val mapper: ObjectMapper with ScalaObjectMapper
) {

    import PiiColumnsConfig._

    private val jsonPathConfig = Configuration.defaultConfiguration
        .jsonProvider(new JacksonJsonNodeJsonProvider()) // process Jackson JsonNode objects
        .addOptions(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS) // don't fail if json path is not found

    private val jsonPathCache = mutable.Map.empty[String, JsonPath]

    private def compiledJsonPath(rawJsonPath: String): JsonPath = {
        jsonPathCache.getOrElseUpdate(rawJsonPath, JsonPath.compile(rawJsonPath))
    }

    def apply(jsonNode: JsonNode): JsonNode = {
        jsonNode match {
            case objectNode: ObjectNode => processObjectNode(objectNode)
            case _ => jsonNode
        }
    }

    private def processObjectNode(objectNode: ObjectNode): ObjectNode = {
        val result: ObjectNode = objectNode.deepCopy()
        val hashedData = result.putArray("hashed_data")

        for (piiField <- config.columns) {
            val (outerCol, innerCol, transformFunction, identityTypeOpt) = piiField

            if (result.has(outerCol)) {
                val node = result.get(outerCol)

                // if we already processed outerCol before we are working not with string but reusing actual node
                val innerContent = if (node.isTextual) mapper.readTree(node.asText()) else node

                innerCol match {
                    case InnerColumn.JsonPath(jsonPath) =>
                        compiledJsonPath(jsonPath)
                            .map(
                                innerContent,
                                (originalValue, _) => {
                                    convertToString(originalValue) match {
                                        case None => originalValue.asInstanceOf[Object]

                                        case Some(originalValueAsString) =>
                                            val normalizedValue = transformFunction(originalValueAsString)
                                            val hashedValue: String = Hash.sha256Hash(normalizedValue, salt)

                                            // extract pii info to a separate top level column
                                            val entry = hashedData.addObject()
                                            entry.put(CryptoHashingResult.OriginalValueColName, normalizedValue)
                                            entry.put(CryptoHashingResult.HashedValueColName, hashedValue)
                                            entry.put(
                                                CryptoHashingResult.IdentityTypeValueColName,
                                                identityTypeOpt.map(_.code).orNull
                                            )

                                            hashedValue
                                    }
                                },
                                jsonPathConfig
                            )

                        result.set[ObjectNode](outerCol, innerContent)
                }
            }
        }
        result
    }

    private def convertToString(originalValue: Any): Option[String] = {
        Option(originalValue)
            .map({
                case textNode: TextNode => textNode.asText
                case other => other.toString
            })
    }

}
