package com.cxi.cdp.data_processing
package raw_zone

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.convertToAnyShouldWrapper

class FileIngestionFrameworkTransformationsTest extends BaseSparkBatchJobTest {
    before {
        conf.set("hive.exec.dynamic.partition", "true")
        conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    }

    test("test identity transformation") {
        // given
        val expected = spark.createDataFrame(List((1, 1.0), (2, 2.0), (3, 3.0))).toDF("id", "v")

        // when
        val actual = FileIngestionFrameworkTransformations.identity(expected)

        // then
        assertDataFrameEquals(expected, actual)
    }

    test("test column names to underscores transformation") {
        // given
        val expected = spark.createDataFrame(List((1, 1.0), (2, 2.0), (3, 3.0))).toDF("this is id", "v")

        // when
        val actual = FileIngestionFrameworkTransformations.spaceToUnderScoreInColumnNamesTransformation(expected)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("this_is_id", "v")
        }
    }
}
