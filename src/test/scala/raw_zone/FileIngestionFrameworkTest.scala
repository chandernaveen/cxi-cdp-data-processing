package com.cxi.cdp.data_processing
package raw_zone

import raw_zone.file_ingestion_framework.FileIngestionFrameworkConfig
import raw_zone.FileIngestionFramework.CliArgs
import support.utils.ContractUtils
import support.BaseSparkBatchJobTest
import support.SparkPrettyStringClueFormatter.toPrettyString

import org.apache.hadoop.fs.{FileSystem, Path}
import org.mockito.Mockito.when
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.LocalDate

class FileIngestionFrameworkTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("test read - no fileFormat prop specified") {
        // given
        val config = mock[FileIngestionFrameworkConfig]
        when(config.fileFormat).thenReturn(null)

        // when
        assertThrows[IllegalArgumentException] {
            val srcDf = FileIngestionFramework.read(config)(spark)
        }

        // given
        when(config.fileFormat).thenReturn("")

        // when
        assertThrows[IllegalArgumentException] {
            val srcDf = FileIngestionFramework.read(config)(spark)
        }
    }

    test("test read - fileFormatOptions is null") {
        // given
        val config = mock[FileIngestionFrameworkConfig]
        when(config.fileFormat).thenReturn("json")
        when(config.fileFormatOptions).thenReturn(null)

        // when
        assertThrows[IllegalArgumentException] {
            val srcDf = FileIngestionFramework.read(config)(spark)
        }
    }

    test("test read - no files under source path") {
        // given
        val config = mock[FileIngestionFrameworkConfig]
        when(config.fileFormat).thenReturn("json")
        when(config.fileFormatOptions).thenReturn(Map[String, String]())
        val path = new Path(s"/tmp/${generateUniqueTableName(FileIngestionFrameworkTest.this.getClass.getSimpleName)}/")
        when(config.sourcePath).thenReturn(path.toString)

        makeTmpDirsAndExecuteFunctionBody(path) {
            // when
            assertThrows[IllegalStateException] {
                val srcDf = FileIngestionFramework.read(config)(spark)
            }
        }
    }

    test("test read - there are files under source path") {
        // given
        val config = mock[FileIngestionFrameworkConfig]
        when(config.fileFormat).thenReturn("json")
        when(config.fileFormatOptions).thenReturn(Map[String, String]())
        val path = new Path(
            s"/tmp/${generateUniqueTableName(FileIngestionFrameworkTest.this.getClass.getSimpleName)}/some_file.json"
        )
        when(config.sourcePath).thenReturn(path.toString)

        createTmpFileWithContentAndExecuteFunctionBody(path, """{ "a": 1, "b": true, "c": "yes" }""") {
            // when
            val srcDf = FileIngestionFramework.read(config)(spark)

            // then
            srcDf
                .select("a", "b", "c")
                .collect() shouldBe spark.createDataFrame(Seq((1, true, "yes"))).toDF("a", "b", "c").collect()
        }
    }

    private def makeTmpDirsAndExecuteFunctionBody[T](path: Path)(body: => T): T = {
        val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        try {
            fileSystem.mkdirs(path)
            body
        } finally {
            fileSystem.delete(path, false)
            fileSystem.close()
        }
    }

    private def createTmpFileWithContentAndExecuteFunctionBody[T](path: Path, content: String)(body: => T): T = {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        def writeToFileAndClose(): Unit = {
            var bufferedWriter: BufferedWriter = null
            try {
                bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(fs.create(path), StandardCharsets.UTF_8)
                )
                bufferedWriter.write(content);
                bufferedWriter.flush()
            } finally {
                if (bufferedWriter != null) {
                    bufferedWriter.close()
                }
            }
        }

        try {
            writeToFileAndClose()
            body
        } finally {
            fs.delete(path, false)
            fs.close()
        }
    }

    test("test transform with no cryptoshredding") {
        // given
        val config = mock[FileIngestionFrameworkConfig]
        when(config.transformationName).thenReturn("identity")
        val cliArgs = mock[CliArgs]
        when(cliArgs.date).thenReturn(LocalDate.of(2022, 2, 24))
        when(cliArgs.runId).thenReturn("some_run_id")
        val contract = new ContractUtils(
            s"""
               | {"some_prop": "abc" }
               |""".stripMargin
        )
        val srcDf = spark.createDataFrame(Seq((1, false, "no"))).toDF("col_a", "col_b", "col_c")

        // when
        val actual = FileIngestionFramework.transform(srcDf, cliArgs, contract, config)(spark)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("col_a", "col_b", "col_c", "feed_date", "file_name", "cxi_id")
        }
        val actualTransformedData = actual.drop("cxi_id").collect()
        withClue("Actual transformed data do not match" + toPrettyString(actualTransformedData)) {
            val expected = List((1, false, "no", "2022-02-24", ""))
                .toDF("col_a", "col_b", "col_c", "feed_date", "file_name")
                .collect()
            actualTransformedData.length should equal(expected.length)
            actualTransformedData should contain theSameElementsAs expected
        }

    }

}
