package com.cxi.cdp.data_processing
package support

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import java.nio.file.{Files, Paths}
import java.util.stream.Collectors

object SparkSessionFactory {
    private def isLocalEnv(): Boolean = {
        try {
            "local".equals(Files.readAllLines(Paths.get("env.properties")).stream().collect(Collectors.joining()))
        } catch {
            case e: java.nio.file.NoSuchFileException => false
        }
    }

    def getSparkSession(conf: SparkConf = new SparkConf): SparkSession = {
        val builder = SparkSession.builder()
        val isLocalEnv: Boolean = SparkSessionFactory.isLocalEnv()
        if (isLocalEnv) {
            // disable auto-adding dependencies jars to the databricks cluster
            // (as we already have all dependencies in the uber jar)
            conf.set("spark.databricks.service.client.checkDeps", "false")
            conf.set("spark.databricks.service.client.autoAddDeps", "false")
            builder.master("local")
        }
        if (!conf.getAll.isEmpty) {
            builder.config(conf)
        }
        val session = builder.getOrCreate()
        if (isLocalEnv) {
            session.sparkContext.addJar(assemblyJarLocalPath)
        }
        session
    }

    def assemblyJarLocalPath: String = "target/scala-2.12/cxi_cdp_data_processing_assembly_2_12.jar"
}
