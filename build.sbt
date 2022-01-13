
ThisBuild / organization := "com.cxi"
ThisBuild / scalaVersion := "2.12.10"
ThisBuild / version      := "1.0.0" // actual artifact version is controlled by azure devops (please check ci/cd for more details)
name := "cxi-cdp-data-processing"
idePackagePrefix := Some("com.cxi.cdp.data_processing")
lazy val sparkVersion = "3.1.2"
lazy val javaVersion = "1.8"

lazy val setupDatabricksConnect = taskKey[Unit]("Execute scripts that add Databricks jars")

setupDatabricksConnect := {
  val s: TaskStreams = streams.value
  val isUnix = !sys.props("os.name").contains("Windows")
  val sbtLibPath = baseDirectory.value / "lib"

  val unixScript: Seq[String] = Seq("bash", "-c") :+
    s"""source .venv/bin/activate
       |pip install -r requirements.txt
       |databricks_jars_base_path="$$(databricks-connect get-jar-dir)"
       |echo "$$databricks_jars_base_path"
       |if ! [ -L $sbtLibPath ]; then
       |ln -s $$databricks_jars_base_path $sbtLibPath
       |fi
       |""".stripMargin

  s.log.info("adding databricks jars...")
  import scala.sys.process._
  if (isUnix && (unixScript !) == 0) {
    s.log.success("added jars successfully!")
  } else {
    throw new IllegalStateException("Setup Databricks Connect task failed!")
  }
}

libraryDependencies ++= Seq(
  "com.holdenkarau" %% "spark-testing-base" % (sparkVersion + "_1.1.0") % Test,
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,
  "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % Test,
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "com.github.scopt" %% "scopt" % "4.0.1",
  "com.jayway.jsonpath" % "json-path" % "2.6.0" exclude("org.slf4j", "slf4j-api"),
  "com.beachape" %% "enumeratum" % "1.7.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "io.delta" %% "delta-core" % "1.0.0" % Provided,
  "com.databricks" %% "dbutils-api" % "0.0.5" % Provided)

Test / fork := true
Test / parallelExecution := false
testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

javacOptions ++= Seq("-source", javaVersion, "-target", javaVersion)
javaOptions ++= Seq("-XX:+PrintCommandLineFlags", "-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=./java_pid<pid>.hprof", "-XX:+UnlockExperimentalVMOptions")
scalacOptions += s"-target:jvm-$javaVersion"

assembly / assemblyJarName := s"${normalizedName.value.replace("-", "_")}_assembly_${scalaBinaryVersion.value.replace(".", "_")}.jar"
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  val databricksJarsPath = s"""${baseDirectory.value / "lib"}"""

  def shouldExcludeJar(f: Attributed[File]): Boolean =
    f.data.getPath.startsWith(databricksJarsPath) // do not include databricks-connect jars in uber-jar as they already present on the cluster
  cp filter shouldExcludeJar
}
assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

scalastyleFailOnError := true
scalastyleFailOnWarning := true
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

jacocoReportSettings := JacocoReportSettings(
  "Jacoco Coverage Report",
  None,
  JacocoThresholds( instruction = 8.28,
            method = 3.69,
            branch = 1.52,
            complexity = 2.60,
            line = 14.30,
            clazz = 12.49),
  Seq(JacocoReportFormats.ScalaHTML, JacocoReportFormats.XML), "utf-8")

(Test / test) := ((Test / test) dependsOn assembly).value
