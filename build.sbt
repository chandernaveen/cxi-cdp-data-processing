ThisBuild / organization := "com.cxi"
ThisBuild / scalaVersion := "2.12.10"
ThisBuild / version      := "1.0.0" // actual artifact version is controlled by azure devops (please check ci/cd for more details)
name := "cxi-cdp-data-processing"
SettingKey[Option[String]]("ide-package-prefix").withRank(KeyRanks.Invisible) := Option("com.cxi.cdp.data_processing")
lazy val sparkVersion = "3.1.2"
lazy val javaVersion = "1.8"
resolvers += "Spark Packages" at "https://repos.spark-packages.org/"

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
  "commons-validator" % "commons-validator" % "1.7"
    exclude("commons-beanutils", "commons-beanutils")
    exclude("commons-collections", "commons-collections")
    exclude("commons-logging", "commons-logging")
    exclude("commons-digester", "commons-digester"),
  "com.googlecode.libphonenumber" % "libphonenumber" % "8.12.45",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "io.delta" %% "delta-core" % "1.0.0" % Provided,
  "com.databricks" %% "dbutils-api" % "0.0.5" % Provided,
  "graphframes" % "graphframes" % "0.8.2-spark3.1-s_2.12" exclude("org.slf4j", "slf4j-api"),
  "org.elasticsearch" %% "elasticsearch-spark-30" % "7.17.0"
    excludeAll ExclusionRule(organization = "org.apache.spark")
    exclude("com.google.protobuf", "protobuf-java")
    exclude("commons-logging", "commons-logging")
    exclude("javax.xml.bind", "jaxb-api")
    exclude("org.scala-lang", "scala-reflect")
    exclude("org.slf4j", "slf4j-api")
)

dependencyOverrides ++= Seq(
    "com.google.protobuf" % "protobuf-java" % "2.6.1",
    "commons-collections" % "commons-collections" % "3.2.2",
    "commons-logging" % "commons-logging" % "1.1.3",
    "commons-beanutils" % "commons-beanutils" % "1.9.4",
    "commons-digester" % "commons-digester" % "1.8",
    "javax.xml.bind" % "jaxb-api" % "2.2.2",
    "org.scala-lang" %% "scala-reflect" % "2.12.10",
    "org.slf4j" % "slf4j-api" % "1.7.30",
    "org.slf4j" % "slf4j-log4j12" % "1.7.30"
)

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
  JacocoThresholds( instruction = 24.84,
            method = 13.51,
            branch = 7.66,
            complexity = 10.40,
            line = 36.0,
            clazz = 35.51),
  Seq(JacocoReportFormats.ScalaHTML, JacocoReportFormats.XML), "utf-8")

(Test / test) := ((Test / test) dependsOn assembly).value
(Test / test) := ((Test / test) dependsOn Keys.`package`.in(Test)).value // package src/test/scala into a separate jar for databricks-connect
