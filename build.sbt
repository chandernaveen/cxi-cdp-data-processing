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
  "org.mockito" % "mockito-core" % "3.4.6" % Test,
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "com.github.scopt" %% "scopt" % "4.0.1",
  "com.jayway.jsonpath" % "json-path" % "2.6.0",
  "com.beachape" %% "enumeratum" % "1.7.0",
  "commons-validator" % "commons-validator" % "1.7"
    exclude("commons-beanutils", "commons-beanutils")
    exclude("commons-collections", "commons-collections")
    exclude("commons-logging", "commons-logging")
    exclude("commons-digester", "commons-digester"),
  "com.googlecode.libphonenumber" % "libphonenumber" % "8.12.45",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "graphframes" % "graphframes" % "0.8.2-spark3.1-s_2.12",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "7.17.0"
    exclude("com.google.protobuf", "protobuf-java")
    exclude("commons-logging", "commons-logging")
    exclude("javax.xml.bind", "jaxb-api")
    exclude("org.scala-lang", "scala-reflect"),
  "org.apache.sedona" %% "sedona-viz-3.0" % "1.1.1-incubating" % Provided,
  "org.apache.sedona" %% "sedona-python-adapter-3.0" % "1.1.1-incubating" % Provided,
  "org.datasyslab" % "geotools-wrapper" % "1.1.0-25.2" % Provided,
  "me.xdrop" % "fuzzywuzzy" % "1.4.0"
)

dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0",
    "com.fasterxml.jackson.core" % "jackson-annotations" % "2.10.0",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0",
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

excludeDependencies ++= Seq(
    ExclusionRule(organization = "org.apache.spark"),
    ExclusionRule(organization = "org.slf4j")
)

Test / fork := true
Test / parallelExecution := false
testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

javacOptions ++= Seq("-source", javaVersion, "-target", javaVersion)
javaOptions ++= Seq("-XX:+PrintCommandLineFlags", "-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=./java_pid<pid>.hprof", "-XX:+UnlockExperimentalVMOptions")
scalacOptions += s"-target:jvm-$javaVersion"

// enable assembly test artifact creation
Project.inConfig(Test)(baseAssemblySettings)

assembly / assemblyJarName := s"${normalizedName.value.replace("-", "_")}_assembly_${scalaBinaryVersion.value.replace(".", "_")}.jar"

Test / assembly / assemblyJarName := s"${normalizedName.value.replace("-", "_")}_assembly_test_${scalaBinaryVersion.value.replace(".", "_")}.jar"

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

assembly / assemblyMergeStrategy := {
    case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last == "log4j.properties" => MergeStrategy.discard
    case PathList("scala", _ @ _*) => MergeStrategy.discard
    case x => {
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
}

/* Filter out undesirable dependencies for the test assembly jar:
 * - provided dependencies are included into the test assembly jar by default; it seems to be a bug: https://github.com/sbt/sbt-assembly/issues/295
 * - jacoco instruments the classes, resulting in duplicates that are not needed in the test assembly jar
 */
Test / assembly / fullClasspath := {
    val cp = (Test / assembly / fullClasspath).value
    val providedDependencies = update.map (f => f.select(configurationFilter("provided"))).value
    cp.filter { f => !providedDependencies.contains(f.data) && !f.data.getPath.contains("jacoco") }
}

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

scalastyleFailOnError := true
scalastyleFailOnWarning := true
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value




jacocoExcludes   :=Seq(
    "com.cxi.cdp.data_processing.curated_zone.audience.model*","com.cxi.cdp.data_processing.raw_zone.pos_omnivore.model*",
    "com.cxi.cdp.data_processing.raw_zone.pos_square.model*","com.cxi.cdp.data_processing.refined_zone.hub.model*",
    "com.cxi.cdp.data_processing.refined_zone.pos_omnivore.model*","com.cxi.cdp.data_processing,refined_zone.pos_square.model*",
    "com.cxi.cdp.data_processing.refined_zone.pos_toast.model*"
)

jacocoReportSettings := JacocoReportSettings(
  "Jacoco Coverage Report",
  None,
  JacocoThresholds(instruction = 41.6,
            method = 17.7,
            branch = 12.1,
            complexity = 15.2,
            line = 57.8,
            clazz = 45.9),
  Seq(JacocoReportFormats.ScalaHTML, JacocoReportFormats.XML), "utf-8")



(Compile / runMain) := Defaults.runMainTask(Compile / fullClasspath, runner.in(Compile, run)).evaluated // set correct classpath for runMain
(Compile / runMain) := ((Compile / runMain) dependsOn assembly).evaluated // build assembly jar for runMain

(Test / test) := ((Test / test) dependsOn assembly).value // we don't actually need the assembly jar for tests but build it to produce the main PR artifact
(Test / test) := ((Test / test) dependsOn (Test / assembly)).value // package the test assembly jar for databricks-connect

(Test / testOnly) := ((Test / testOnly) dependsOn assembly).evaluated // we don't actually need the assembly jar for tests but build it to produce the main PR artifact
(Test / testOnly) := ((Test / testOnly) dependsOn (Test / assembly)).evaluated // package the test assembly jar for databricks-connect
