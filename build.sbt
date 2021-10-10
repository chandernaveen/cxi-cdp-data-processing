
ThisBuild / organization := "com.cxi"
ThisBuild / scalaVersion := "2.12.10"
ThisBuild / version      := "0.1.0"
name := "cxi-cdp-data-processing"
idePackagePrefix := Some("com.cxi.cdp.data_processing")
lazy val sparkVersion = "3.1.1"
lazy val javaVersion = "1.8"

lazy val setupDatabricksConnect = taskKey[Unit]("Execute scripts that add Databricks jars")

setupDatabricksConnect := {
  val s: TaskStreams = streams.value
  val sbtLibPath = baseDirectory.value / "lib"

  val windowsScript = Seq("cmd", "/c") :+
      s"""
        |./venv/Scripts/activate.bat
        |pip install -r requirements.txt
        |databricks_jars_base_path="$$(databricks-connect get-jar-dir)"
        |echo "$$databricks_jars_base_path"
        |if exist $sbtLibPath\\nul
        |(mklink $$databricks_jars_base_path $sbtLibPath)
        |""".stripMargin
  val unixScript: Seq[String] = Seq("bash", "-c") :+
    s"""source .venv/bin/activate
       |pip install -r requirements.txt
       |databricks_jars_base_path="$$(databricks-connect get-jar-dir)"
       |echo "$$databricks_jars_base_path"
       |if ! [ -L $sbtLibPath ]; then
       |ln -s $$databricks_jars_base_path $sbtLibPath
       |fi
       |""".stripMargin
  val script = if (sys.props("os.name").contains("Windows")) windowsScript else unixScript
  s.log.info("adding databricks jars...")
  import scala.sys.process._
  if ((script !) == 0) {
    s.log.success("added jars successfully!")
  } else {
    throw new IllegalStateException("Setup Databricks Connect task failed!")
  }
}

libraryDependencies ++= Seq(
  "com.holdenkarau" %% "spark-testing-base" % (sparkVersion + "_1.1.0") % Test,
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "io.delta" %% "delta-core" % "0.8.0" % Provided,
  "com.databricks" %% "dbutils-api" % "0.0.5" % Provided)

Test / fork := true
Test / parallelExecution := false
testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

javacOptions ++= Seq("-source", javaVersion, "-target", javaVersion)
javaOptions ++= Seq("-XX:+PrintCommandLineFlags", "-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=./java_pid<pid>.hprof", "-XX:+UnlockExperimentalVMOptions")
scalacOptions += s"-target:jvm-$javaVersion"

//assembly / assemblyJarName := s"${normalizedName.value.replace("-", "_")}_assembly_${scalaBinaryVersion.value.replace(".", "_")}_${version.value.replace(".", "_")}.jar" TODO: uncomment once we have proper CI/CD with artifact repository and versionning implemented
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
//scalastyleFailOnWarning := true TODO: uncomment after fixing styling issues
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value
