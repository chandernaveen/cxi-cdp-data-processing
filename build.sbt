name := "cxi-cdp-data-processing"
version := "0.1.0"
scalaVersion := "2.12.10"
idePackagePrefix := Some("com.cxi.cdp.data_processing")

val sparkVersion = "3.1.1"

//resolvers += Resolver.jcenterRepo
//resolvers += Resolver.typesafeRepo("releases")

lazy val setupDatabricksConnect = taskKey[Unit]("Execute scripts that add Databricks jars")

setupDatabricksConnect := {
  val s: TaskStreams = streams.value
  val shell: Seq[String] = if (sys.props("os.name").contains("Windows")) Seq("cmd", "/c") else Seq("bash", "-c")
  val sbtLibPath = baseDirectory.value / "lib"
  val script: Seq[String] = shell :+
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
  if ((script !) == 0) {
    s.log.success("added jars successfully!")
  } else {
    throw new IllegalStateException("Adding Databricks jars task failed!")
  }
}
//(Compile / compile) := ((Compile / compile) dependsOn setupDatabricksConnect).value

libraryDependencies ++= Seq(
  "com.holdenkarau" %% "spark-testing-base" % (sparkVersion + "_1.1.0") % Test,
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "io.delta" %% "delta-core" % "0.7.0" % Provided,
  "com.databricks" %% "dbutils-api" % "0.0.5" % Provided
)

Test / fork := true
Test / parallelExecution := false
javaOptions ++= Seq("-XX:+PrintCommandLineFlags", "-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=./java_pid<pid>.hprof", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap")
testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  val databricksJarsPath = baseDirectory.value / "lib"
  cp filter { value => !value.data.getPath.contains(databricksJarsPath) }
}
assembly / assemblyOption ~= {
  _.withIncludeScala(false)
    .withIncludeDependency(false)
}
