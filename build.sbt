ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.11.12"

lazy val common = (project in file("common"))
  .settings(name := "common")
  .settings(globalSettings)

lazy val example = newProject(name = "example")
lazy val logging = newExampleProject(name = "logging")
lazy val wordCount = newExampleProject(name = "wordCount")
lazy val kafkaConsumer = newExampleProject(name = "kafkaConsumer")
lazy val kafkaProducer = newExampleProject(name = "kafkaProducer")
lazy val kafkaTable = newExampleProject(name = "kafkaTable")
lazy val fraudDetection = newExampleProject(name = "fraudDetection")
lazy val haFailFast = newExampleProject(name = "haFailFast")

lazy val useCase = newProject(name = "useCase")
lazy val accumulatedAggregation = newUseCaseProject(name = "accumulatedAggregation")
lazy val splitStreamByKeyLocalFs = newUseCaseProject(name = "splitStreamByKeyLocalFs")
lazy val splitStreamByKeyHdfsSameSchema = newUseCaseProject(name = "splitStreamByKeyHdfsSameSchema")
lazy val splitStreamByKeyHdfsParquetSameSchema = newUseCaseProject(name = "splitStreamByKeyHdfsParquetSameSchema")
lazy val splitStreamByKeyHdfsParquetDifferentSchema = newUseCaseProject(
  name = "splitStreamByKeyHdfsParquetDifferentSchema"
)
lazy val splitKafkaTopic = newUseCaseProject(name = "splitKafkaTopic")
lazy val autoScaling = newUseCaseProject(name = "autoScaling")

lazy val globalSettings = dependencySettings ++ runSettings ++ assemblySettings

lazy val dependencySettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.flink" %% "flink-scala" % "1.13.1" % "provided",
    "org.apache.flink" %% "flink-streaming-scala" % "1.13.1" % "provided",
    "org.apache.flink" %% "flink-streaming-java" % "1.13.1" % "provided",
    "org.apache.flink" %% "flink-examples-streaming" % "1.13.1" % "provided",
    "org.apache.flink" % "flink-table-common" % "1.13.1" % "provided", // for implementing custom format or connector
    "org.apache.flink" %% "flink-table-api-java-bridge" % "1.13.1" % "provided", // for Table API & SQL
    "org.apache.flink" %% "flink-table-api-scala-bridge" % "1.13.1" % "provided",
    "org.apache.flink" %% "flink-table-planner" % "1.13.1" % "provided", // for running Table API & SQL locally in IDE
    "org.apache.flink" %% "flink-table-planner-blink" % "1.13.1" % "provided", // for running Table API & SQL locally in IDE
    "org.apache.flink" %% "flink-clients" % "1.13.1" % "provided",
    "org.apache.flink" %% "flink-connector-kafka" % "1.13.1",
    "org.apache.flink" % "flink-csv" % "1.13.1",
    "org.apache.flink" % "flink-json" % "1.13.1",
    "org.apache.flink" %% "flink-parquet" % "1.13.1",
    "org.apache.flink" %% "flink-hadoop-compatibility" % "1.13.1" % "provided",
    "org.apache.hadoop" % "hadoop-client" % "2.8.3", // no need _2.11 after hadoop-client
    "org.apache.flink" % "flink-avro" % "1.13.1",
    "org.apache.parquet" % "parquet-avro" % "1.12.0",
    "org.apache.parquet" % "parquet-hadoop" % "1.12.0",
    "org.apache.flink" %% "flink-walkthrough-common" % "1.13.1" % "provided", // for example/fraudDetection
    "org.json4s" %% "json4s-native" % "4.0.0",
    "org.json4s" %% "json4s-jackson" % "4.0.0",
    "joda-time" % "joda-time" % "2.9.3" % "provided",
    "org.joda" % "joda-convert" % "1.8" % "provided",
    "com.kailuowang" %% "henkan-convert" % "0.6.2",
    "com.github.daddykotex" %% "courier" % "1.0.0",
    "com.softwaremill.sttp" %% "core" % "1.5.11",
    // for testing
    "junit" % "junit" % "4.11" % Test,
    "org.mockito" % "mockito-core" % "3.9.0" % Test,
    "org.apache.flink" %% "flink-test-utils" % "1.13.1" % Test,
    "org.apache.flink" %% "flink-runtime" % "1.13.1" % Test classifier "tests",
    "org.apache.flink" %% "flink-streaming-java" % "1.13.1" % Test classifier "tests", // test-jar, for TestHarnesses only
    "org.scalacheck" %% "scalacheck" % "1.13.5" % Test, // dependency is only for the Test configuration
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.6" % Test
  )
)

lazy val runSettings = Seq(
  scalacOptions := Seq("-unchecked", "-deprecation"),
  // add back 'provided' dependencies when calling 'run' task. This allows us to run modules and
  // testing locally.
  Compile / run := Defaults
    .runTask(
      Compile / fullClasspath,
      Compile / run / mainClass,
      Compile / run / runner
    )
    .evaluated,
  Test / testOptions += Tests.Argument("-oD"),
  // show message periodically for long running tests
  Test / testOptions += Tests
    .Argument(TestFrameworks.ScalaTest, "-W", "30", "15"),
  Test / logBuffered := false,
  Test / parallelExecution := false
)

lazy val assemblySettings = Seq(
  // disable test when package
  assembly / test := {},
  // since classes with the same name in one package is not allowed, we need to
  // rename (i.e. "shade") them
  assembly / assemblyShadeRules := Seq(
    // rename 'shapeless' to avoid accidentally using the ancient version of this library
    // from Spark
    ShadeRule
      .rename("shapeless.**" -> "shaded.@0")
      .inLibrary("com.chuusai" % "shapeless_2.11" % "2.3.3")
      .inLibrary("com.kailuowang" % "henkan-convert_2.11" % "0.6.2")
      .inLibrary("org.tpolecat" % "doobie-core_2.11" % "0.5.4")
      .inProject
//    ShadeRule
//      .rename("org.apache.commons.beanutils.**" -> "shadedstuff.beanutils.@1")
//      .inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0")
  ),
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) =>
      (xs map { _.toLowerCase }) match {
        case "services" :: xs =>
          MergeStrategy.concat
        case _ => MergeStrategy.discard
      }
    case "module-info.class" =>
      MergeStrategy.concat // resolve clash among jackson-databind, jackson-annotations, and jackson-core used by json4s-jackson
//    case x: Any =>
//      val oldStrategy = (assembly / assemblyMergeStrategy).value
//      oldStrategy(x)
    case x => MergeStrategy.first
  },
  // don't package Scala library
  assembly / assemblyOption := (assembly / assemblyOption).value
    .copy(includeScala = false)
)

def newProject(name: String): Project =
  createProject(name, name)

def newProject(name: String, parent: String): Project =
  createProject(name, s"$parent/$name").withId(s"${parent}_$name")

def newExampleProject(name: String): Project =
  newProject(name, parent = "example")

def newUseCaseProject(name: String): Project =
  newProject(name, parent = "useCase")

def createProject(name: String, path: String): Project = {
  Project(name, file(path))
    .settings(globalSettings)
    .settings(
      assembly / mainClass := Some("Main"),
      assembly / assemblyJarName := s"$name.jar"
    )
    .dependsOn(common % "compile->compile;test->test") // the compilation/test of project depends on the compilation/test of common
}
