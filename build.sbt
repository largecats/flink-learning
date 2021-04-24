ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.11.12"

lazy val common = (project in file("common"))
  .settings(name := "common")
  .settings(globalSettings)

lazy val examples = newProject(name = "examples")

lazy val globalSettings = dependencySettings ++ runSettings ++ assemblySettings

lazy val dependencySettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.flink" %% "flink-scala" % "1.12.2" % "provided",
    "org.apache.flink" %% "flink-streaming-scala" % "1.12.2" % "provided",
    "org.apache.flink" %% "flink-examples-streaming" % "1.12.2" % "provided",
    "joda-time" % "joda-time" % "2.9.3" % "provided",
    "org.joda" % "joda-convert" % "1.8" % "provided",
    "com.kailuowang" %% "henkan-convert" % "0.6.2",
    "com.github.daddykotex" %% "courier" % "1.0.0",
    "com.softwaremill.sttp" %% "core" % "1.5.11",
    // for testing
    "junit" % "junit" % "4.11" % Test,
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
  ),
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", _ @_*) => MergeStrategy.discard

    case x: Any =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  // don't package Scala library
  assembly / assemblyOption := (assembly / assemblyOption).value
    .copy(includeScala = false)
)

def newProject(name: String): Project =
  createProject(name, name)

def newProject(name: String, parent: String): Project =
  createProject(name, s"$parent/$name").withId(s"${parent}_$name")

def createProject(name: String, path: String): Project = {
  Project(name, file(path))
    .settings(globalSettings)
    .settings(
      assembly / mainClass := Some("Main"),
      assembly / assemblyJarName := s"$name.jar"
    )
    .dependsOn(common % "compile->compile;test->test") // the compilation/test of project depends on the compilation/test of common
}