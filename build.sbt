lazy val junitInterfaceVersion = "0.11"

ThisBuild / scalaVersion := "3.7.1"
Global / parallelExecution := false

lazy val core = (project in file("core"))
  .settings(
    name := "core",
    libraryDependencies += "ch.epfl.lamp" %% "gears" % "0.2.0",
    libraryDependencies += "com.novocode" % "junit-interface" % junitInterfaceVersion % "test",
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")
  )

lazy val sample = (project in file("sample"))
  .settings(
    name := "core",
    libraryDependencies += "ch.epfl.lamp" %% "gears" % "0.2.0"
  )
  .dependsOn(core)

lazy val failure = (project in file("failure"))
  .settings(
    name := "failure",
    libraryDependencies += "ch.epfl.lamp" %% "gears" % "0.2.0",
    libraryDependencies += "com.novocode" % "junit-interface" % junitInterfaceVersion,
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")
  )
  .dependsOn(core)

lazy val savina = (project in file("savina_benchmarks"))
  .settings(
    name := "Savina",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor" % "1.2.1",
      "org.apache.pekko" %% "pekko-stream" % "1.2.1"
    ),
    //testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
    Compile / scalaSource := baseDirectory.value / "src" / "main" / "scala",
    Compile / javaSource  := baseDirectory.value / "src"/ "main" / "java",
    Test / scalaSource    := baseDirectory.value / "src" / "test" / "scala",
  ).dependsOn(core)