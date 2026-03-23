lazy val junitInterfaceVersion = "0.11"

ThisBuild / scalaVersion   := "3.7.1"
Global / parallelExecution := false

lazy val core = (project in file("core"))
  .settings(
    name                                  := "core",
    libraryDependencies += "ch.epfl.lamp" %% "gears"           % "0.2.0",
    libraryDependencies += "com.novocode"  % "junit-interface" % junitInterfaceVersion % "test",
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")
  )

lazy val sample = (project in file("sample"))
  .settings(
    name                                  := "core",
    libraryDependencies += "ch.epfl.lamp" %% "gears" % "0.2.0"
  )
  .dependsOn(core)
