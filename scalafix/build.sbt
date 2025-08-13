lazy val V = _root_.scalafix.sbt.BuildInfo
inThisBuild(
  List(
    scalaVersion       := V.scala213,
    crossScalaVersions := List(V.scala213, V.scala212),
    addCompilerPlugin(scalafixSemanticdb),
    scalacOptions ++= List(
      "-Yrangepos",
      "-P:semanticdb:synthetics:on"
    )
  )
)

skip in publish := true

lazy val rules = project.settings(
  moduleName          := "scalafix",
  libraryDependencies += "ch.epfl.scala" %% "scalafix-core" % V.scalafixVersion
)

lazy val input = project.settings(
  skip in publish      := true,
  libraryDependencies ++= Seq("com.github.fd4s" %% "fs2-kafka" % "1.4.1")
)

lazy val output = project.settings(
  skip in publish      := true,
  libraryDependencies ++= Seq("com.github.fd4s" %% "fs2-kafka" % "3.0.0-M3")
)

lazy val tests = project
  .settings(
    skip in publish     := true,
    libraryDependencies += "ch.epfl.scala" % "scalafix-testkit" % V
      .scalafixVersion   % Test cross CrossVersion.full,
    compile.in(Compile) :=
      compile.in(Compile).dependsOn(compile.in(input, Compile)).value,
    scalafixTestkitOutputSourceDirectories :=
      unmanagedSourceDirectories.in(output, Compile).value,
    scalafixTestkitInputSourceDirectories :=
      unmanagedSourceDirectories.in(input, Compile).value,
    scalafixTestkitInputClasspath :=
      fullClasspath.in(input, Compile).value
  )
  .dependsOn(rules)
  .enablePlugins(ScalafixTestkitPlugin)
