val avroVersion                = "1.12.1"
val catsEffectVersion          = "3.7.0"
val catsVersion                = "2.13.0"
val confluentVersion           = "8.2.0"
val disciplineVersion          = "2.3.0"
val fs2Version                 = "3.13.0"
val kafkaVersion               = "4.2.0"
val logbackVersion             = "1.5.32"
val munitVersion               = "1.2.4"
val slf4jVersion               = "1.7.36"
val testcontainersScalaVersion = "0.44.1"
val vulcanVersion              = "1.13.0"

val scala212 = "2.12.21"
val scala213 = "2.13.18"
val scala3   = "3.3.7"

ThisBuild / tlBaseVersion := "4.0"

lazy val root = project
  .in(file("."))
  .settings(
    noPublishSettings,
    scalaSettings,
    mimaPreviousArtifacts := Set.empty,
    console               := (core / Compile / console).value,
    Test / console        := (core / Test / console).value
  )
  .enablePlugins(TypelevelMimaPlugin)
  .aggregate(core, vulcan, `vulcan-testkit-munit`)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    moduleName := "fs2-kafka",
    name       := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "co.fs2"          %% "fs2-core"           % fs2Version,
        "org.apache.kafka" % "kafka-clients"      % kafkaVersion,
        "org.slf4j"        % "slf4j-api"          % slf4jVersion,
        "org.typelevel"   %% "cats-core"          % catsVersion,
        "org.typelevel"   %% "cats-effect-kernel" % catsEffectVersion,
        "org.typelevel"   %% "cats-effect-std"    % catsEffectVersion,
        "org.typelevel"   %% "cats-effect"        % catsEffectVersion,
        "org.typelevel"   %% "cats-kernel"        % catsVersion
      )
    ),
    publishSettings,
    scalaSettings,
    testSettings
  )

lazy val vulcan = project
  .in(file("modules/vulcan"))
  .settings(
    moduleName := "fs2-kafka-vulcan",
    name       := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.github.fd4s" %% "vulcan"                       % vulcanVersion,
        "io.confluent"     % "kafka-avro-serializer"        % confluentVersion,
        "io.confluent"     % "kafka-schema-registry-client" % confluentVersion,
        "io.confluent"     % "kafka-schema-serializer"      % confluentVersion,
        "org.apache.avro"  % "avro"                         % avroVersion,
        "org.typelevel"   %% "cats-core"                    % catsVersion,
        "org.typelevel"   %% "cats-effect-kernel"           % catsEffectVersion,
        "org.typelevel"   %% "cats-effect"                  % catsEffectVersion
      )
    ),
    publishSettings,
    scalaSettings,
    testSettings
  )
  .dependsOn(core)

lazy val `vulcan-testkit-munit` = project
  .in(file("modules/vulcan-testkit-munit"))
  .settings(
    moduleName := "fs2-kafka-vulcan-testkit-munit",
    name       := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.github.fd4s" %% "vulcan"                       % vulcanVersion,
        "io.confluent"     % "kafka-schema-registry-client" % confluentVersion,
        "org.apache.avro"  % "avro"                         % avroVersion,
        "org.scalameta"   %% "munit-diff"                   % munitVersion,
        "org.scalameta"   %% "munit"                        % munitVersion,
        "org.typelevel"   %% "cats-effect"                  % catsEffectVersion
      )
    ),
    publishSettings,
    scalaSettings,
    testSettings,
    versionIntroduced("2.2.0")
  )
  .dependsOn(vulcan)

lazy val docs = project
  .in(file("docs"))
  .settings(
    moduleName := "fs2-kafka-docs",
    name       := moduleName.value,
    dependencySettings,
    noPublishSettings,
    scalaSettings,
    mdocSettings,
    buildInfoSettings
  )
  .dependsOn(core, vulcan, `vulcan-testkit-munit`)
  .enablePlugins(BuildInfoPlugin, DocusaurusPlugin, MdocPlugin, ScalaUnidocPlugin)

lazy val dependencySettings = Seq(
  resolvers            += "confluent".at("https://packages.confluent.io/maven/"),
  libraryDependencies ++= Seq(
    "com.dimafeng"  %% "testcontainers-scala-scalatest" % testcontainersScalaVersion,
    "com.dimafeng"  %% "testcontainers-scala-kafka"     % testcontainersScalaVersion,
    "org.typelevel" %% "discipline-scalatest"           % disciplineVersion,
    "org.typelevel" %% "cats-effect-laws"               % catsEffectVersion,
    "org.typelevel" %% "cats-effect-testkit"            % catsEffectVersion,
    "ch.qos.logback" % "logback-classic"                % logbackVersion
  ).map(_ % Test),
  libraryDependencies ++= {
    if (scalaVersion.value.startsWith("3")) Nil
    else
      Seq(
        compilerPlugin(
          ("org.typelevel" %% "kind-projector" % "0.13.4").cross(CrossVersion.full)
        )
      )
  },
  pomPostProcess := { (node: xml.Node) =>
    new xml.transform.RuleTransformer(new xml.transform.RewriteRule {

      def scopedDependency(e: xml.Elem): Boolean =
        e.label == "dependency" && e.child.exists(_.label == "scope")

      override def transform(node: xml.Node): xml.NodeSeq =
        node match {
          case e: xml.Elem if scopedDependency(e) => Nil
          case _                                  => Seq(node)
        }

    }).transform(node).head
  }
)

lazy val mdocSettings = Seq(
  mdoc                                       := (Compile / run).evaluated,
  scalacOptions                             --= Seq("-Xfatal-warnings", "-Ywarn-unused"),
  crossScalaVersions                         := Seq(scala213),
  ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(core, vulcan),
  ScalaUnidoc / unidoc / target              := (LocalRootProject / baseDirectory)
    .value / "website" / "static" / "api",
  cleanFiles           += (ScalaUnidoc / unidoc / target).value,
  docusaurusCreateSite := docusaurusCreateSite
    .dependsOn(Compile / unidoc)
    .dependsOn(ThisBuild / updateSiteVariables)
    .value,
  docusaurusPublishGhpages :=
    docusaurusPublishGhpages
      .dependsOn(Compile / unidoc)
      .dependsOn(ThisBuild / updateSiteVariables)
      .value,
  // format: off
  ScalaUnidoc / unidoc / scalacOptions ++= Seq(
    "-doc-source-url", s"https://github.com/typelevel/fs2-kafka/tree/v${(ThisBuild / latestVersion).value}€{FILE_PATH}.scala",
    "-sourcepath", (LocalRootProject / baseDirectory).value.getAbsolutePath,
    "-doc-title", "FS2 Kafka",
    "-doc-version", s"v${(ThisBuild / latestVersion).value}"
  )
  // format: on
)

lazy val buildInfoSettings = Seq(
  buildInfoPackage := "fs2.kafka.build",
  buildInfoObject  := "info",
  buildInfoKeys    := Seq[BuildInfoKey](
    scalaVersion,
    scalacOptions,
    sourceDirectory,
    ThisBuild / latestVersion,
    BuildInfoKey.map(ThisBuild / version) { case (_, v) =>
      "latestSnapshotVersion" -> v
    },
    BuildInfoKey.map(core / moduleName) { case (k, v) =>
      "core" ++ k.capitalize -> v
    },
    BuildInfoKey.map(core / crossScalaVersions) { case (k, v) =>
      "core" ++ k.capitalize -> v
    },
    BuildInfoKey.map(vulcan / moduleName) { case (k, v) =>
      "vulcan" ++ k.capitalize -> v
    },
    BuildInfoKey.map(vulcan / crossScalaVersions) { case (k, v) =>
      "vulcan" ++ k.capitalize -> v
    },
    BuildInfoKey.map(`vulcan-testkit-munit` / moduleName) { case (k, v) =>
      "vulcanTestkitMunit" ++ k.capitalize -> v
    },
    LocalRootProject / organization,
    core / crossScalaVersions,
    BuildInfoKey("fs2Version"       -> fs2Version),
    BuildInfoKey("kafkaVersion"     -> kafkaVersion),
    BuildInfoKey("vulcanVersion"    -> vulcanVersion),
    BuildInfoKey("confluentVersion" -> confluentVersion)
  )
)

lazy val metadataSettings = Seq(
  organization := "org.typelevel"
)

ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep.Sbt(List("ci")),
  WorkflowStep.Sbt(
    List("docs/run"),
    cond = Some(s"matrix.scala == '2.13'")
  )
)

ThisBuild / githubWorkflowArtifactUpload := false

ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("21"))

ThisBuild / githubWorkflowPublish := Seq(
  WorkflowStep.Sbt(
    List("tlCiRelease", "docs/docusaurusPublishGhpages"),
    env = Map(
      "GIT_DEPLOY_KEY"    -> "${{ secrets.GIT_DEPLOY_KEY }}",
      "PGP_PASSPHRASE"    -> "${{ secrets.PGP_PASSPHRASE }}",
      "PGP_SECRET"        -> "${{ secrets.PGP_SECRET }}",
      "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
      "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
    )
  )
)

lazy val publishSettings =
  metadataSettings ++ Seq(
    Test / publishArtifact := false,
    pomIncludeRepository   := (_ => false),
    homepage               := Some(url("https://typelevel.org/fs2-kafka")),
    licenses               := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    startYear              := Some(2018),
    headerLicense          := Some(
      de.heikoseeberger
        .sbtheader
        .License
        .ALv2(
          s"${startYear.value.get}",
          "OVO Energy Limited",
          HeaderLicenseStyle.SpdxSyntax
        )
    ),
    headerSources / excludeFilter := HiddenFileFilter,
    developers                    := List(
      tlGitHubDev("vlovgr", "Viktor Rudebeck").withUrl(url("https://vlovgr.se")),
      tlGitHubDev("bplommer", "Ben Plommer"),
      tlGitHubDev("LMNet", "Yuriy Badalyantc").withEmail("lmnet89@gmail.com"),
      tlGitHubDev("aartigao", "Alan Artigao").withEmail("alanartigao@gmail.com")
    )
  )

ThisBuild / mimaBinaryIssueFilters ++= {
  import com.typesafe.tools.mima.core.*
  Seq(
    ProblemFilters.exclude[Problem]("fs2.kafka.internal.*")
  )
}

lazy val noMimaSettings = Seq(mimaPreviousArtifacts := Set())

lazy val noPublishSettings =
  publishSettings ++ Seq(
    publish / skip  := true,
    publishArtifact := false
  )

ThisBuild / scalaVersion       := scala213
ThisBuild / crossScalaVersions := Seq(scala212, scala213, scala3)

lazy val scalaSettings = Seq(
  Compile / doc / scalacOptions      += "-nowarn", // workaround for https://github.com/scala/bug/issues/12007 but also suppresses genunine problems
  Compile / console / scalacOptions --= Seq("-Xlint", "-Ywarn-unused"),
  Compile / compile / scalacOptions --= {
    if (tlIsScala3.value) Seq("-Wvalue-discard", "-Wunused:privates") else Seq.empty
  },
  Compile / compile / scalacOptions ++= {
    if (tlIsScala3.value) Seq.empty else Seq("-Xsource:3")
  },
  Test / console / scalacOptions        := (Compile / console / scalacOptions).value,
  Compile / unmanagedSourceDirectories ++=
    Seq(
      baseDirectory.value / "src" / "main" / {
        if (scalaVersion.value.startsWith("2.12"))
          "scala-2.12"
        else "scala-2.13+"
      }
    ),
  Test / fork := true
)

lazy val testSettings = Seq(
  Test / logBuffered       := false,
  Test / parallelExecution := false,
  Test / testOptions       += Tests.Argument("-oDF")
)

def minorVersion(version: String): String = {
  val (major, minor) =
    CrossVersion.partialVersion(version).get
  s"$major.$minor"
}

val latestVersion = settingKey[String]("Latest stable released version")
ThisBuild / latestVersion := tlLatestVersion
  .value
  .getOrElse(
    throw new IllegalStateException("No tagged version found")
  )

val updateSiteVariables = taskKey[Unit]("Update site variables")
ThisBuild / updateSiteVariables := {
  val file =
    (LocalRootProject / baseDirectory).value / "website" / "variables.js"

  val variables =
    Map[String, String](
      "organization"         -> (LocalRootProject / organization).value,
      "coreModuleName"       -> (core / moduleName).value,
      "latestVersion"        -> latestVersion.value,
      "scalaPublishVersions" -> {
        val minorVersions = (core / crossScalaVersions).value.map(minorVersion)
        if (minorVersions.size <= 2) minorVersions.mkString(" and ")
        else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
      }
    )

  val fileHeader =
    "// Generated by sbt. Do not edit directly."

  val fileContents =
    variables
      .toList
      .sortBy { case (key, _) => key }
      .map { case (key, value) => s"  $key: '$value'" }
      .mkString(s"$fileHeader\nmodule.exports = {\n", ",\n", "\n};\n")

  IO.write(file, fileContents)
}

def versionIntroduced(v: String) = Seq(
  tlVersionIntroduced := List("2.12", "2.13", "3").map(_ -> v).toMap
)

def addCommandsAlias(name: String, values: List[String]) =
  addCommandAlias(name, values.mkString(";", ";", ""))

addCommandsAlias(
  "validate",
  List(
    "+clean",
    "+test",
    "+mimaReportBinaryIssues",
    "+scalafmtCheck",
    "scalafmtSbtCheck",
    "+headerCheck",
    "+doc",
    "docs/run"
  )
)

addCommandsAlias(
  "ci",
  List(
    "clean",
    "test",
    "mimaReportBinaryIssues",
    "scalafmtCheck",
    "scalafmtSbtCheck",
    "headerCheck",
    "doc"
  )
)
