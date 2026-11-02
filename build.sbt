val avroVersion                = "1.12.2"
val catsEffectVersion          = "3.7.1"
val catsVersion                = "2.13.0"
val confluentVersion           = "8.4.0"
val disciplineVersion          = "2.3.0"
val fs2Version                 = "3.14.0"
val kafkaVersion               = "8.4.0-ce"
val logbackVersion             = "1.6.6"
val munitVersion               = "1.3.6"
val slf4jNopVersion            = "2.0.20"
val slf4jVersion               = "1.7.36"
val testcontainersScalaVersion = "0.44.1"
val vulcanVersion              = "1.14.1"

val scala212 = "2.12.21"
val scala213 = "2.13.18"
val scala3   = "3.3.9"

inThisBuild(
  Seq(
    crossScalaVersions := Seq(scala212, scala213, scala3),
    developers         := List(
      tlGitHubDev("vlovgr", "Viktor Rudebeck").withUrl(url("https://vlovgr.se")),
      tlGitHubDev("bplommer", "Ben Plommer"),
      tlGitHubDev("LMNet", "Yuriy Badalyantc").withEmail("lmnet89@gmail.com"),
      tlGitHubDev("aartigao", "Alan Artigao").withEmail("alanartigao@gmail.com")
    ),
    githubWorkflowJavaVersions := Seq(JavaSpec.temurin("21")),
    mergifyLabelPaths          := Map.empty,
    mergifyRequiredJobs        += "site",
    mergifyStewardConfig ~= (_.map(_.withAuthor("typelevel-steward[bot]"))),
    organization            := "org.typelevel",
    resolvers               += "confluent".at("https://packages.confluent.io/maven/"),
    scalaVersion            := scala3,
    startYear               := Some(2018),
    tlBaseVersion           := "4.1",
    tlSiteKeepFiles         := false,
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core.*
      // scalafmt: { maxColumn = 200 }
      Seq(
        ProblemFilters.exclude[Problem]("fs2.kafka.internal.*"),
        ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.KafkaConsumer.settings"),
        ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.ConsumerSettings.withMaxParallelism"),
        ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.ConsumerSettings.maxParallelism"),
        ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.ConsumerSettings.commitOnRevoke"),
        ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.ConsumerSettings.withCommitOnRevoke"),
        ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.ConsumerSettings#ConsumerSettingsImpl.copy"),
        ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.ConsumerSettings#ConsumerSettingsImpl.this"),
        ProblemFilters.exclude[DirectMissingMethodProblem]("fs2.kafka.ConsumerSettings#ConsumerSettingsImpl.apply"),
        ProblemFilters.exclude[MissingClassProblem]("fs2.kafka.KafkaConsumer$AssignmentSignals"),
        ProblemFilters.exclude[MissingClassProblem]("fs2.kafka.KafkaConsumer$AssignmentSignals$"),
        ProblemFilters.exclude[MissingClassProblem]("fs2.kafka.KafkaConsumer$AssignmentSignals$EagerSignals"),
        ProblemFilters.exclude[MissingClassProblem]("fs2.kafka.KafkaConsumer$AssignmentSignals$EagerSignals$"),
        ProblemFilters.exclude[MissingClassProblem]("fs2.kafka.KafkaConsumer$AssignmentSignals$GracefulSignals"),
        ProblemFilters.exclude[MissingClassProblem]("fs2.kafka.KafkaConsumer$AssignmentSignals$GracefulSignals$"),
        ProblemFilters.exclude[ReversedMissingMethodProblem]("fs2.kafka.KafkaCommitter.source")
      )
      // scalafmt: {}
    }
  )
)

lazy val root = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, vulcan, `vulcan-testkit-munit`, unidocs)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    name                 := "fs2-kafka",
    libraryDependencies ++= Seq(
      "co.fs2"          %% "fs2-core"           % fs2Version,
      "org.apache.kafka" % "kafka-clients"      % kafkaVersion,
      "org.slf4j"        % "slf4j-api"          % slf4jVersion,
      "org.typelevel"   %% "cats-core"          % catsVersion,
      "org.typelevel"   %% "cats-effect-kernel" % catsEffectVersion,
      "org.typelevel"   %% "cats-effect-std"    % catsEffectVersion,
      "org.typelevel"   %% "cats-effect"        % catsEffectVersion,
      "org.typelevel"   %% "cats-kernel"        % catsVersion
    ),
    headerSettings,
    testSettings
  )

lazy val vulcan = project
  .in(file("modules/vulcan"))
  .dependsOn(core)
  .settings(
    name                 := "fs2-kafka-vulcan",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "vulcan"                       % vulcanVersion,
      "io.confluent"     % "kafka-avro-serializer"        % confluentVersion,
      "io.confluent"     % "kafka-schema-registry-client" % confluentVersion,
      "io.confluent"     % "kafka-schema-serializer"      % confluentVersion,
      "org.apache.avro"  % "avro"                         % avroVersion,
      "org.typelevel"   %% "cats-core"                    % catsVersion,
      "org.typelevel"   %% "cats-effect-kernel"           % catsEffectVersion,
      "org.typelevel"   %% "cats-effect"                  % catsEffectVersion
    ),
    headerSettings,
    testSettings
  )

lazy val `vulcan-testkit-munit` = project
  .in(file("modules/vulcan-testkit-munit"))
  .dependsOn(vulcan)
  .settings(
    name                 := "fs2-kafka-vulcan-testkit-munit",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "vulcan"                       % vulcanVersion,
      "io.confluent"     % "kafka-schema-registry-client" % confluentVersion,
      "org.apache.avro"  % "avro"                         % avroVersion,
      "org.scalameta"   %% "munit-diff"                   % munitVersion,
      "org.scalameta"   %% "munit"                        % munitVersion,
      "org.typelevel"   %% "cats-effect"                  % catsEffectVersion
    ),
    headerSettings,
    testSettings,
    apiURL              := None, // not part of the unified API docs in fs2-kafka-docs
    tlVersionIntroduced := List("2.12", "2.13", "3").map(_ -> "2.2.0").toMap
  )

lazy val docs = project
  .in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .dependsOn(core, vulcan, `vulcan-testkit-munit`)
  .settings {
    import laika.ast.Image
    import laika.ast.InlineSVGIcon
    import laika.ast.LengthUnit.em
    import laika.ast.LengthUnit.px
    import laika.ast.Path.Root
    import laika.ast.Styles
    import laika.config.ApiLinks
    import laika.config.LinkConfig
    import laika.helium.Helium
    import laika.helium.config.ButtonLink
    import laika.helium.config.ColorQuintet
    import laika.helium.config.Favicon
    import laika.helium.config.HeliumIcon
    import laika.helium.config.IconLink
    import laika.helium.config.LinkGroup
    import laika.helium.config.LinkPanel
    import laika.helium.config.ReleaseInfo
    import laika.helium.config.TextLink
    import laika.theme.config.Color
    import org.typelevel.sbt.site.GenericSiteSettings

    Seq(
      crossScalaVersions  := Seq(scala3),
      libraryDependencies += "org.slf4j" % "slf4j-nop" % slf4jNopVersion % Runtime,
      scalacOptions ~= (_.filterNot(_.startsWith("-Wunused"))),
      Compile / packageDoc / mappings := (unidocs / Compile / packageDoc / mappings)
        .value
        .map { case (file, path) =>
          file -> path.replace('\\', '/')
        },
      laikaConfig ~= (_.withConfigValue(LinkConfig.empty.addApiLinks(ApiLinks("/api/")))),
      laikaIncludeAPI := true,
      tlSiteApiUrl    := None,
      tlSitePreview   := tlSitePreview.dependsOn(laikaGenerateAPI).value,
      mdocVariables  ++= Map(
        "CONFLUENT_VERSION"                -> confluentVersion,
        "CORE_CROSS_SCALA_VERSIONS"        -> crossScalaVersionsOf(core).value,
        "CORE_MODULE_NAME"                 -> (core / moduleName).value,
        "FS2_VERSION"                      -> fs2Version,
        "KAFKA_API_BASE_URL"               -> kafkaApiBaseUrl(kafkaVersion),
        "KAFKA_VERSION"                    -> kafkaVersion,
        "MAJOR_VERSION"                    -> majorVersion(version.value),
        "ORGANIZATION"                     -> (ThisBuild / organization).value,
        "SCALA_DOCS_VERSION"               -> scalaVersionOf(scalaVersion.value),
        "SCALA_PUBLISH_VERSIONS"           -> crossScalaVersionsOf(core).value,
        "VULCAN_CROSS_SCALA_VERSIONS"      -> crossScalaVersionsOf(vulcan).value,
        "VULCAN_MODULE_NAME"               -> (vulcan / moduleName).value,
        "VULCAN_TESTKIT_MUNIT_MODULE_NAME" -> (`vulcan-testkit-munit` / moduleName).value,
        "VULCAN_VERSION"                   -> vulcanVersion
      ),
      tlSiteHelium := {
        val gitHubUrl = "https://github.com/typelevel/fs2-kafka"

        val projectLinks = List(
          IconLink.internal(Root / "api" / "fs2" / "kafka.html", HeliumIcon.api),
          IconLink.external(gitHubUrl, HeliumIcon.github),
          IconLink.external("https://discord.gg/fjqZMuqzAp", HeliumIcon.chat)
        )

        Helium
          .defaults
          .extendWith(GenericSiteSettings.themeExtensions.value)
          .site
          .metadata(
            title = Some("FS2 Kafka · Functional Kafka for Scala"),
            description =
              Some("Functional streams for Kafka with FS2 and the official Apache Kafka client."),
            authors = developers.value.map(_.name),
            language = Some("en"),
            version = Some(version.value)
          )
          .site
          .themeColors(
            primary = Color.hex("122932"),
            secondary = Color.hex("22685e"),
            primaryMedium = Color.hex("cdeae4"),
            primaryLight = Color.hex("f1f8f6"),
            text = Color.hex("24292e"),
            background = Color.hex("ffffff"),
            bgGradient = (Color.hex("122932"), Color.hex("15333a"))
          )
          .site
          .syntaxHighlightingColors(
            base = ColorQuintet(
              Color.hex("f8f8f8"), // background
              Color.hex("999988"), // comments
              Color.hex("333333"), // punctuation
              Color.hex("333333"), // identifiers
              Color.hex("333333")  // unclassified
            ),
            wheel = ColorQuintet(
              Color.hex("999999"), // substitutions, annotations
              Color.hex("333333"), // keywords, escape sequences
              Color.hex("990000"), // declaration names
              Color.hex("dd1144"), // literals
              Color.hex("445588")  // types
            )
          )
          .site
          .clearFontResources
          .site
          .fontSizes(
            body = px(16),
            code = em(0.85),
            title = px(32),
            header2 = px(24),
            header3 = px(20),
            header4 = px(16),
            small = px(12)
          )
          .site
          .darkMode
          .disabled
          .site
          .layout(topBarHeight = px(50))
          .site
          .topNavigationBar(
            homeLink = IconLink.internal(
              Root / "README",
              InlineSVGIcon(IO.read(file("docs/img/logo.svg"))),
              text = Some("FS2 Kafka"),
              options = Styles("home-link")
            ),
            navLinks = projectLinks,
            highContrast = true
          )
          .site
          .internalCSS(Root / "css")
          .site
          .favIcons(
            Favicon.external(
              "https://typelevel.org/fs2-kafka/img/logo.png",
              "180x180",
              "image/png"
            ),
            Favicon.internal(Root / "img" / "logo.svg")
          )
          .site
          .landingPage(
            logo = Some(
              Image.internal(Root / "img" / "logo.svg", height = Some(px(150)), alt = Some(""))
            ),
            title = Some("FS2 Kafka"),
            subtitle = Some("Functional Kafka for Scala"),
            latestReleases = tlLatestVersion.value.toList.map(ReleaseInfo("Latest Release", _)),
            license = Some("Apache 2.0"),
            titleLinks = Seq(
              ButtonLink.internal(Root / "overview.md", "Documentation"),
              LinkGroup.create(projectLinks.head, projectLinks.tail: _*)
            ),
            linkPanel = Some(
              LinkPanel(
                "Documentation",
                TextLink.internal(Root / "overview.md", "Overview"),
                TextLink.internal(Root / "quick-example.md", "Quick Example"),
                TextLink.internal(Root / "consumers.md", "Consumers"),
                TextLink.internal(Root / "producers.md", "Producers")
              )
            )
          )
          .site
          .footer()
          .site
          .pageNavigation(
            sourceBaseURL = Some("https://github.com/typelevel/fs2-kafka/tree/main/docs")
          )
      }
    )
  }

lazy val unidocs = project
  .enablePlugins(TypelevelUnidocPlugin)
  .settings(
    name := "fs2-kafka-docs",
    headerSettings,
    crossScalaVersions                         := Seq(scala3),
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(core, vulcan),
    ScalaUnidoc / unidoc / apiMappings         := {
      val docsApiUrl = apiURL.value.map(_.toString)
      (ScalaUnidoc / unidoc / apiMappings)
        .value
        .filterNot { case (_, url) =>
          docsApiUrl.contains(url.toString)
        }
    },
    ScalaUnidoc / unidoc / scalacOptions := {
      val docVersion = tlLatestVersion.value.getOrElse(version.value)
      val logo       = (LocalRootProject / baseDirectory).value / "docs" / "img" / "logo.svg"
      val options    = (ScalaUnidoc / unidoc / scalacOptions)
        .value
        .filter {
          case "-Werror" | "-Wvalue-discard"                   => false
          case option if option.startsWith("-Ykind-projector") => false
          case _                                               => true
        }
      val projectOptions = withOption(options, "-project", "FS2 Kafka")
      withOption(projectOptions, "-project-version", s"v$docVersion") ++ Seq(
        "-comment-syntax:wiki",
        "-doc-canonical-base-url:https://typelevel.org/fs2-kafka/api/",
        "-groups",
        "-project-footer:Licensed under Apache License, Version 2.0.",
        s"-project-logo:${logo.getAbsolutePath}",
        "-social-links:github::https://github.com/typelevel/fs2-kafka",
        "-social-links:discord::https://discord.gg/fjqZMuqzAp",
        s"-source-links:library=github://scala/scala3/${scalaVersion.value}#library"
      )
    }
  )

lazy val headerSettings = Seq(
  headerLicense := Some(
    HeaderLicense.ALv2(
      startYear.value.get.toString,
      "OVO Energy Limited",
      HeaderLicenseStyle.SpdxSyntax
    )
  )
)

lazy val testSettings = Seq(
  libraryDependencies ++= Seq(
    "com.dimafeng"  %% "testcontainers-scala-scalatest" % testcontainersScalaVersion,
    "com.dimafeng"  %% "testcontainers-scala-kafka"     % testcontainersScalaVersion,
    "org.typelevel" %% "discipline-scalatest"           % disciplineVersion,
    "org.typelevel" %% "cats-effect-laws"               % catsEffectVersion,
    "org.typelevel" %% "cats-effect-testkit"            % catsEffectVersion,
    "ch.qos.logback" % "logback-classic"                % logbackVersion
  ).map(_ % Test),
  Test / fork              := true,
  Test / logBuffered       := false,
  Test / parallelExecution := false,
  Test / testOptions       += Tests.Argument("-oDF")
)

def kafkaApiBaseUrl(kafkaVersion: String): String = {
  val (major, minor) =
    CrossVersion.partialVersion(kafkaVersion).get
  s"https://kafka.apache.org/$major$minor/javadoc"
}

def majorVersion(version: String): String =
  version.split('.').head

def withOption(options: Seq[String], name: String, value: String): Seq[String] =
  options.indexOf(name) match {
    case -1    => options ++ Seq(name, value)
    case index => options.updated(index + 1, value)
  }

def scalaVersionOf(version: String): String = {
  if (version.contains("-")) version
  else {
    val (major, minor) =
      CrossVersion.partialVersion(version).get
    s"$major.$minor"
  }
}

def crossScalaVersionsOf(project: Project): Def.Initialize[String] =
  Def.setting {
    val scalaVersions = (project / crossScalaVersions).value.map(scalaVersionOf)
    if (scalaVersions.size <= 2) scalaVersions.mkString(" and ")
    else scalaVersions.init.mkString(", ") ++ " and " ++ scalaVersions.last
  }
