package fs2.kafka.docs

import java.nio.file.{FileSystems, Path}

import fs2.kafka.build.info.*

object Main {

  def sourceDirectoryPath(rest: String*): Path =
    FileSystems.getDefault.getPath(sourceDirectory.getAbsolutePath, rest*)

  def minorVersion(version: String): String = version.split('.') match {
    case Array(major, minor, _) => s"$major.$minor"
    case _                      => throw new IllegalArgumentException(s"Invalid major/minor: $version")
  }

  def majorVersion(version: String): String = version.split('.') match {
    case Array(major, _, _) => major
    case _                  => throw new IllegalArgumentException(s"Invalid major: $version")
  }

  def minorVersionsString(versions: Seq[String]): String = {
    val minorVersions = versions.map(minorVersion)
    if (minorVersions.size <= 2) minorVersions.mkString(" and ")
    else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
  }

  def main(args: Array[String]): Unit = {
    val scalaMinorVersion = minorVersion(scalaVersion)
    val kafkaDocsVersion  = minorVersion(kafkaVersion).filter(_ != '.')

    val settings = mdoc
      .MainSettings()
      .withSiteVariables {
        Map(
          "ORGANIZATION"                     -> organization,
          "CORE_MODULE_NAME"                 -> coreModuleName,
          "CORE_CROSS_SCALA_VERSIONS"        -> minorVersionsString(coreCrossScalaVersions),
          "OTEL4S_TRACE_MODULE_NAME"         -> otel4sModuleName,
          "VULCAN_MODULE_NAME"               -> vulcanModuleName,
          "VULCAN_CROSS_SCALA_VERSIONS"      -> minorVersionsString(vulcanCrossScalaVersions),
          "VULCAN_TESTKIT_MUNIT_MODULE_NAME" -> vulcanTestkitMunitModuleName,
          "LATEST_VERSION"                   -> latestVersion,
          "LATEST_SNAPSHOT_VERSION"          -> latestSnapshotVersion,
          "LATEST_MAJOR_VERSION"             -> majorVersion(latestVersion),
          "DOCS_SCALA_MINOR_VERSION"         -> scalaMinorVersion,
          "FS2_VERSION"                      -> fs2Version,
          "KAFKA_VERSION"                    -> kafkaVersion,
          "VULCAN_VERSION"                   -> vulcanVersion,
          "CONFLUENT_VERSION"                -> confluentVersion,
          "KAFKA_DOCS_VERSION"               -> kafkaDocsVersion,
          "OTEL4S_TRACE_MODULE_NAME"         -> otel4sVersion,
          "SCALA_PUBLISH_VERSIONS"           -> minorVersionsString(crossScalaVersions),
          "API_BASE_URL"                     -> s"/fs2-kafka/api/fs2/kafka",
          "KAFKA_API_BASE_URL"               -> s"https://kafka.apache.org/$kafkaDocsVersion/javadoc"
        )
      }
      .withScalacOptions(scalacOptions.mkString(" "))
      .withIn(sourceDirectoryPath("main", "mdoc"))
      .withArgs(args.toList)

    val exitCode = mdoc.Main.process(settings)
    if (exitCode != 0) sys.exit(exitCode)
  }

}
