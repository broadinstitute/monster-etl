val beamVersion = "2.16.0"
val betterFilesVersion = "3.8.0"
val circeVersion = "0.12.3"
val circeDerivationVersion = "0.12.0-M7"
val enumeratumVersion = "1.5.13"
val logbackVersion = "1.2.3"
val scalaCsvVersion = "1.3.6"
val scioVersion = "0.8.0-beta2"
val uPickleVersion = "0.8.0"

val scalaTestVersion = "3.0.8"

// Settings to apply to all sub-projects.
// Can't be applied at the build level because of scoping rules.
val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-Xmacro-settings:show-coder-fallback=true",
    "-language:higherKinds"
  ),
  libraryDependencies ++= Seq(
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
  ).map(_ % Runtime),
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-test" % scioVersion
  ).map(_ % s"${Test.name},${IntegrationTest.name}")
)

lazy val `monster-etl` = project
  .in(file("."))
  .aggregate(common, encode, v2f, clinvar)

lazy val common = project
  .in(file("common"))
  .enablePlugins(BasePlugin)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.lihaoyi" %% "upickle" % uPickleVersion,
      "com.spotify" %% "scio-core" % scioVersion,
      "io.circe" %% "circe-parser" % circeVersion
    ),
    libraryDependencies ++= Seq(
      "com.github.pathikrit" %% "better-files" % betterFilesVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test)
  )

lazy val encode = project
  .in(file("encode"))
  .enablePlugins(BasePlugin)
  .dependsOn(common)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "com.spotify" %% "scio-extra" % scioVersion
    )
  )

lazy val v2f = project
  .in(file("v2f"))
  .enablePlugins(BasePlugin)
  .dependsOn(common)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-extra" % scioVersion,
      "com.github.tototoshi" %% "scala-csv" % scalaCsvVersion,
      "com.github.pathikrit" %% "better-files" % betterFilesVersion
    )
  )

lazy val clinvar = project
  .in(file("clinvar"))
  .enablePlugins(BasePlugin)
  .dependsOn(common)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-derivation" % circeDerivationVersion
    )
  )
