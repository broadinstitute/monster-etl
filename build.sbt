// Settings to apply across the entire build.
enablePlugins(GitVersioning)
inThisBuild(
  Seq(
    organization := "org.broadinstitute",
    scalaVersion := "2.12.8",
    // Auto-format
    scalafmtConfig := (ThisBuild / baseDirectory)(_ / ".scalafmt.conf").value,
    scalafmtOnCompile := true,
    // Recommended guardrails
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-feature",
      "-target:jvm-1.8",
      "-unchecked",
      "-Xcheckinit",
      "-Xfatal-warnings",
      "-Xfuture",
      "-Xlint",
      "-Xmax-classfile-name",
      "200",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-extra-implicit",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-value-discard"
    )
  )
)

val betterMonadicForVersion = "0.3.0"

val beamVersion = "2.11.0"
val betterFilesVersion = "3.8.0"
val circeVersion = "0.11.1"
val enumeratumVersion = "1.5.13"
val logbackVersion = "1.2.3"
val scalaCsvVersion = "1.3.6"
val scioVersion = "0.7.4"
val uPickleVersion = "0.7.5"

val scalaTestVersion = "3.0.8"

// Settings to apply to all sub-projects.
// Can't be applied at the build level because of scoping rules.
val commonSettings = Seq(
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion),
  Compile / console / scalacOptions := (Compile / scalacOptions).value.filterNot(
    Set(
      "-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-unused",
      "-Ywarn-unused-import"
    )
  ),
  // Spam warnings if we end up falling back to reflection-based Coders.
  scalacOptions += "-Xmacro-settings:show-coder-fallback=true",
  Compile / doc / scalacOptions += "-no-link-warnings",
  Test / fork := true
)

lazy val `monster-etl` = project
  .in(file("."))
  .aggregate(common, encode, v2f)

lazy val common = project
  .in(file("common"))
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
      "com.spotify" %% "scio-test" % scioVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion,
    ).map(_ % Test)
  )

lazy val encode = project
  .in(file("encode"))
  .dependsOn(common)
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "com.spotify" %% "scio-extra" % scioVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Runtime,
    ),
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-test" % scioVersion
    ).map(_ % Test),
    buildInfoKeys := Seq(version),
    buildInfoPackage := "org.broadinstitute.monster.etl"
  )

lazy val v2f = project
  .in(file("v2f"))
  .dependsOn(common)
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-extra" % scioVersion,
      "com.github.tototoshi" %% "scala-csv" % scalaCsvVersion,
      "com.github.pathikrit" %% "better-files" % betterFilesVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Runtime,
    ),
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-test" % scioVersion
    ).map(_ % Test),
    buildInfoKeys := Seq(version),
    buildInfoPackage := "org.broadinstitute.monster.etl"
  )
