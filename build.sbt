lazy val connector =
  (project in file("connector"))
    .settings(
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
      scalaVersion := "2.13.3",
      organization := "io.github.jsfwa",
      licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
      name := "zio-cassandra",
      libraryDependencies ++=
        Dependencies.cassandraDependencies ++
          Dependencies.zioDependencies ++
          Dependencies.testCommon,
      scalacOptions ++= Seq(
        "-encoding",
        "utf-8",
        "-unchecked",
        "-explaintypes",
        "-Yrangepos",
        "-Ywarn-unused",
        "-Ymacro-annotations",
        "-deprecation",
        "-language:higherKinds",
        "-language:implicitConversions",
        "-Xlint:-serial",
        "-Xfatal-warnings",
        "-Werror",
        "-Wconf:any:error"
      ),
      sources in (Compile, doc) := Seq.empty,
      publishArtifact in GlobalScope in Test := false,
      publishArtifact in (Compile, packageDoc) := false,
      parallelExecution in Test := false
    )

lazy val root = (project in file("."))
  .aggregate(connector)
  .settings(
    skip in publish := true
  )
