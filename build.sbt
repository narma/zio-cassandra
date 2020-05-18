lazy val connector =
  (project in file("connector"))
    .settings(
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
      version := "0.0.4-SNAPSHOT",
      scalaVersion := "2.13.2",
      name := "zio-cassandra",
      organization := "com.quadcode",
      libraryDependencies ++=
        Dependencies.cassandraDependencies ++
          Dependencies.zioDependencies ++
          Dependencies.testCommon,
      credentials += Credentials(Path.userHome / ".ivy2" / ".credentials_artifactory"),
      publishTo := Some("artifactory01-releases" at "https://artifactory.mobbtech.com/maven-releases"),
      scalacOptions ++= Seq(
        "-encoding",
        "utf-8",
        "-unchecked",
        "-explaintypes",
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
