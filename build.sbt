lazy val connector =
  (project in file("connector"))
    .settings(
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
      scalaVersion := "2.13.3",
      organization := "io.github.jsfwa",
      homepage := Some(url("https://github.com/jsfwa/zio-cassandra")),
      scmInfo := Some(ScmInfo(url("https://github.com/jsfwa/zio-cassandra"), "git@github.com:jsfwa/zio-cassandra.git")),
      developers := List(
        Developer("jsfwa", "jsfwa", "zubrilinandrey@gmail.com", url("https://gitlab.com/jsfwa")),
        Developer("alzo", "Sergey Rublev", "alzo@alzo.space", url("https://github.com/narma/"))
      ),
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
      publishArtifact in GlobalScope in Test := false,
      parallelExecution in Test := false,
      fork in Test := true
    )

lazy val root = (project in file("."))
  .aggregate(connector)
  .settings(
    skip in publish := true
  )
