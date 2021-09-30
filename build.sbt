name := "zio-cassandra"

inThisBuild(
  List(
    organization := "io.github.jsfwa",
    scalaVersion := "2.13.6",
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("jsfwa", "jsfwa", "zubrilinandrey@gmail.com", url("https://gitlab.com/jsfwa")),
      Developer("alzo", "Sergey Rublev", "alzo@alzo.space", url("https://github.com/narma/"))
    ),
    scmInfo := Some(ScmInfo(url("https://github.com/jsfwa/zio-cassandra"), "git@github.com:jsfwa/zio-cassandra.git")),
    homepage := Some(url("https://github.com/jsfwa/zio-cassandra"))
  )
)

testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))

lazy val root =
  (project in file("."))
    .settings(
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
      Test / parallelExecution := false,
      Test / fork := true
    )

