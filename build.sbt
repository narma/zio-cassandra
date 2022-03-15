name := "zio-cassandra"

inThisBuild(
  List(
    organization := "st.alzo",
    scalaVersion := "2.13.8",
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers   := List(
      Developer("jsfwa", "jsfwa", "zubrilinandrey@gmail.com", url("https://gitlab.com/jsfwa")),
      Developer("alzo", "Sergey Rublev", "alzo@alzo.space", url("https://github.com/narma/"))
    ),
    scmInfo      := Some(ScmInfo(url("https://github.com/narma/zio-cassandra"), "git@github.com:jsfwa/zio-cassandra.git")),
    homepage     := Some(url("https://github.com/narma/zio-cassandra")),
    sonatypeCredentialHost := "s01.oss.sonatype.org"
  )
)

testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))

lazy val root =
  (project in file("."))
    .configs(IntegrationTest)
    .settings(
      Defaults.itSettings,
      libraryDependencies ++=
        Dependencies.cassandraDependencies ++
          Dependencies.zioDependencies ++
          Dependencies.commonDependencies ++
          Dependencies.testCommon ++
          Dependencies.testIntegrationDeps,
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
      Compile / console / scalacOptions --= Seq("-Wconf:any:error", "-Werror", "-Xfatal-warnings", "-Ywarn-unused"),
      Test / parallelExecution := false,
      Test / fork              := false,
      IntegrationTest / fork   := true
    )
