name := "zio-cassandra"
import Dependencies.V
import sbt.librarymanagement.CrossVersion
import BuildHelper._

inThisBuild(
  List(
    organization           := "st.alzo",
    scalaVersion           := V.scala2,
    crossScalaVersions     := V.allScala,
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0")),
    developers             := List(
      Developer("jsfwa", "jsfwa", "zubrilinandrey@gmail.com", url("https://gitlab.com/jsfwa")),
      Developer("alzo", "Sergey Rublev", "alzo@alzo.space", url("https://github.com/narma/"))
    ),
    scmInfo                := Some(ScmInfo(url("https://github.com/narma/zio-cassandra"), "git@github.com:jsfwa/zio-cassandra.git")),
    homepage               := Some(url("https://github.com/narma/zio-cassandra")),
    sonatypeCredentialHost := "s01.oss.sonatype.org"
  )
)

testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))

def shapelessDeps(scalaVersion: String) = CrossVersion.partialVersion(scalaVersion) match {
  case Some((3, _)) => Nil
  case _            => Seq("com.chuusai" %% "shapeless" % "2.3.9")
}

lazy val root =
  (project in file("."))
    .configs(IntegrationTest)
    .settings(stdSettings("zio-cassandra"))
    .settings(
      Defaults.itSettings,
      libraryDependencies ++=
        Dependencies.cassandraDependencies ++
          Dependencies.zioDependencies ++
          shapelessDeps(scalaVersion.value) ++
          Dependencies.testCommon ++
          Dependencies.testIntegrationDeps,
      Test / parallelExecution := false,
      Test / fork              := false,
      IntegrationTest / fork   := true
    )
