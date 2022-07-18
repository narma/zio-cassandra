import sbt.Keys._
import sbt.librarymanagement.CrossVersion
import sbt.{Compile, Test}

object BuildHelper {

  private val stdOptions = Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked"
  ) ++ {
    if (isCI) {
      Seq("-Xfatal-warnings")
    } else {
      Nil
    }
  }

  private val std2xOptions = Seq(
    "-language:higherKinds",
    "-language:implicitConversions",
    "-explaintypes",
    "-Yrangepos",
    "-Ymacro-annotations",
    "-Xlint:_,-missing-interpolator,-type-parameter-shadow,-infer-any,-byname-implicit",
    "-Xlint:stars-align",
    "-Xlint:strict-unsealed-patmat",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Ywarn-unused:params,imports,-implicits",
    "-Ypatmat-exhaust-depth",
    "40",
    "-Xfatal-warnings",
    "-Werror",
    "-Wconf:any:error"
    // "-Xsource:3"
  )

  def optimizerOptions(optimize: Boolean): Seq[String] =
    if (optimize)
      Seq(
        "-opt:l:inline",
        "-opt:l:method",
        "-opt-inline-from:zio.cassandra.**"
      )
    else Nil

  def extraOptions(scalaVersion: String, optimize: Boolean) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((3, _))  =>
        Seq(
          "-language:implicitConversions",
          "-explain-types",
          "-Xmax-inlines",
          "40",
          "-Ycheck-all-patmat",
          "-noindent"
        )
      case Some((2, 13)) =>
        std2xOptions ++ optimizerOptions(optimize)
      case _             => Seq.empty
    }

  private def getEnv(name: String): Option[String] = Option(System.getenv(name))

  private def envExist(name: String): Boolean =
    getEnv(name).exists(_.nonEmpty)

  private def isCI: Boolean =
    envExist("CI")

  def stdSettings(projectName: String) = Seq(
    name                 := s"$projectName",
    scalacOptions ++= stdOptions ++ extraOptions(
      scalaVersion.value,
      optimize = !isSnapshot.value || isCI
    ),
    Compile / console / scalacOptions ~= (_.filterNot(Set("-Wconf:any:error", "-Werror", "-Xfatal-warnings", "-Ywarn-unused").contains)),
    Test / scalacOptions := (Compile / console / scalacOptions).value
  )

}
