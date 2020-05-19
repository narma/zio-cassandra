import sbt._

object Dependencies {

  val cassandraDriverVersion = "4.6.1"

  val zioVersion = "1.0.0-RC19"

  val javaStreamsInterop = "1.0.3.5-RC8"

  val cassandraDependencies = Seq(
    "com.datastax.oss" % "java-driver-core" % cassandraDriverVersion
  )

  val zioDependencies = Seq(
    "dev.zio" %% "zio"                         % zioVersion,
    "dev.zio" %% "zio-streams"                 % zioVersion,
    "dev.zio" %% "zio-interop-reactivestreams" % javaStreamsInterop
  )

  val testCommon = Seq(
    "com.github.nosan"   % "embedded-cassandra" % "3.0.3",
    "org.wvlet.airframe" %% "airframe-log"      % "20.5.1",
    "org.slf4j"          % "slf4j-jdk14"        % "1.7.21",
    "dev.zio"            %% "zio-test"          % zioVersion,
    "dev.zio"            %% "zio-test-sbt"      % zioVersion
  ).map(_ % Test)

}
