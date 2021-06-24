import sbt._

object Dependencies {

  val cassandraDriverVersion = "4.8.0"

  val zioVersion = "1.0.1"

  val javaStreamsInterop = "1.0.3.5"

  val testContainersVersion = "0.38.8"

  val cassandraDependencies = Seq(
    "com.datastax.oss" % "java-driver-core" % cassandraDriverVersion
  )

  val zioDependencies = Seq(
    "dev.zio" %% "zio"                         % zioVersion,
    "dev.zio" %% "zio-streams"                 % zioVersion,
    "dev.zio" %% "zio-interop-reactivestreams" % javaStreamsInterop
  )

  val testCommon = Seq(
    "org.wvlet.airframe" %% "airframe-log"                   % "20.5.1",
    "org.slf4j"          % "slf4j-jdk14"                     % "1.7.21",
    "dev.zio"            %% "zio-test"                       % zioVersion,
    "dev.zio"            %% "zio-test-sbt"                   % zioVersion,
    "com.dimafeng"       %% "testcontainers-scala-core"      % testContainersVersion,
    "com.dimafeng"       %% "testcontainers-scala-cassandra" % testContainersVersion
  ).map(_ % Test)

}
