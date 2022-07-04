package zio.cassandra.session

import wvlet.log.{ LogFormatter, LogLevel, LogSupport, Logger }
import zio.cassandra.session.cql.{ CqlSpec, ReadsSpec }
import zio.container.TestsSharedInstances
import zio.test.DefaultRunnableSpec

object IntegrationSpec extends DefaultRunnableSpec with TestsSharedInstances with LogSupport {

  Logger.setDefaultLogLevel(LogLevel.INFO)
  Logger.setDefaultFormatter(LogFormatter.SourceCodeLogFormatter)
  Logger.scanLogLevels

  override def spec =
    suite("zio-cassandra")(SessionSpec.sessionTests, CqlSpec.cqlSuite, ReadsSpec.readsTests)
      .provideCustomLayerShared(layer)
}
