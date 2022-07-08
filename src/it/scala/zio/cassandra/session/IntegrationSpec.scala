package zio.cassandra.session

import wvlet.log.{ LogFormatter, LogLevel, LogSupport, Logger }
import zio.Scope
import zio.cassandra.session.cql.CqlSpec
import zio.cassandra.session.cql.codec.{ ReadsSpec, UdtReadsSpec }
import zio.container.TestsSharedInstances
import zio.test.ZIOSpecDefault

object IntegrationSpec extends ZIOSpecDefault with TestsSharedInstances with LogSupport {

  Logger.setDefaultLogLevel(LogLevel.INFO)
  Logger.setDefaultFormatter(LogFormatter.SourceCodeLogFormatter)
  Logger.scanLogLevels

  override def spec =
    suite("zio-cassandra")(SessionSpec.sessionTests, CqlSpec.cqlSuite, ReadsSpec.readsTests, UdtReadsSpec.readsTests)
      .provideLayerShared(Scope.default >+> layer)
}
