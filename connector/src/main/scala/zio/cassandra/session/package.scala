package zio.cassandra

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql._
import com.typesafe.config.{ Config, ConfigFactory }
import zio.cassandra.config.CassandraConnectionConfig
import zio.stream.ZStream
import zio.{ Has, Task, ZIO, ZLayer, ZManaged }

package object session {

  type Session = Has[Session.CassandraSession]

  object Session {
    trait CassandraSession {
      type Done = Unit

      def underlying: CqlSession

      def close(): Task[Unit]

      def prepare(stmt: String): Task[PreparedStatement]

      def bind(stmt: String, bindValues: Seq[AnyRef]): Task[BoundStatement]

      def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement]

      def bindWithDefault(stmt: String, bindValues: Seq[AnyRef]): Task[BoundStatement]

      def bindWithDefault(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement]

      def bind(stmt: String, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement]

      def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement]

      def executeDDL(stmt: String): Task[Done]

      def executeWrite(stmt: Statement[_]): Task[Done]

      def executeWrite(stmt: String, bindValues: AnyRef*): Task[Unit]

      def executeWriteBatch(stmt: BatchStatement): Task[Done]

      def selectOne(stmt: Statement[_]): Task[Option[Row]]

      def selectOne(stmt: String, bindValues: AnyRef*): Task[Option[Row]]

      def select[R](stmt: Statement[_]): ZStream[R, Throwable, ReactiveRow]

      def select[R](stmt: String, bindValues: AnyRef*): ZStream[R, Throwable, ReactiveRow]

      def selectAll(stmt: Statement[_]): Task[Seq[ReactiveRow]]

      def selectAll(stmt: String, bindValues: AnyRef*): Task[Seq[ReactiveRow]]
    }

    /*
        Managed
     */
    def managed[R](config: Config): ZManaged[R, Throwable, CassandraSession] =
      managed(DefaultSessionProvider(config, None))

    def managed[R](cassandraConnectionConfig: CassandraConnectionConfig): ZManaged[R, Throwable, CassandraSession] =
      managed {
        for {
          config  <- ZIO(ConfigFactory.load())
          session <- DefaultSessionProvider(config, Some(cassandraConnectionConfig))
        } yield session
      }

    def managed[R](config: Config,
                   cassandraConnectionConfig: CassandraConnectionConfig): ZManaged[R, Throwable, CassandraSession] =
      managed(DefaultSessionProvider(config, Some(cassandraConnectionConfig)))

    def managed[R](cs: ZIO[R, Throwable, CassandraSession]): ZManaged[R, Throwable, CassandraSession] =
      ZManaged.make(cs) { _.close().orDie }

    /*
        Layers
     */
    def create(config: Config): ZLayer[Any, Throwable, Session] =
      ZLayer.fromManaged(managed(config))

    def create(
      cassandraConnectionConfig: CassandraConnectionConfig
    ): ZLayer[Any, Throwable, Session] =
      ZLayer.fromManaged(managed(cassandraConnectionConfig))

    def create(
      config: Config,
      cassandraConnectionConfig: CassandraConnectionConfig
    ): ZLayer[Any, Throwable, Session] =
      ZLayer.fromManaged(managed(config, cassandraConnectionConfig))
  }

}
