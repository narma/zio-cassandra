package zio.cassandra

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql._
import com.typesafe.config.{Config, ConfigFactory}
import zio.cassandra.config.CassandraConnectionConfig
import zio.stream.ZStream
import zio.{Has, Task, ZIO, ZLayer}

package object session {

  type Session = Has[Session.CassandraSession]

  object Session {
    trait CassandraSession {
      type Done = Unit

      def underlying : CqlSession

      def prepare(stmt: String) : Task[PreparedStatement]

      def bind(stmt: String, bindValues: Seq[AnyRef]): Task[BoundStatement]

      def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement]

      def bindWithDefault (stmt: String, bindValues: Seq[AnyRef]): Task[BoundStatement]

      def bindWithDefault(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement]

      def bind(stmt: String, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement]

      def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement]

      def executeDDL(stmt: String) : Task[Done]

      def executeWrite(stmt: Statement[_]) : Task[Done]

      def executeWrite(stmt: String, bindValues: AnyRef*) : Task[Unit]

      def executeWriteBatch(stmt: BatchStatement) : Task[Done]

      def selectOne(stmt: Statement[_]) : Task[Option[Row]]

      def selectOne(stmt: String, bindValues: AnyRef*): Task[Option[Row]]

      def select[R](stmt: Statement[_]) : ZStream[R, Throwable, ReactiveRow]

      def select[R](stmt: String, bindValues: AnyRef*) : ZStream[R, Throwable, ReactiveRow]

      def selectAll(stmt: Statement[_]): Task[Seq[ReactiveRow]]

      def selectAll(stmt: String, bindValues: AnyRef*): Task[Seq[ReactiveRow]]
    }

    def create(config: Config): ZLayer[Any, Throwable, Session] =
      ZLayer.fromEffect(
        DefaultSessionProvider.apply(config, None)
      )

    def create(
      config: Config,
      cassandraConnectionConfig: CassandraConnectionConfig
    ): ZLayer[Any, Throwable, Session] =
      ZLayer.fromEffect(DefaultSessionProvider.apply(config, Some(cassandraConnectionConfig)))

    def create(
      cassandraConnectionConfig: CassandraConnectionConfig
    ): ZLayer[Any, Throwable, Session] =
      ZLayer.fromEffect(
        ZIO(ConfigFactory.load())
          .flatMap(DefaultSessionProvider.apply(_, Some(cassandraConnectionConfig)))
      )

  }

}
