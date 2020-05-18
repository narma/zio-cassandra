package zio.cassandra.session

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import com.typesafe.config.Config
import zio.cassandra.config.{CassandraConnectionConfig, DriverConfigLoaderFromConfig}
import zio.interop.reactivestreams._
import zio.stream.ZStream
import zio.{Task, ZIO}

import scala.jdk.CollectionConverters._

object DefaultSessionProvider {

  def apply[R](
    config: Config,
    cassandraConnectionConfig: Option[CassandraConnectionConfig]
  ): ZIO[R, Throwable, Session.CassandraSession] =
    for {
      driverConfigLoader <- Task(DriverConfigLoaderFromConfig.fromConfig(config))
      session <- ZIO.fromCompletionStage(
                  builderFromConfig(driverConfigLoader, cassandraConnectionConfig)
                    .buildAsync()
                )
    } yield
      new Session.CassandraSession {

        val underlying: CqlSession = session

        private def executeAsync(stmt: Statement[_]): Task[AsyncResultSet] =
          ZIO.fromCompletionStage(underlying.executeAsync(stmt))

        private def executeAsync(stmt: String): Task[AsyncResultSet] =
          ZIO.fromCompletionStage(underlying.executeAsync(stmt))

        override def prepare(stmt: String): Task[PreparedStatement] =
          ZIO.fromCompletionStage(underlying.prepareAsync(stmt))

        override def bind(stmt: String, bindValues: Seq[AnyRef]): Task[BoundStatement] =
          prepare(stmt).flatMap(bind(_, bindValues))

        override def bindWithDefault(stmt: String, bindValues: Seq[AnyRef]): Task[BoundStatement] =
          prepare(stmt).flatMap(bindWithDefault(_, bindValues))

        override def bind(stmt: String, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement] =
          prepare(stmt).flatMap(bind(_, bindValues, profileName))

        override def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement] =
          Task(
            if (bindValues.isEmpty) stmt.bind()
            else stmt.bind(bindValues: _*)
          )

        override def bindWithDefault(stmt: PreparedStatement, bindValues: Seq[AnyRef]): Task[BoundStatement] =
          bind(stmt, bindValues).map(
            ps =>
              cassandraConnectionConfig
                .flatMap(_.defaultProfileName.map(ps.setExecutionProfileName))
                .fold(ps)(identity)
          )

        override def bind(stmt: PreparedStatement, bindValues: Seq[AnyRef], profileName: String): Task[BoundStatement] =
          bind(stmt, bindValues).map(_.setExecutionProfileName(profileName))

        override def selectOne(stmt: Statement[_]): Task[Option[Row]] =
          executeAsync(stmt).map(rs => Option(rs.one()))

        override def selectOne(stmt: String, bindValues: AnyRef*): Task[Option[Row]] =
          bind(stmt, bindValues).flatMap(selectOne)

        override def select[R1](stmt: Statement[_]): ZStream[R1, Throwable, ReactiveRow] =
          underlying.executeReactive(stmt).toStream(qSize = 256)

        override def select[R1](stmt: String, bindValues: AnyRef*): ZStream[R1, Throwable, ReactiveRow] =
          ZStream.fromEffect(bind(stmt, bindValues).map(select)).flatten

        override def selectAll(stmt: Statement[_]): Task[Seq[`ReactiveRow`]] =
          select(stmt).fold(List.empty[ReactiveRow])((a, b) => b +: a).map(_.reverse)

        override def selectAll(stmt: String, bindValues: AnyRef*): Task[Seq[ReactiveRow]] =
          bind(stmt, bindValues).flatMap(selectAll)

        override def executeDDL(stmt: String): Task[Unit] =
          executeAsync(stmt).map(_ => ())

        override def executeWrite(stmt: Statement[_]): Task[Done] =
          executeAsync(stmt).map(_ => ())

        override def executeWrite(stmt: String, bindValues: AnyRef*): Task[Done] =
          bind(stmt, bindValues).flatMap(executeWrite)

        override def executeWriteBatch(stmt: BatchStatement): Task[Done] =
          executeWrite(stmt)

      }

  private def builderFromConfig(loader: DriverConfigLoader,
                                cassandraConnectionConfig: Option[CassandraConnectionConfig]): CqlSessionBuilder = {
    val builder = CqlSession
      .builder()
      .withConfigLoader(loader)

    cassandraConnectionConfig.fold(builder) { cfg =>
      for {
        u <- cfg.username
        p <- cfg.password
      } builder.withAuthCredentials(u, p)
      builder.addContactPoints(cfg.contactPoints.asJava)
    }
  }

}
