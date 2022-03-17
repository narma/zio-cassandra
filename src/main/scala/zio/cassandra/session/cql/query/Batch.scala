package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.{BatchStatementBuilder, BatchType}
import zio.cassandra.session.Session
import zio.{Has, RIO, ZIO}

class Batch(batchStatementBuilder: BatchStatementBuilder) {
  def add(queries: Seq[Query[_]]) = new Batch(batchStatementBuilder.addStatements(queries.map(_.statement): _*))

  def execute: RIO[Has[Session], Boolean] =
    ZIO.accessM[Has[Session]] { session =>
      session.get.execute(batchStatementBuilder.build()).map(_.wasApplied)
    }

  def config(config: BatchStatementBuilder => BatchStatementBuilder): Batch =
    new Batch(config(batchStatementBuilder))
}

object Batch {
  def logged   = new Batch(new BatchStatementBuilder(BatchType.LOGGED))
  def unlogged = new Batch(new BatchStatementBuilder(BatchType.UNLOGGED))
}
