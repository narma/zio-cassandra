package zio.cassandra.session.cql.query

import com.datastax.oss.driver.api.core.cql.{ BatchStatement, BatchStatementBuilder, BatchType }
import zio.cassandra.session.Session
import zio.{ RIO, ZIO }

class Batch(batchStatementBuilder: BatchStatementBuilder) {

  def add(queries: Seq[PreparedQuery[_]]): this.type = {
    batchStatementBuilder.addStatements(queries.map(_.statement): _*)
    this
  }

  def build: BatchStatement = batchStatementBuilder.build()

  def execute: RIO[Session, Boolean] =
    ZIO.serviceWithZIO(_.execute(this))

  def config(config: BatchStatementBuilder => BatchStatementBuilder): Batch =
    new Batch(config(batchStatementBuilder))
}

object Batch {
  def logged   = new Batch(new BatchStatementBuilder(BatchType.LOGGED))
  def unlogged = new Batch(new BatchStatementBuilder(BatchType.UNLOGGED))
}
