package zio.cassandra.session.cql

import zio.{ Task, ZIO }
import com.github.benmanes.caffeine.cache.Caffeine
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import java.util.concurrent.{ CompletableFuture, Executor }
import com.datastax.oss.driver.api.core.CqlSession

class PreparedStatementCache(session: CqlSession, capacity: Long) {
  val cache = Caffeine
    .newBuilder()
    .maximumSize(capacity)
    .buildAsync[String, PreparedStatement](new AsyncCacheLoader[String, PreparedStatement] {
      override def asyncLoad(key: String, executor: Executor): CompletableFuture[_ <: PreparedStatement] =
        session.prepareAsync(key).toCompletableFuture()
    })

  def get(key: String): Task[PreparedStatement] = ZIO.fromCompletableFuture(cache.get(key))

  def put(key: String, st: PreparedStatement) = ZIO.attempt(cache.put(key, CompletableFuture.completedFuture(st)))
}
