package zio.cassandra.session.cql.cache

import com.github.benmanes.caffeine.cache.{ AsyncCacheLoader, Caffeine }
import zio.{ Task, ZIO }

import java.util.concurrent.{ CompletableFuture, Executor }

private[session] class AsyncCache[K, V](load: K => CompletableFuture[V])(capacity: Long) {

  private val cache =
    Caffeine
      .newBuilder()
      .maximumSize(capacity)
      .buildAsync[K, V] {
        new AsyncCacheLoader[K, V] {
          override def asyncLoad(key: K, executor: Executor): CompletableFuture[V] = load(key)
        }
      }

  def get(key: K): Task[V] = ZIO.fromCompletableFuture(cache.get(key))

}
