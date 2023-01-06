package zio.cassandra.session.cql.cache

import com.github.benmanes.caffeine.cache.{ CacheLoader, Caffeine }

private[session] class SyncCache[K, V](load_ : K => V)(capacity: Long) {

  private val cache =
    Caffeine
      .newBuilder()
      .maximumSize(capacity)
      .build[K, V] {
        new CacheLoader[K, V] {
          override def load(key: K): V = load_(key)
        }
      }

  def get(key: K): V = cache.get(key)

}
