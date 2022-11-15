package zio.cassandra.session

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.dimafeng.testcontainers.CassandraContainer
import zio.cassandra.session.cql.CqlStringContext
import zio.cassandra.session.cql.unsafe.lift
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Task, ZIO }

import java.net.InetSocketAddress
import java.util.UUID

object SessionSpec extends CassandraSpecUtils {

  val sessionTests = suite("Cassandra session")(
    testM("Session.make must be referentially transparent") {
      val st = SimpleStatement.newInstance(s"select cluster_name from system.local")
      for {
        container   <- ZIO.service[CassandraContainer]
        testSession <-
          Task(
            Session.make(
              CqlSession
                .builder()
                .addContactPoint(
                  InetSocketAddress.createUnresolved(container.containerIpAddress, container.mappedPort(9042))
                )
                .withLocalDatacenter("datacenter1")
            )
          )
        r1          <- testSession.use(session => session.selectFirst(st).map(_.map(_.getString(0))))
        r2          <- testSession.use(session => session.selectFirst(st).map(_.map(_.getString(0))))
      } yield assertTrue(r1 == r2)
    },
    testM("prepare should return PreparedStatement") {
      for {
        session <- ZIO.service[Session]
        st      <- session.prepare(s"select data FROM $keyspace.test_data WHERE id = :id")
      } yield assertTrue(st.getQuery == s"select data FROM $keyspace.test_data WHERE id = :id")
    },
    testM("prepare should return error on invalid request") {
      for {
        session <- ZIO.service[Session]
        result  <- session.prepare(s"select column404 FROM $keyspace.test_data WHERE id = :id").either
      } yield assert(result)(isLeft(hasMessage(containsString("Undefined column name column404")))) &&
        assert(result)(isLeft(isSubtype[InvalidQueryException](Assertion.anything)))
    },
    testM("select should return prepared data") {
      for {
        session <- ZIO.service[Session]
        results <- session
                     .select(s"select data FROM $keyspace.test_data WHERE id IN (1,2,3)")
                     .map(_.getString(0))
                     .runCollect
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    testM("select interpolated query (cqlConst) should return prepared data") {
      for {
        session <- ZIO.service[Session]
        results <- session
                     .select(cqlConst"select data FROM $keyspace.test_data WHERE id IN (1,2,3)".as[String])
                     .runCollect
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    testM("select interpolated query (cql) should return prepared data") {
      for {
        session <- ZIO.service[Session]
        ids      = List(1L, 2L, 3L)
        results <- session
                     .select(cql"select data FROM ${lift(keyspace)}.test_data WHERE id IN $ids".as[String])
                     .runCollect
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    testM("select should be pure stream") {
      for {
        session     <- ZIO.service[Session]
        selectStream = session
                         .select(s"select data FROM $keyspace.test_data WHERE id IN (1,2,3)")
                         .map(_.getString(0))
                         .runCollect
        _           <- selectStream
        results     <- selectStream
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    testM("selectFirst should return None on empty result") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(s"select data FROM $keyspace.test_data WHERE id = 404")
                     .map(_.map(_.getString(0)))
      } yield assertTrue(result.isEmpty)
    },
    testM("selectFirst should return Some for one") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(s"select data FROM $keyspace.test_data WHERE id = 1")
                     .map(_.map(_.getString(0)))
      } yield assertTrue(result.contains("one"))
    },
    testM("selectFirst should return Some(null) for null") {
      for {
        session <- ZIO.service[Session]
        result <- session.selectFirst(s"select data FROM $keyspace.test_data WHERE id = 0").map(_.map(_.getString(0)))
      } yield assertTrue(result.contains(null))
    },
    testM("selectFirst interpolated query (cqlConst) should return Some") {
      for {
        session <- ZIO.service[Session]
        result <- session.selectFirst(cqlConst"select data FROM $keyspace.test_data WHERE id = 1".as[String])
      } yield assertTrue(result.contains("one"))
    },
    testM("selectFirst interpolated query (cql) should return Some") {
      for {
        session <- ZIO.service[Session]
        id = 1L
        result <- session.selectFirst(cql"select data FROM ${lift(keyspace)}.test_data WHERE id = $id".as[String])
      } yield assertTrue(result.contains("one"))
    },
    testM("select will emit in chunks sized equal to statement pageSize") {
      val st = SimpleStatement.newInstance(s"select data from $keyspace.test_data").setPageSize(2)
      for {
        session    <- ZIO.service[Session]
        stream      = session.select(st)
        chunkSizes <- stream.mapChunks(ch => Chunk.single(ch.size)).runCollect
      } yield assert(chunkSizes)(forall(equalTo(2))) && assertTrue(chunkSizes.size > 1)
    },
    testM("execute will create a table") {
      for {
        session <- ZIO.service[Session]
        table    = "table_" +UUID.randomUUID().toString.replaceAll("-", "_")
        created <- session.execute(cqlConst"create table $keyspace.$table(id text primary key)")
      } yield assertTrue(created)
    }
  )
}
