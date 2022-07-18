package zio.cassandra.session

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.dimafeng.testcontainers.CassandraContainer
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Scope, ZIO }

import java.net.InetSocketAddress

object SessionSpec extends ZIOCassandraSpec with ZIOCassandraSpecUtils {

  val spec: Spec[Scope with CassandraContainer with Session, Throwable] = suite("Cassandra session")(
    test("Session.make must be referentially transparent") {
      val st = SimpleStatement.newInstance(s"select cluster_name from system.local")
      for {
        container   <- ZIO.service[CassandraContainer]
        testSession <-
          ZIO.attempt(
            Session.make(
              CqlSession
                .builder()
                .addContactPoint(
                  InetSocketAddress.createUnresolved(container.containerIpAddress, container.mappedPort(9042))
                )
                .withLocalDatacenter("datacenter1")
            )
          )
        r1          <- testSession.flatMap(session => session.selectFirst(st).map(_.map(_.getString(0))))
        r2          <- testSession.flatMap(session => session.selectFirst(st).map(_.map(_.getString(0))))
      } yield assertTrue(r1 == r2)
    },
    test("prepare should return PreparedStatement") {
      for {
        session <- ZIO.service[Session]
        st      <- session.prepare(s"select data FROM $keyspace.test_data WHERE id = :id")
      } yield assertTrue(st.getQuery == s"select data FROM $keyspace.test_data WHERE id = :id")
    },
    test("prepare should return error on invalid request") {
      for {
        session <- ZIO.service[Session]
        result  <- session.prepare(s"select column404 FROM $keyspace.test_data WHERE id = :id").either
      } yield assert(result)(isLeft(hasMessage(containsString("Undefined column name column404")))) &&
        assert(result)(isLeft(isSubtype[InvalidQueryException](Assertion.anything)))
    },
    test("select should return prepared data") {
      for {
        session <- ZIO.service[Session]
        results <- session
                     .select(s"select data FROM $keyspace.test_data WHERE id IN (1,2,3)")
                     .map(_.getString(0))
                     .runCollect
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    test("select should be pure stream") {
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
    test("selectOne should return None on empty result") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(s"select data FROM $keyspace.test_data WHERE id = 404")
                     .map(_.map(_.getString(0)))
      } yield assertTrue(result.isEmpty)
    },
    test("selectOne should return Some for one") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(s"select data FROM $keyspace.test_data WHERE id = 1")
                     .map(_.map(_.getString(0)))
      } yield assertTrue(result.contains("one"))
    },
    test("selectFirst should return Some(null) for null") {
      for {
        result <- Session
                    .selectFirst(s"select data FROM $keyspace.test_data WHERE id = 0")
                    .map(_.map(_.getString(0)))
      } yield assertTrue(result.contains(null))
    },
    test("select will emit in chunks sized equal to statement pageSize") {
      val st = SimpleStatement.newInstance(s"select data from $keyspace.test_data").setPageSize(2)
      for {
        session    <- ZIO.service[Session]
        stream      = session.select(st)
        chunkSizes <- stream.mapChunks(ch => Chunk.single(ch.size)).runCollect
      } yield assert(chunkSizes)(forall(equalTo(2))) && assertTrue(chunkSizes.size > 1)
    },
    test("select will fetch all data even there's more than one page") {
      val st = SimpleStatement.newInstance(s"select data from $keyspace.test_data WHERE id IN (1,2,3)").setPageSize(1)
      for {
        session <- ZIO.service[Session]
        results <- session.select(st).map(_.getString(0)).runCollect
      } yield assert(results)(hasSameElements(Chunk("one", "two", "three")))
    }
  )
}
