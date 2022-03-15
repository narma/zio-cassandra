package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.ConsistencyLevel
import zio.{ Chunk, Task, ZIO }
import zio.cassandra.session.Session
import zio.test._
import zio.duration._

import java.time.{ LocalDate, LocalTime }
import java.util.UUID
import zio.stream.Stream
import zio.test.Assertion.{ isLeft, isSubtype }
// import zio.test.TestAspect.ignore

object CqlSpec {

  case class Data(id: Long, data: String)
  case class OptData(id: Long, data: Option[String])

  case class BasicInfo(weight: Double, height: String, datapoints: Set[Int])
  object BasicInfo {
    implicit val cqlReads: Reads[BasicInfo]   = FromUdtValue.deriveReads[BasicInfo]
    implicit val cqlBinder: Binder[BasicInfo] = ToUdtValue.deriveBinder[BasicInfo]
  }

  case class PersonAttribute(personId: Int, info: BasicInfo)

  case class CollectionTestRow(
    id: Int,
    maptest: Map[String, UUID],
    settest: Set[Int],
    listtest: Option[List[LocalDate]]
  )

  case class ExampleType(x: Long, y: Long, date: LocalDate, time: Option[LocalTime])

  case class ExampleNestedType(a: Int, b: String, c: Option[ExampleType])

  case class ExampleCollectionNestedUdtType(a: Int, b: Map[Int, Set[Set[Set[Set[ExampleNestedType]]]]])
  object ExampleCollectionNestedUdtType {
    implicit val binderExampleCollectionNestedUdtType: Binder[ExampleCollectionNestedUdtType] =
      ToUdtValue.deriveBinder[ExampleCollectionNestedUdtType]

    implicit val readsExampleCollectionNestedUdtType: Reads[ExampleCollectionNestedUdtType] =
      FromUdtValue.deriveReads[ExampleCollectionNestedUdtType]
  }

  case class ExampleNestedPrimitiveType(a: Int, b: Map[Int, Set[Set[Set[Set[Int]]]]])
  object ExampleNestedPrimitiveType {
    implicit val binderExampleNestedPrimitiveType: Binder[ExampleNestedPrimitiveType] =
      ToUdtValue.deriveBinder[ExampleNestedPrimitiveType]

    implicit val readsExampleNestedPrimitiveType: Reads[ExampleNestedPrimitiveType] =
      FromUdtValue.deriveReads[ExampleNestedPrimitiveType]
  }

  case class TableContainingExampleCollectionNestedUdtType(id: Int, data: ExampleCollectionNestedUdtType)

  case class TableContainingExampleNestedPrimitiveType(id: Int, data: ExampleNestedPrimitiveType)

  val cqlSuite = suite("cql suite")(
    testM("interpolated select template should return data from migration") {
      for {
        session  <- ZIO.service[Session]
        prepared <- cqlt"select data FROM tests.test_data WHERE id in ${Put[List[Long]]}"
                      .as[String]
                      .config(_.setTimeout(1.seconds))
                      .prepare(session)
        query     = prepared(List[Long](1, 2, 3))
        results  <- query.select.runCollect
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    testM("interpolated select template should return tuples from migration with multiple binding") {
      for {
        session <- ZIO.service[Session]
        query   <-
          cqlt"select data FROM tests.test_data_multiple_keys WHERE id1 = ${Put[Long]} and id2 = ${Put[Int]}"
            .as[String]
            .prepare(session)
        results <- query(1L, 2).config(_.setExecutionProfileName("default")).select.runCollect
      } yield assertTrue(results == Chunk("one-two"))
    },
    testM(
      "interpolated select template should return tuples from migration with multiple binding and margin stripped"
    ) {
      for {
        session <- ZIO.service[Session]
        query   <- cqlt"""select data FROM tests.test_data_multiple_keys
                       |WHERE id1 = ${Put[Long]} and id2 = ${Put[Int]}""".stripMargin.as[String].prepare(session)
        results <- query(1L, 2).config(_.setExecutionProfileName("default")).select.runCollect
      } yield assertTrue(results == Chunk("one-two"))
    },
    testM("interpolated select template should return data case class from migration") {
      for {
        session  <- ZIO.service[Session]
        prepared <-
          cqlt"select id, data FROM tests.test_data WHERE id in ${Put[List[Long]]}".as[Data].prepare(session)
        query     = prepared(List[Long](1, 2, 3))
        results  <- query.select.runCollect
      } yield assertTrue(results == Chunk(Data(1, "one"), Data(2, "two"), Data(3, "three")))
    },
    testM("interpolated select template should be reusable") {
      for {
        session <- ZIO.service[Session]
        query   <- cqlt"select data FROM tests.test_data WHERE id = ${Put[Long]}".as[String].prepare(session)
        result  <- Stream.fromIterable(Seq(1L, 2L, 3L)).flatMap(i => query(i).select).runCollect
      } yield assertTrue(result == Chunk("one", "two", "three"))
    },
    testM("interpolated select should return data from migration") {
      def getDataByIds(ids: List[Long]) =
        cql"select data FROM tests.test_data WHERE id in $ids"
          .as[String]
          .config(_.setConsistencyLevel(ConsistencyLevel.ALL))
      for {
        session <- ZIO.service[Session]
        results <- getDataByIds(List(1, 2, 3)).select(session).runCollect
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    testM("interpolated select should return tuples from migration") {
      def getAllByIds(ids: List[Long]) =
        cql"select id, data FROM tests.test_data WHERE id in $ids".as[(Long, String)]
      for {
        session <- ZIO.service[Session]
        results <- getAllByIds(List(1, 2, 3)).config(_.setQueryTimestamp(0L)).select(session).runCollect
      } yield assertTrue(results == Chunk((1L, "one"), (2L, "two"), (3L, "three")))
    },
    testM("interpolated select should return tuples from migration with multiple binding") {
      def getAllByIds(id1: Long, id2: Int) =
        cql"select data FROM tests.test_data_multiple_keys WHERE id1 = $id1 and id2 = $id2".as[String]
      for {
        session <- ZIO.service[Session]
        results <- getAllByIds(1, 2).select(session).runCollect
      } yield assertTrue(results == Chunk("one-two"))
    },
    testM("interpolated select should return tuples from migration with multiple binding and margin stripped") {
      def getAllByIds(id1: Long, id2: Int) =
        cql"""select data FROM tests.test_data_multiple_keys
             |WHERE id1 = $id1 and id2 = $id2""".stripMargin.as[String]
      for {
        session <- ZIO.service[Session]
        results <- getAllByIds(1, 2).select(session).runCollect
      } yield assertTrue(results == Chunk("one-two"))
    },
    testM("interpolated select should return data case class from migration") {
      def getIds(ids: List[Long]) =
        cql"select id, data FROM tests.test_data WHERE id in $ids".as[Data]
      for {
        session <- ZIO.service[Session]
        results <- getIds(List(1, 2, 3)).select(session).runCollect
      } yield assertTrue(results == Chunk(Data(1, "one"), Data(2, "two"), Data(3, "three")))
    },
    testM(
      "interpolated inserts and selects should produce UDTs and return data case classes when nested case classes are used"
    ) {
      val data = PersonAttribute(1, BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))

      for {
        session <- ZIO.service[Session]
        _       <- cql"INSERT INTO tests.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
                     .execute(session)
        result  <- cql"SELECT person_id, info FROM tests.person_attributes WHERE person_id = ${data.personId}"
                     .as[PersonAttribute]
                     .select(session)
                     .runCollect
      } yield assertTrue(result.length == 1 && result.head == data)
    },
    testM("interpolated inserts and selects should handle cassandra collections") {
      val dataRow1 = CollectionTestRow(1, Map("2" -> UUID.randomUUID()), Set(1, 2, 3), Option(List(LocalDate.now())))
      val dataRow2 = CollectionTestRow(2, Map("3" -> UUID.randomUUID()), Set(4, 5, 6), None)

      def insert(session: Session)(data: CollectionTestRow): Task[Boolean] =
        cql"INSERT INTO tests.test_collection (id, maptest, settest, listtest) VALUES (${data.id}, ${data.maptest}, ${data.settest}, ${data.listtest})"
          .execute(session)

      def retrieve(session: Session, id: Int, ids: Int*): Task[Chunk[CollectionTestRow]] = {
        val allIds = id :: ids.toList
        cql"SELECT id, maptest, settest, listtest FROM tests.test_collection WHERE id IN $allIds"
          .as[CollectionTestRow]
          .select(session)
          .runCollect
      }

      for {
        session <- ZIO.service[Session]
        _       <- ZIO.foreachPar_(List(dataRow1, dataRow2))(insert(session))
        res1    <- retrieve(session, dataRow1.id)
        res2    <- retrieve(session, dataRow2.id)
      } yield assertTrue(res1.length == 1 && res1.head == dataRow1) && assertTrue(
        res2.length == 1 && res2.head == dataRow2
      )
    },
    testM("interpolated inserts and selects should handle nested UDTs in heavily nested collections") {
      val row = TableContainingExampleCollectionNestedUdtType(
        id = 1,
        data = ExampleCollectionNestedUdtType(
          a = 2,
          b = Map(
            1 -> Set(
              Set(
                Set(
                  Set(
                    ExampleNestedType(
                      a = 3,
                      b = "4",
                      c = Option(ExampleType(x = 5L, y = 6L, date = LocalDate.now(), time = Option(LocalTime.now())))
                    )
                  )
                )
              )
            ),
            2 -> Set(
              Set(
                Set(
                  Set(
                    ExampleNestedType(
                      a = 10,
                      b = "100",
                      c = Option(ExampleType(x = 105L, y = 106L, date = LocalDate.now(), time = None))
                    )
                  )
                )
              )
            ),
            3 -> Set(
              Set(
                Set(
                  Set(
                    ExampleNestedType(
                      a = 24,
                      b = "101",
                      c = None
                    )
                  )
                )
              )
            )
          )
        )
      )

      for {
        session <- ZIO.service[Session]
        _       <- cql"INSERT INTO tests.heavily_nested_udt_table (id, data) VALUES (${row.id}, ${row.data})".execute(session)
        actual  <- cql"SELECT id, data FROM tests.heavily_nested_udt_table WHERE id = ${row.id}"
                     .as[TableContainingExampleCollectionNestedUdtType]
                     .select(session)
                     .runCollect
      } yield assertTrue(actual.length == 1 && actual.head == row)
    },
    testM("interpolated inserts and selects should handle UDTs and primitives in heavily nested collections") {
      val row                      = TableContainingExampleNestedPrimitiveType(
        id = 1,
        data = ExampleNestedPrimitiveType(
          a = 1,
          b = Map(
            1 -> Set(Set(Set(Set(2, 3), Set(4, 5)))),
            2 -> Set(Set(Set(Set(7, 8))))
          )
        )
      )
      def insert(session: Session) =
        cql"INSERT INTO tests.heavily_nested_prim_table (id, data) VALUES (${row.id}, ${row.data})".execute(
          session
        )

      def retrieve(session: Session) = cql"SELECT id, data FROM tests.heavily_nested_prim_table WHERE id = ${row.id}"
        .as[TableContainingExampleNestedPrimitiveType]
        .select(session)
        .runCollect

      for {
        session <- ZIO.service[Session]
        _       <- insert(session)
        actual  <- retrieve(session)
      } yield assertTrue(actual.length == 1 && actual.head == row)
    },
    testM("interpolated select should bind constants") {
      val query = cql"select data FROM tests.test_data WHERE id = ${1L}".as[String]
      for {
        session <- ZIO.service[Session]
        result  <- query.select(session).runCollect
      } yield assertTrue(result == Chunk("one"))
    },
    testM("cqlConst allows you to interpolate on what is usually not possible with cql strings") {
      val data         = PersonAttribute(2, BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
      val keyspaceName = "tests"
      val tableName    = "person_attributes"
      val selectFrom   = cql"SELECT person_id, info FROM "
      val keyspace     = cqlConst"$keyspaceName."
      val table        = cqlConst"$tableName"

      def where(personId: Int) =
        cql" WHERE person_id = $personId"

      def insert(session: Session, data: PersonAttribute) =
        (cql"INSERT INTO " ++ keyspace ++ table ++ cql" (person_id, info) VALUES (${data.personId}, ${data.info})")
          .execute(session)

      for {
        session <- ZIO.service[Session]
        _       <- insert(session, data)
        result  <- (selectFrom ++ keyspace ++ table ++ where(data.personId)).as[PersonAttribute].selectFirst(session)
      } yield assertTrue(result.isDefined && result.get == data)
    },
    suite("handle NULL values")(
      testM("return None if a type is Option") {
        for {
          session <- ZIO.service[Session]
          result  <- cql"select data FROM tests.test_data WHERE id = 0".as[Option[String]].selectFirst(session)
        } yield assertTrue(result.isDefined && result.get.isEmpty)
      },
      testM("raise error if a type is not an Option") {
        for {
          session <- ZIO.service[Session]
          result  <- cql"select data FROM tests.test_data WHERE id = 0".as[String].selectFirst(session).either
        } yield assert(result)(isLeft(isSubtype[UnexpectedNullValue](Assertion.anything)))
      },
      testM("return value for field in case class have Option type") {
        for {
          session <- ZIO.service[Session]
          row     <- cql"select id, data FROM tests.test_data WHERE id = 0".as[OptData].selectFirst(session)
        } yield assertTrue(row.isDefined && row.get.data.isEmpty)
      },
      testM("raise error if field in case class have Option type") {
        for {
          session <- ZIO.service[Session]
          result  <- cql"select id, data FROM tests.test_data WHERE id = 0".as[Data].selectFirst(session).either
        } yield assert(result)(isLeft(isSubtype[UnexpectedNullValue](Assertion.anything)))
      }
    )
  )
}
