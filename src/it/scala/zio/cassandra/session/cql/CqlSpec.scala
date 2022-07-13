package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.ConsistencyLevel
import zio.cassandra.session.{ Session, ZIOCassandraSpec }
import zio.cassandra.session.cql.codec.UnexpectedNullValue
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect.ignore
import zio.test._
import zio.{ test => _, _ }

import java.time.{ LocalDate, LocalTime }
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

object CqlSpec extends ZIOCassandraSpec {

  case class Data(id: Long, data: String)
  case class OptData(id: Long, data: Option[String])

  case class BasicInfo(weight: Double, height: String, datapoints: Set[Int])

  val personAttributeIdxCounter = new AtomicInteger(0)

  case class PersonAttribute(personId: Int, info: BasicInfo)
  case class OptPersonAttribute(personId: Int, info: Option[BasicInfo])

  case class OptBasicInfo(weight: Option[Double], height: Option[String], datapoints: Option[Set[Int]])

  case class PersonOptAttribute(personId: Int, info: OptBasicInfo)

  case class CollectionTestRow(
    id: Int,
    maptest: Map[String, UUID],
    settest: Set[Int],
    listtest: Option[List[LocalDate]]
  )

  case class ExampleType(x: Long, y: Long, date: LocalDate, time: Option[LocalTime])

  case class ExampleNestedType(a: Int, b: String, c: Option[ExampleType])

  case class ExampleCollectionNestedUdtType(a: Int, b: Map[Int, Set[Set[Set[Set[ExampleNestedType]]]]])

  case class ExampleNestedPrimitiveType(a: Int, b: Map[Int, Set[Set[Set[Set[Int]]]]])

  case class TableContainingExampleCollectionNestedUdtType(id: Int, data: ExampleCollectionNestedUdtType)

  case class TableContainingExampleNestedPrimitiveType(id: Int, data: ExampleNestedPrimitiveType)

  case class TableContainingNestedType(id: Int, data: ExampleNestedType)

  locally {
    val _ = ignore // make compiler happy about unused import
  }

  val spec: Spec[Session, Throwable] = suite("cql suite")(
    test("interpolated select template should return data from migration") {
      for {
        prepared <- cqlt"select data FROM tests.test_data WHERE id in ${Put[List[Long]]}"
                      .as[String]
                      .config(_.setTimeout(1.seconds))
                      .prepare
        query     = prepared(List[Long](1, 2, 3))
        results  <- query.select.runCollect
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    test("interpolated select template should return tuples from migration") {
      for {
        prepared <- cqlt"select id, data, dataset FROM tests.test_data WHERE id in ${Put[List[Long]]}"
                      .as[(Long, String, Option[Set[Int]])]
                      .prepare
        query     = prepared(List[Long](1, 2, 3))
        results  <- query.select.runCollect
      } yield assertTrue {
        results == Chunk((1L, "one", Some(Set.empty[Int])), (2L, "two", Some(Set(201))), (3L, "three", None))
      }
    },
    test("interpolated select template should return tuples from migration with multiple binding") {
      for {
        query   <-
          cqlt"select data FROM tests.test_data_multiple_keys WHERE id1 = ${Put[Long]} and id2 = ${Put[Int]}"
            .as[String]
            .prepare
        results <- query(1L, 2).config(_.setExecutionProfileName("default")).select.runCollect
      } yield assertTrue(results == Chunk("one-two"))
    },
    test(
      "interpolated select template should return tuples from migration with multiple binding and margin stripped"
    ) {
      for {
        query   <- cqlt"""select data FROM tests.test_data_multiple_keys
                       |WHERE id1 = ${Put[Long]} and id2 = ${Put[Int]}""".stripMargin.as[String].prepare
        results <- query(1L, 2).config(_.setExecutionProfileName("default")).select.runCollect
      } yield assertTrue(results == Chunk("one-two"))
    },
    test("interpolated select template should return data case class from migration") {
      for {
        prepared <-
          cqlt"select id, data FROM tests.test_data WHERE id in ${Put[List[Long]]}".as[Data].prepare
        query     = prepared(List[Long](1, 2, 3))
        results  <- query.select.runCollect
      } yield assertTrue(results == Chunk(Data(1, "one"), Data(2, "two"), Data(3, "three")))
    },
    test("interpolated select template should be reusable") {
      for {
        query  <- cqlt"select data FROM tests.test_data WHERE id = ${Put[Long]}".as[String].prepare
        result <- ZStream.fromIterable(Seq(1L, 2L, 3L)).flatMap(i => query(i).select).runCollect
      } yield assertTrue(result == Chunk("one", "two", "three"))
    },
    test("interpolated select should return data from migration") {
      def getDataByIds(ids: List[Long]) =
        cql"select data FROM tests.test_data WHERE id in $ids"
          .as[String]
          .config(_.setConsistencyLevel(ConsistencyLevel.ALL))
      for {
        results <- getDataByIds(List(1, 2, 3)).select.runCollect
      } yield assertTrue(results == Chunk("one", "two", "three"))
    },
    test("interpolated select should return tuples from migration") {
      def getAllByIds(ids: List[Long]) =
        cql"select id, data FROM tests.test_data WHERE id in $ids".as[(Long, String)]
      for {
        results <- getAllByIds(List(1, 2, 3)).config(_.setQueryTimestamp(0L)).select.runCollect
      } yield assertTrue(results == Chunk((1L, "one"), (2L, "two"), (3L, "three")))
    },
    test("interpolated select should return tuples from migration with multiple binding") {
      def getAllByIds(id1: Long, id2: Int) =
        cql"select data FROM tests.test_data_multiple_keys WHERE id1 = $id1 and id2 = $id2".as[String]
      for {
        results <- getAllByIds(1, 2).select.runCollect
      } yield assertTrue(results == Chunk("one-two"))
    },
    test("interpolated select should return tuples from migration with multiple binding and margin stripped") {
      def getAllByIds(id1: Long, id2: Int) =
        cql"""select data FROM tests.test_data_multiple_keys
             |WHERE id1 = $id1 and id2 = $id2""".stripMargin.as[String]
      for {
        results <- getAllByIds(1, 2).select.runCollect
      } yield assertTrue(results == Chunk("one-two"))
    },
    test("interpolated select should return data case class from migration") {
      def getIds(ids: List[Long]) =
        cql"select id, data FROM tests.test_data WHERE id in $ids".as[Data]
      for {
        results <- getIds(List(1, 2, 3)).select.runCollect
      } yield assertTrue(results == Chunk(Data(1, "one"), Data(2, "two"), Data(3, "three")))
    },
    test(
      "interpolated inserts and selects should produce UDTs and return data case classes when nested case classes are used"
    ) {
      val data =
        PersonAttribute(personAttributeIdxCounter.incrementAndGet(), BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))

      for {
        _      <- cql"INSERT INTO tests.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})".execute
        result <- cql"SELECT person_id, info FROM tests.person_attributes WHERE person_id = ${data.personId}"
                    .as[PersonAttribute]
                    .select
                    .runCollect
      } yield assertTrue(result.length == 1 && result.head == data)
    },
    test("interpolated inserts and selects should handle cassandra collections") {
      val dataRow1 = CollectionTestRow(1, Map("2" -> UUID.randomUUID()), Set(1, 2, 3), Option(List(LocalDate.now())))
      val dataRow2 = CollectionTestRow(2, Map("3" -> UUID.randomUUID()), Set(4, 5, 6), None)

      def insert(data: CollectionTestRow): RIO[Session, Boolean] =
        cql"INSERT INTO tests.test_collection (id, maptest, settest, listtest) VALUES (${data.id}, ${data.maptest}, ${data.settest}, ${data.listtest})".execute

      def retrieve(id: Int, ids: Int*): RIO[Session, Chunk[CollectionTestRow]] = {
        val allIds = id :: ids.toList
        cql"SELECT id, maptest, settest, listtest FROM tests.test_collection WHERE id IN $allIds"
          .as[CollectionTestRow]
          .select
          .runCollect
      }

      for {
        _    <- ZIO.foreachParDiscard(List(dataRow1, dataRow2))(insert)
        res1 <- retrieve(dataRow1.id)
        res2 <- retrieve(dataRow2.id)
      } yield assertTrue(res1.length == 1 && res1.head == dataRow1) && assertTrue(
        res2.length == 1 && res2.head == dataRow2
      )
    },
    test("interpolated inserts and selects should handle nested UDTs in heavily nested collections") {
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
        _      <- cql"INSERT INTO tests.heavily_nested_udt_table (id, data) VALUES (${row.id}, ${row.data})".execute
        actual <- cql"SELECT id, data FROM tests.heavily_nested_udt_table WHERE id = ${row.id}"
                    .as[TableContainingExampleCollectionNestedUdtType]
                    .select
                    .runCollect
      } yield assertTrue(actual.length == 1 && actual.head == row)
    },
    test("interpolated inserts and selects should handle UDTs and primitives in heavily nested collections") {
      val row    = TableContainingExampleNestedPrimitiveType(
        id = 1,
        data = ExampleNestedPrimitiveType(
          a = 1,
          b = Map(
            1 -> Set(Set(Set(Set(2, 3), Set(4, 5)))),
            2 -> Set(Set(Set(Set(7, 8))))
          )
        )
      )
      def insert =
        cql"INSERT INTO tests.heavily_nested_prim_table (id, data) VALUES (${row.id}, ${row.data})".execute

      def retrieve = cql"SELECT id, data FROM tests.heavily_nested_prim_table WHERE id = ${row.id}"
        .as[TableContainingExampleNestedPrimitiveType]
        .select
        .runCollect

      for {
        _      <- insert
        actual <- retrieve
      } yield assertTrue(actual.length == 1 && actual.head == row)
    },
    test("interpolated select should bind constants") {
      val query = cql"select data FROM tests.test_data WHERE id = ${1L}".as[String]
      for {
        result <- query.select.runCollect
      } yield assertTrue(result == Chunk("one"))
    },
    test("cqlConst allows you to interpolate on what is usually not possible with cql strings") {
      val data         =
        PersonAttribute(personAttributeIdxCounter.incrementAndGet(), BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
      val keyspaceName = "tests"
      val tableName    = "person_attributes"
      val selectFrom   = cql"SELECT person_id, info FROM "
      val keyspace     = cqlConst"$keyspaceName."
      val table        = cqlConst"$tableName"

      def where(personId: Int) =
        cql" WHERE person_id = $personId"

      def insert(data: PersonAttribute) =
        (cql"INSERT INTO " ++ keyspace ++ table ++ cql" (person_id, info) VALUES (${data.personId}, ${data.info})").execute

      for {
        _      <- insert(data)
        result <- (selectFrom ++ keyspace ++ table ++ where(data.personId)).as[PersonAttribute].selectFirst
      } yield assertTrue(result.isDefined && result.get == data)
    },
    suite("handle NULL values")(
      test("return None for Option[String]") {
        for {
          result <- cql"select data FROM tests.test_data WHERE id = 0".as[Option[String]].selectFirst
        } yield assertTrue(result.isDefined && result.get.isEmpty)
      },
      test("raise error for String(non-primitive)") {
        for {
          result <- cql"select data FROM tests.test_data WHERE id = 0".as[String].selectFirst.either
        } yield assert(result)(isLeft(isSubtype[UnexpectedNullValue](Assertion.anything)))
      },
      test("raise error for Int(primitive)") {
        for {
          result <- cql"select count FROM tests.test_data WHERE id = 0".as[Int].selectFirst.either
        } yield assert(result)(isLeft(isSubtype[UnexpectedNullValue](Assertion.anything)))
      },
      test("return value for field in case class have Option type") {
        for {
          row <- cql"select id, data FROM tests.test_data WHERE id = 0".as[OptData].selectFirst
        } yield assertTrue(row.isDefined && row.get.data.isEmpty)
      },
      test("raise error if field in case class have Option type") {
        for {
          result <- cql"select id, data FROM tests.test_data WHERE id = 0".as[Data].selectFirst.either
        } yield assert(result)(isLeft(isSubtype[UnexpectedNullValue](Assertion.anything)))
      },
      test("render meaningful message in error") {
        for {
          result <- cql"select id, data FROM tests.test_data WHERE id = 0".as[Data].selectFirst.either
        } yield assert(result) {
          isLeft {
            hasMessage {
              equalTo {
                "Read NULL value from table [tests.test_data] in column [data], expected type [text]. Row: [id:0, data:NULL]"
              }
            }
          }
        }
      },
      suite("handle NULL values with udt")(
        test("return None when optional udt value is null") {
          val data = OptPersonAttribute(personAttributeIdxCounter.incrementAndGet(), None)

          for {
            _      <-
              cql"INSERT INTO tests.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})".execute
            result <- cql"SELECT person_id, info FROM tests.person_attributes WHERE person_id = ${data.personId}"
                        .as[OptPersonAttribute]
                        .select
                        .runCollect
          } yield assertTrue(result.length == 1 && result.head == data)
        },
        test("raise error when non-optional udt value is null") {
          val data = OptPersonAttribute(personAttributeIdxCounter.incrementAndGet(), None)

          for {
            _      <-
              cql"INSERT INTO tests.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})".execute
            result <- cql"SELECT person_id, info FROM tests.person_attributes WHERE person_id = ${data.personId}"
                        .as[PersonAttribute]
                        .selectFirst
                        .either
          } yield assert(result)(isLeft(isSubtype[UnexpectedNullValue](Assertion.anything)))
        },
        test("return None when udt field value is null for optional type") {
          val data = PersonOptAttribute(
            personAttributeIdxCounter.incrementAndGet(),
            OptBasicInfo(None, None, None)
          )

          for {
            _      <-
              cql"INSERT INTO tests.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})".execute
            result <- cql"SELECT person_id, info FROM tests.person_attributes WHERE person_id = ${data.personId}"
                        .as[PersonOptAttribute]
                        .selectFirst
          } yield assertTrue(result.contains(data))
        },
        test("raise error if udt field value is mapped to String(non-primitive)") {
          val data =
            PersonOptAttribute(
              personAttributeIdxCounter.incrementAndGet(),
              OptBasicInfo(Some(160.0), None, Some(Set(1)))
            )

          for {
            _      <-
              cql"INSERT INTO tests.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})".execute
            result <- cql"SELECT person_id, info FROM tests.person_attributes WHERE person_id = ${data.personId}"
                        .as[PersonAttribute]
                        .selectFirst
                        .either
          } yield assert(result)(isLeft(isSubtype[UnexpectedNullValue](Assertion.anything)))
        },
        test("raise error if udt field value is mapped to Double(primitive)") {
          val data =
            PersonOptAttribute(
              personAttributeIdxCounter.incrementAndGet(),
              OptBasicInfo(None, Some("tall"), Some(Set(1)))
            )

          for {
            _      <-
              cql"INSERT INTO tests.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})".execute
            result <- cql"SELECT person_id, info FROM tests.person_attributes WHERE person_id = ${data.personId}"
                        .as[PersonAttribute]
                        .selectFirst
                        .either
          } yield assert(result)(isLeft(isSubtype[UnexpectedNullValue](Assertion.anything)))
        },
        test("render meaningful message in UDT error") {
          val data =
            PersonOptAttribute(
              personAttributeIdxCounter.incrementAndGet(),
              OptBasicInfo(None, Some("tall"), Some(Set(1)))
            )

          for {
            _      <-
              cql"INSERT INTO tests.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})".execute
            result <- cql"SELECT person_id, info FROM tests.person_attributes WHERE person_id = ${data.personId}"
                        .as[PersonAttribute]
                        .selectFirst
                        .either
          } yield assert(result) {
            isLeft {
              hasMessage {
                equalTo {
                  s"Read NULL value from table [tests.person_attributes] in UDT [info], type [tests.basic_info]. NULL value in field [weight], expected type [DOUBLE]. Row: [person_id:${data.personId}, info:{weight:NULL,height:'tall',datapoints:{1}}]"
                }
              }
            }
          }
        },
        test("render meaningful message in nested UDT error") {
          val data =
            TableContainingNestedType(
              0,
              ExampleNestedType(0, "0", Some(ExampleType(0, 0, null, None)))
            )

          for {
            _      <-
              cql"INSERT INTO tests.nested_udt_table (id, data) VALUES (${data.id}, ${data.data})".execute
            result <- cql"SELECT id, data FROM tests.nested_udt_table WHERE id = ${data.id}"
                        .as[TableContainingNestedType]
                        .selectFirst
                        .either
          } yield assert(result) {
            isLeft {
              hasMessage {
                equalTo {
                  "Read NULL value from table [tests.nested_udt_table] in UDT [data], type [tests.example_nested_type]. NULL value in field [date], expected type [DATE]. Row: [id:0, data:{a:0,b:'0',c:{x:0,y:0,date:NULL,time:NULL}}]"
                }
              }
            }
          }
        }
      )
    )
  )
}
