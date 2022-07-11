package zio.cassandra.session.cql.codec

import zio.cassandra.session.{ CassandraSpecUtils, Session }
import zio.test.Assertion.hasSameElements
import zio.test.{ assert, assertTrue, suite, testM }
import zio.{ Chunk, ZIO }

object ReadsSpec extends CassandraSpecUtils {

  final case class TypeTestData(
    id: BigInt,
    data: String,
    count: Option[Int],
    flag: Option[Boolean],
    dataset: Option[Set[Int]],
    datalist: Option[List[Int]],
    datamap: Option[Map[Int, String]]
  )

  final case class ChunkTestData(id: BigInt, datalist: Chunk[Int])

  final case class NameTestData(id: BigInt, ALLUPPER: String, alllower: String, someName: String, someLongName: String)

  final case class NullableCollectionsTestData(id: Int, regularList: Seq[Int], frozenList: Seq[Int])

  private val nameTestData = NameTestData(0, "ALL-UPPER", "all-lower", "some-name", "some-long-name")

  val readsTests = suite("Reads")(
    testM("should read simple data types") {
      val expected =
        Chunk(
          TypeTestData(0, "zero", None, None, None, None, None),
          TypeTestData(1, "one", None, None, Some(Set.empty), Some(List.empty), Some(Map.empty)),
          TypeTestData(2, "two", Some(20), Some(false), Some(Set(200)), Some(List(210)), Some(Map(220 -> "2_zero"))),
          TypeTestData(
            3,
            "three",
            Some(30),
            Some(true),
            Some(Set(300, 301, 302)),
            Some(List(310, 311, 312)),
            Some(Map(320 -> "3_zero", 321 -> "3_one"))
          ),
          TypeTestData(4, "four", None, None, None, None, None)
        )

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .select(s"select id, data, count, flag, dataset, datalist, datamap FROM $keyspace.reads_type_test")
                     .mapM(read[TypeTestData](_))
                     .runCollect
      } yield assert(result)(hasSameElements(expected))
    },
    testM("should read cassandra lists as chunks") {
      val expected = Chunk(ChunkTestData(2, Chunk(210)), ChunkTestData(3, Chunk(310, 311, 312)))

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .select(s"select id, datalist FROM $keyspace.reads_type_test where id in (2, 3)")
                     .mapM(read[ChunkTestData](_))
                     .runCollect
      } yield assert(result)(hasSameElements(expected))
    },
    testM("should read names as is") {
      implicit val configuration: Configuration = Configuration(identity(_))

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select id, ALLUPPER, alllower, someName, someLongName FROM $keyspace.reads_default_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    testM("should read names as is regardless of order in row") {
      implicit val configuration: Configuration = Configuration(identity(_))

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select alllower, id, someLongName, ALLUPPER, someName FROM $keyspace.reads_default_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    testM("should read names transformed to snake_case") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select id, ALLUPPER, alllower, some_name, some_long_name FROM $keyspace.reads_snake_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    testM("should read names transformed to snake_case regardless of order in row") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select alllower, id, some_long_name, ALLUPPER, some_name FROM $keyspace.reads_snake_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    testM("should read nulls and empty collections to empty collections") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .select(s"select id, regular_list, frozen_list FROM $keyspace.nullable_collection_tests")
                     .mapM(read[NullableCollectionsTestData](_))
                     .runCollect
      } yield assertTrue(result.forall(d => d.regularList.isEmpty && d.frozenList.isEmpty))
    }
  )

}
