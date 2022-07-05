package zio.cassandra.session.cql

import zio.cassandra.session.cql.codec.Configuration
import zio.test._
import zio.test.environment.TestEnvironment

object ReadsConfigurationSpec extends DefaultRunnableSpec {

  private val toSnakeCase = Configuration.snakeCaseTransformation

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("Reads Configuration")(
      suite("snake_case field name transformer")(
        test("should not modify lower cased names") {
          val input    = "name"
          val expected = input
          assertTrue(toSnakeCase(input) == expected)
        },
        test("should not go crazy with uppper cased names") {
          val input    = "NAME"
          val expected = input.toLowerCase
          assertTrue(toSnakeCase(input) == expected)
        },
        test("should replace camelCase names with snakeCase") {
          val input    = "longName"
          val expected = "long_name"
          assertTrue(toSnakeCase(input) == expected)
        },
        test("should replace camelCase names with snakeCase if name is partially upper cased") {
          val input    = "IPProcessor"
          val expected = "ip_processor"
          assertTrue(toSnakeCase(input) == expected)
        }
      )
    )
}
