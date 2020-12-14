package com.microsoft.spark.insight.utils.spark

import java.io.Reader
import java.text.SimpleDateFormat
import java.util.{Calendar, SimpleTimeZone}

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Utility class for serializing/deserializing Scala JSON objects
 */
object SparkJsonUtils {

  /**
   * Deserialize a JSON string into a Scala object
   *
   * @param json JSON string
   * @param m Manifest for type T
   * @tparam T type T of the object to be deserialized
   * @return Deserialized object
   */
  def fromJson[T](json: String)(implicit m: Manifest[T]): T = SCALA_OBJECT_MAPPER.readValue[T](json)

  /**
   * Deserialize a JSON string from a Reader into a Scala object
   *
   * @param json JSON reader
   * @param m Manifest for type T
   * @tparam T type T of the object to be deserialized
   * @return Deserialized object
   */
  def fromJson[T](json: Reader)(implicit m: Manifest[T]): T = SCALA_OBJECT_MAPPER.readValue[T](json)

  /**
   * Deserialize a JSON string into a generic [[JsonNode]] tree
   *
   * @param json JSON string
   * @return JsonNode tree
   */
  def jsonNode(json: String): JsonNode = SCALA_OBJECT_MAPPER.readTree(json)

  /**
   * Deserialize a JSON string from a Reader into a generic [[JsonNode]] tree
   *
   * @param json JSON reader
   * @return JsonNode tree
   */
  def jsonNode(json: Reader): JsonNode = SCALA_OBJECT_MAPPER.readTree(json)

  /**
   * Serialize a Scala object into a generic [[JsonNode]] tree
   *
   * @param value Scala object to serialize
   * @return JsonNode tree
   */
  def jsonNode(value: Any): JsonNode = SCALA_OBJECT_MAPPER.valueToTree(value)

  /**
   * Serialize the given object to a JSON string
   *
   * @param value object to serialize
   * @return JSON string
   */
  def toJson(value: Any): String = {
    SCALA_OBJECT_MAPPER.writeValueAsString(value)
  }

  /**
   * Date format used in Spark History Server
   */
  val DATE_FORMAT: SimpleDateFormat = {
    val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'")
    val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
    iso8601.setCalendar(cal)
    iso8601
  }

  /**
   * ScalaObjectMapper for serializing/deserializing JSON objects
   */
  val SCALA_OBJECT_MAPPER: ObjectMapper with ScalaObjectMapper = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.setDateFormat(DATE_FORMAT)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper
  }
}
