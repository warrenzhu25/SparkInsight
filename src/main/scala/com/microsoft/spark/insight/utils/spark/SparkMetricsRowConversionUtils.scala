package com.microsoft.spark.insight.utils.spark

import java.util.Date

import com.microsoft.spark.insight.fetcher.status.ExecutorMetricDistributions
import com.microsoft.spark.insight.utils.spark.RawSparkApplicationAttempt.{JobDataMap, StageDataMap}
import com.microsoft.spark.insight.utils.spark.RawSparkMetrics.AppAttemptId
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.Row
import org.apache.spark.status.api.v1._
import org.slf4j.{Logger, LoggerFactory}
import sun.misc.Unsafe

import scala.reflect.api._
import scala.util.Try

/**
 * A conversion utility for parsing a Spark Row containing an STP (Spark Tracking Pipeline) record (from
 * dalids://u_griddev.sparkmetrics) into a [[RawSparkMetrics]] container
 * <p>
 * The lists of fields below define the conversion from fields in STP to fields in SHS structures. If fields are added
 * to STP and are desired in the SHS structures, the lists should be updated accordingly.
 * <p>
 * This is a temporary utility that will be replaced later once STS (Spark Tracking Service) will go live and replace
 * STP
 */
object SparkMetricsRowConversionUtils {
  val LOGGER: Logger = LoggerFactory.getLogger(SparkMetricsRowConversionUtils.getClass)

  /**
   * A utility method for parsing a Spark Row containing an STP record (from dalids://u_griddev.sparkmetrics) into a
   * [[RawSparkMetrics]] container.
   * Since all of the SHS structures (e.g. [[StageData]]) do not expose a public c'tor, we use reflection black magic
   * to reconstruct and populate the structures
   *
   * @param sparkMetrics a Spark Row containing an STP
   * @return a RawSparkMetrics container
   */
  def convert(sparkMetrics: Row): RawSparkMetrics = {
    val appInfo: ApplicationInfo = {
      // An STP record only represents a single application attempt. This is less of a concern as multi-attempt apps are
      // unusual in GRID. Collect the single attempt info and populate the array with it.
      val applicationAttemptInfo = createTypedInstanceAndPopulate[ApplicationAttemptInfo](sparkMetrics)

      val appInfo = createTypedInstanceAndPopulate[ApplicationInfo](sparkMetrics)
      setField(appInfo, "attempts", Seq(applicationAttemptInfo))
      appInfo
    }

    val appAttemptMap: Map[AppAttemptId, RawSparkApplicationAttempt] = {
      val applicationEnvironmentInfo: ApplicationEnvironmentInfo = {
        val applicationEnvironmentInfo = createTypedInstanceAndPopulate[ApplicationEnvironmentInfo](sparkMetrics)

        setField(applicationEnvironmentInfo, "sparkProperties", getRowSeqAsMap(sparkMetrics, "sparkproperties").toSeq)
        setField(applicationEnvironmentInfo, "systemProperties", getRowSeqAsMap(sparkMetrics, "systemproperties").toSeq)
        setField(applicationEnvironmentInfo, "classpathEntries", getRowSeqAsMap(sparkMetrics, "classpathentries").toSeq)
        populateFieldFromNestedRow("runtime", sparkMetrics, applicationEnvironmentInfo)

        applicationEnvironmentInfo
      }

      val jobDataMap: JobDataMap = getRowSeq(sparkMetrics, "jobs")
        .getOrElse(Seq.empty)
        .map(jobRow => {
          val jobData = createTypedInstanceAndPopulate[JobData](jobRow)
          setField(jobData, "killedTasksSummary", getRowSeqAsMap[Int](jobRow, "killedtaskssummary"))
          jobData
        })
        .map(jobData => jobData.jobId -> jobData)
        .toMap

      val stageDataMap: StageDataMap = getRowSeq(sparkMetrics, "stages")
        .getOrElse(Seq.empty)
        .map(stageRow => {
          val stageData = createTypedInstanceAndPopulate[StageData](stageRow)

          val accumulatorUpdates = getRowSeq(stageRow, "accumulatorupdates")
            .getOrElse(Seq.empty)
            .map(createTypedInstanceAndPopulate[AccumulableInfo])
          setField(stageData, "accumulatorUpdates", accumulatorUpdates)

          getRow(stageRow, "tasksummary").foreach(taskSummaryRow => {
            val taskSummary = createTypedInstanceAndPopulate[TaskMetricDistributions](taskSummaryRow)
            setField(stageData, "taskSummary", Option(taskSummary))

            populateFieldFromNestedRow("inputMetrics", taskSummaryRow, taskSummary)
            populateFieldFromNestedRow("outputMetrics", taskSummaryRow, taskSummary)
            populateFieldFromNestedRow("shuffleReadMetrics", taskSummaryRow, taskSummary)
            populateFieldFromNestedRow("shuffleWriteMetrics", taskSummaryRow, taskSummary)
          })
          populateFieldFromNestedRow("executorMetricsSummary", stageRow, stageData)

          stageData
        })
        .groupBy(_.stageId)
        .mapValues(_.map(stageData => stageData.attemptId -> stageData).toMap)

      Map(
        appInfo.attempts.head.attemptId
          .getOrElse(throw new IllegalArgumentException("No application attempt ID was found")) ->
          RawSparkApplicationAttempt(appInfo.attempts.head, applicationEnvironmentInfo, jobDataMap, stageDataMap))
    }

    RawSparkMetrics(appInfo, appAttemptMap)
  }

  import scala.reflect.runtime.universe.{TermName, Type, TypeTag, typeOf}

  private def getFieldType[T](typeTag: TypeTag[T], fieldName: String): Type = {
    try {
      typeTag.tpe.member(TermName(fieldName)).info.resultType
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Unable to retrieve field type for field $fieldName in ${typeTag.tpe}", e)
    }
  }

  /**
   * Recursively convert the value onto the desired [[Type]]
   *
   * @param destType type to covert the value to
   * @param value value to convert
   * @return converted value in the desired [[Type]]
   */
  private def extractValue(destType: Type, value: Any): Any = (destType, value) match {
    case (t, null) if t <:< typeOf[Option[_]] => None
    case (t, _) if t <:< typeOf[Option[_]] => Option(extractValue(destType.typeArgs.head, value))
    case (t, null) if t =:= typeOf[Long] => 0L
    case (t, value: Any) if t =:= typeOf[String] => value.toString
    case (t, value: String) if t =:= typeOf[Date] => SparkJsonUtils.DATE_FORMAT.parse(value)
    case (t, value: Number) if t =:= typeOf[Int] => value.intValue()
    case (t, value: Long) if t =:= typeOf[Long] => value
    case (t, value: String) if t =:= typeOf[JobExecutionStatus] => JobExecutionStatus.fromString(value)
    case (t, value: String) if t =:= typeOf[StageStatus] => StageStatus.fromString(value)
    case _ => value
  }

  /**
   * An incredibly hacky method for setting immutable fields
   *
   * @param obj object with the field to be set
   * @param fieldName field name to modify
   * @param value value to set for the given field name
   */
  private def setField[T](obj: Any, fieldName: String, value: Any)(implicit typeTag: TypeTag[T]): Unit = {
    val clazz = obj.getClass
    Try(clazz.getDeclaredField(fieldName)).toOption match {
      case Some(field) =>
        // Retrieve the destination member type and convert value if necessary
        val valueToSet = extractValue(getFieldType(typeTag, fieldName), value)
        try {
          field.setAccessible(true)
          clazz.getMethod(fieldName).invoke(obj) // force init in case it's a lazy val
          field.set(obj, valueToSet) // overwrite value
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(s"Failed to set field '$fieldName' in class '$clazz'", e)
        }

      case None =>
        throw new IllegalArgumentException(s"Field $fieldName is not a member of ${typeTag.tpe}")
    }
  }

  private def createEmptyInstance[T](implicit typeTag: TypeTag[T]): Any =
    Unsafe.getUnsafe.allocateInstance(typeTag.mirror.runtimeClass(typeTag.tpe))

  private def assignFieldsFromRow[T](
      fieldNamePairs: Seq[FieldNamePair],
      obj: Any,
      row: Option[Row],
      typeTag: TypeTag[T]): Any = {
    row.foreach { row =>
      fieldNamePairs
        .map(fieldNamePair => (fieldNamePair.destFieldName, getValue(row, fieldNamePair.sourceFieldName)))
        .foreach { case (fieldName, value) => setField(obj, fieldName, value)(typeTag) }
    }
    obj
  }

  private def createInstanceAndPopulate[T](row: Option[Row])(implicit typeTag: TypeTag[T]): Any =
    assignFieldsFromRow(typeToFieldNames.getOrElse(typeTag.tpe, Seq.empty), createEmptyInstance[T], row, typeTag)

  private def createTypedInstanceAndPopulate[T](row: Row)(implicit typeTag: TypeTag[T]): T =
    createInstanceAndPopulate[T](Option(row)).asInstanceOf[T]

  private val mirror = scala.reflect.runtime.universe.runtimeMirror(getClass.getClassLoader)

  private def typeToTypeTag[T](tpe: Type): TypeTag[T] =
    TypeTag(
      mirror,
      new TypeCreator {
        def apply[U <: Universe with Singleton](m: Mirror[U]): U#Type =
          if (m eq mirror) {
            tpe.asInstanceOf[U#Type]
          } else {
            throw new IllegalArgumentException(s"Type tag defined in $mirror cannot be migrated to other mirrors.")
          }
      })

  private def populateFieldFromNestedRow[P](fieldNamePair: FieldNamePair, row: Row, parentObj: P)(
      implicit typeTag: TypeTag[P]): Unit = {

    def createInstanceAndPopulateToType[T](memberType: Type): Any = memberType match {
      case t if t <:< typeOf[Option[_]] => Option(createInstanceAndPopulateToType(memberType.typeArgs.head))
      case _ => createInstanceAndPopulate(getRow(row, fieldNamePair.sourceFieldName))(typeToTypeTag(memberType))
    }

    // Retrieve the destination field type so the appropriate object can be instantiated
    val obj = createInstanceAndPopulateToType(getFieldType(typeTag, fieldNamePair.destFieldName))
    setField(parentObj, fieldNamePair.destFieldName, obj)
  }

  private case class FieldNamePair(sourceFieldName: String, destFieldName: String)

  private object FieldNamePair {
    def apply(destFieldName: String): FieldNamePair = FieldNamePair(destFieldName.toLowerCase, destFieldName)
  }

  implicit private def stringToFieldNamePair(string: String): FieldNamePair = FieldNamePair(string)

  implicit private def stringsToFieldNamePairs(seq: Seq[String]): Seq[FieldNamePair] = seq.map(FieldNamePair(_))

  private def getField(row: Row, fieldName: String): Option[Any] =
    Try(row.fieldIndex(fieldName)).toOption.filterNot(row.isNullAt).map(row.getAs[Any])

  /**
   * Extract more precisely typed values from [[Row]] fields: convert [[String]]s to [[Number]]s and convert empty
   * strings to nulls.
   *
   * @param value object to convert
   * @return transformed value
   */
  private def transformValue(value: Any): Any = value match {
    case seq: Seq[_] => seq.map(transformValue)
    case "" => null
    case number: String if NumberUtils.isCreatable(number) => NumberUtils.createNumber(number)
    case v => v
  }

  private def getValue(row: Row, fieldName: String): Any = getField(row, fieldName).map(transformValue).orNull

  private def getRow(row: Row, fieldName: String): Option[Row] = getField(row, fieldName).map(_.asInstanceOf[Row])

  private def getRowSeq(row: Row, fieldName: String): Option[Seq[Row]] =
    getField(row, fieldName).map(_.asInstanceOf[Seq[Row]])

  /**
   * Extracts a [[Map]] from a [[Row]] field of type [[Seq]] of [[Row]]s, where each inner [[Row]] is of a struct with
   * field "key" and "value"
   *
   * @param row row to extract the KV [[Row]]s from
   * @param fieldName field name that holds the KV [[Row]]s
   * @tparam V type of value
   * @return reconstructed map
   */
  private def getRowSeqAsMap[V](row: Row, fieldName: String): Map[String, V] =
    getRowSeq(row, fieldName)
      .getOrElse(Seq.empty)
      .map(innerRow =>
        getField(innerRow, "key").map(_.asInstanceOf[String]) -> getField(innerRow, "value").map(_.asInstanceOf[V]))
      .filter {
        case (k, v) if k.isDefined && v.isDefined => true
        case (k, v) =>
          LOGGER.warn(s"Malformed KV-pair detected in $fieldName. key: $k, value: $v")
          false
      }
      .map { case (k, v) => k.get -> v.get }
      .toMap

  private val APP_ATTEMPT_INFO_FIELDS =
    Seq("attemptId", "startTime", "endTime", "lastUpdated", "duration", "sparkUser", "completed", "appSparkVersion")
  private val APP_INFO_FIELDS = Seq(FieldNamePair("appid", "id"), FieldNamePair("name"))
  private val RUNTIME_INFO_FIELDS = Seq("javaVersion", "javaHome", "scalaVersion")
  private val JOB_DATA_FIELDS = Seq(
    "jobId",
    "name",
    "description",
    "submissionTime",
    "completionTime",
    "stageIds",
    "jobGroup",
    "status",
    "numTasks",
    "numActiveTasks",
    "numCompletedTasks",
    "numSkippedTasks",
    "numFailedTasks",
    "numKilledTasks",
    "numCompletedIndices",
    "numActiveStages",
    "numCompletedStages",
    "numSkippedStages",
    "numFailedStages")
  private val STAGE_DATA_FIELDS = Seq(
    "status",
    "stageId",
    "numTasks",
    "numActiveTasks",
    "numCompleteTasks",
    "numFailedTasks",
    "numKilledTasks",
    "numCompletedIndices",
    "submissionTime",
    "firstTaskLaunchedTime",
    "completionTime",
    "failureReason",
    "executorDeserializeCpuTime",
    "executorRunTime",
    "executorCpuTime",
    "resultSize",
    "resultSerializationTime",
    "memoryBytesSpilled",
    "diskBytesSpilled",
    "inputBytes",
    "inputRecords",
    "outputBytes",
    "outputRecords",
    "shuffleRemoteBlocksFetched",
    "shuffleLocalBlocksFetched",
    "shuffleFetchBackoffCount",
    "shuffleRemoteBytesRead",
    "shuffleRemoteBytesReadToDisk",
    "shuffleLocalBytesRead",
    "shuffleReadBytes",
    "shuffleReadRecords",
    "shuffleWriteBytes",
    "shuffleWriteRecords",
    "name",
    "description",
    "details",
    "schedulingPool",
    "peakJvmUsedMemory",
    "peakExecutionMemory",
    "peakStorageMemory",
    "peakUnifiedMemory",
    "executorDeserializeTime",
    "jvmGcTime",
    "shuffleReadFetchWaitTime",
    "shuffleWriteTime",
    "rddIds").map(FieldNamePair(_)) ++ Seq(
    FieldNamePair("stageattemptid", "attemptId"),
    FieldNamePair("shufflereadfetchwaittime", "shuffleFetchWaitTime"))
  private val ACCUMULATOR_UPDATE_FIELDS = Seq("id", "name", "update", "value")
  private val TASK_SUMMARY_FIELDS = Seq(
    "quantiles",
    "executorDeserializeTime",
    "executorDeserializeCpuTime",
    "executorRunTime",
    "executorCpuTime",
    "resultSize",
    "jvmGcTime",
    "resultSerializationTime",
    "gettingResultTime",
    "schedulerDelay",
    "peakExecutionMemory",
    "memoryBytesSpilled",
    "diskBytesSpilled")
  private val INPUT_METRICS_FIELDS = Seq("bytesRead", "recordsRead")
  private val OUTPUT_METRICS_FIELDS = Seq("bytesWritten", "recordsWritten")
  private val SHUFFLE_READ_METRICS_FIELDS = Seq(
    "readBytes",
    "readRecords",
    "remoteBlocksFetched",
    "localBlocksFetched",
    "fetchWaitTime",
    "fetchBackoffCount",
    "remoteBytesRead",
    "remoteBytesReadToDisk",
    "totalBlocksFetched")
  private val SHUFFLE_WRITE_METRICS_FIELDS = Seq("writeBytes", "writeRecords", "writeTime")
  private val EXEC_METRICS_SUMMARY_FIELDS = Seq(
    "quantiles",
    "numTasks",
    "inputBytes",
    "inputRecords",
    "outputBytes",
    "outputRecords",
    "shuffleRead",
    "shuffleReadRecords",
    "shuffleWrite",
    "shuffleWriteRecords",
    "memoryBytesSpilled",
    "diskBytesSpilled",
    "peakJvmUsedMemory",
    "peakExecutionMemory",
    "peakStorageMemory",
    "peakUnifiedMemory")

  private val typeToFieldNames: Map[Type, Seq[FieldNamePair]] = Map(
    typeOf[ApplicationInfo] -> APP_INFO_FIELDS,
    typeOf[ApplicationAttemptInfo] -> APP_ATTEMPT_INFO_FIELDS,
    typeOf[RuntimeInfo] -> RUNTIME_INFO_FIELDS,
    typeOf[JobData] -> JOB_DATA_FIELDS,
    typeOf[StageData] -> STAGE_DATA_FIELDS,
    typeOf[AccumulableInfo] -> ACCUMULATOR_UPDATE_FIELDS,
    typeOf[TaskMetricDistributions] -> TASK_SUMMARY_FIELDS,
    typeOf[InputMetricDistributions] -> INPUT_METRICS_FIELDS,
    typeOf[OutputMetricDistributions] -> OUTPUT_METRICS_FIELDS,
    typeOf[ShuffleReadMetricDistributions] -> SHUFFLE_READ_METRICS_FIELDS,
    typeOf[ShuffleWriteMetricDistributions] -> SHUFFLE_WRITE_METRICS_FIELDS,
    typeOf[ExecutorMetricDistributions] -> EXEC_METRICS_SUMMARY_FIELDS)
}
