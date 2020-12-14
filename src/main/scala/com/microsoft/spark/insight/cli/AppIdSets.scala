package com.microsoft.spark.insight.cli

/**
 * Base trait to specify a collection of Spark appId strings
 */
sealed trait AppIdSets

/**
 * Single Spark appId representation
 *
 * @param appId spark appId
 */
private[cli] case class AppId(appId: String) extends AppIdSets {
  override def toString: String = s"$appId"
}

/**
 * One set of Spark appId representation
 *
 * @param appIds one set of Spark appId
 */
private[cli] case class AppIdSet(appIds: Seq[String]) extends AppIdSets {
  override def toString: String = s"${appIds.mkString(", ")}"
}

/**
 * One pair of Spark appIds
 *
 * @param appId1 the first Spark appId
 * @param appId2 the second Spark appId
 */
private[cli] case class AppIdPair(appId1: String, appId2: String) extends AppIdSets {
  override def toString: String = s"$appId1 and $appId2"
}

/**
 * Two sets of Spark appIds
 *
 * @param appIds1 the first set of Spark appIds
 * @param appIds2 the second set of Spark appIds
 */
private[cli] case class AppIdSetPair(appIds1: Seq[String], appIds2: Seq[String]) extends AppIdSets {
  override def toString: String =
    s"Spark appId set #1: ${appIds1.mkString(", ")}\n" +
      s"Spark appId set #2: ${appIds2.mkString(", ")}"
}
