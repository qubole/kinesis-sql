/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kinesis

import com.amazonaws.services.kinesis.model.Shard
import scala.collection.mutable

import org.apache.spark.internal.Logging

/*
 * Helper class to sync batch with shards of the Kinesis stream.
 * It will create new activities when it discovers new Kinesis shards (bootstrap/resharding).
 * It works in similar way as
 * com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer in KCL
 */

private[kinesis] object ShardSyncer extends Logging {

  private def getShardIdToChildShardsMap(latestShards: Seq[Shard]):
    mutable.Map[String, List[String ]] = {
    val shardIdToChildShardsMap = mutable.Map.empty[String, List[String]]

    val shardIdToShardMap =
      latestShards.map {
        s => (s.getShardId -> s)
      }.toMap

    for ((shardId, shard) <- shardIdToShardMap) {
      val parentShardId: String = shard.getParentShardId
      if ( parentShardId != null && shardIdToShardMap.contains(parentShardId) ) {
        shardIdToChildShardsMap += (
          parentShardId ->
            (shardId :: shardIdToChildShardsMap.get(parentShardId).getOrElse(Nil))
          )
      }

      val adjacentParentShardId: String = shard.getAdjacentParentShardId
      if ( adjacentParentShardId != null && shardIdToShardMap.contains(adjacentParentShardId) ) {
        shardIdToChildShardsMap += (
          adjacentParentShardId ->
            (shardId :: shardIdToChildShardsMap.get(adjacentParentShardId).getOrElse(Nil))
          )
      }
    }
    // Assert that Parent Shards are closed
    shardIdToChildShardsMap.keySet.foreach {
      parentShardId =>
        shardIdToShardMap.get(parentShardId) match {
          case None =>
            throw new IllegalStateException(s"ShardId $parentShardId is not closed. " +
              s"This can happen due to a race condition between listShards and a" +
              s" reshard operation")
          case Some(parentShard: Shard) =>
            if (parentShard.getSequenceNumberRange().getEndingSequenceNumber == null) {
              throw new IllegalStateException(s"ShardId $parentShardId is not closed. " +
                s"This can happen due to a race condition between listShards and a " +
                s"reshard operation")
            }
        }
    }
    shardIdToChildShardsMap
  }

  private[kinesis] def AddShardInfoForAncestors(
     shardId: String,
     latestShards: Seq[Shard],
     initialPosition: InitialKinesisPosition,
     prevShardsList: mutable.Set[ String ],
     newShardsInfoMap: mutable.HashMap[ String, ShardInfo ],
     memoizationContext: mutable.Map[String, Boolean ]): Unit = {

    val shardIdToShardMap =
      latestShards.map {
        s => (s.getShardId -> s)
      }.toMap

    if (!memoizationContext.contains(shardId) &&
      shardId != null && shardIdToShardMap.contains(shardId) ) {
      if (prevShardsList.contains(shardId) ) {
        // we already have processed this shard in previous batch and added its ancestors
        memoizationContext.put(shardId, true)
        return
      }
      var shard = shardIdToShardMap.get(shardId).get
      // get parent of shards if exist
      var parentShardIds: mutable.HashSet[String] = getParentShardIds(shard, latestShards)
      for (parentShardId <- parentShardIds) {
        // Add ShardInfo of Parent's ancestors.
        AddShardInfoForAncestors( parentShardId,
          latestShards, initialPosition, prevShardsList,
          newShardsInfoMap, memoizationContext)
      }
      // create shardInfo for its parent shards (if they don't exist)
      for (parentShardId <- parentShardIds) {
        if (!prevShardsList.contains(parentShardId) ) {
          logDebug("Need to create a shardInfo for shardId " + parentShardId)
          if (newShardsInfoMap.get(parentShardId).isEmpty) {
              newShardsInfoMap.put(parentShardId,
                new ShardInfo(parentShardId, initialPosition.shardPosition(parentShardId)))
            }
          }
      }
      memoizationContext.put(shardId, true)
    }
  }

  private[kinesis] def getParentShardIds(
     shard: Shard,
     shards: Seq[Shard]): mutable.HashSet[String] = {
    val parentShardIds = new mutable.HashSet[ String ]
    val parentShardId = shard.getParentShardId
    val shardIdToShardMap =
      shards.map {
        s => (s.getShardId -> s)
      }.toMap

    if ((parentShardId != null) && shardIdToShardMap.contains(parentShardId)) {
      parentShardIds.add(parentShardId)
    }
    val adjacentParentShardId = shard.getAdjacentParentShardId
    if ( (adjacentParentShardId != null) && shardIdToShardMap.contains(adjacentParentShardId)) {
      parentShardIds.add(adjacentParentShardId)
    }
    return parentShardIds
  }

  /*
   *  Takes a sequence of Shard as input params
   *  It iterate though each shards
   *  and return a sequence of shard-ids of open Shards
   */
  def openShards(shards: Seq[Shard]): Seq[String] = {
    // List of open Shards
    shards.collect {
      case s: Shard if (s.getSequenceNumberRange.getEndingSequenceNumber == null) => s.getShardId
    }
  }

  /*
   *  Takes a sequence of Shard as input params
   *  It iterate though each shards
   *  and return a sequence of shard-ids of closed Shards
   */

  def closedShards(shards: Seq[Shard]): Seq[String] = {
    // List of closed Shards
    shards.collect {
      case s: Shard if (s.getSequenceNumberRange.getEndingSequenceNumber != null) => s.getShardId
    }
  }

  def hasNewShards(latestShardsInfo: Seq[ShardInfo],
                   prevShardsInfo: Seq[ShardInfo]): Boolean = {
    latestShardsInfo.foldLeft(false) {
      (hasNewShard, shardInfo) =>
        if (!hasNewShard) {
          // Check only if hasNewShard is false
          prevShardsInfo.contains(shardInfo.shardId)
        } else {
          hasNewShard
        }
    }
  }

  def hasDeletedShards(latestShardsInfo: Seq[ShardInfo],
                   prevShardsInfo: Seq[ShardInfo]): Boolean = {
    prevShardsInfo.foldLeft(false) {
      (hasDeletedShard, shardInfo) =>
        if (!hasDeletedShard) {
          // Check only if hasDeletedShard is false
          latestShardsInfo.contains(shardInfo.shardId)
        } else {
          hasDeletedShard
        }
    }
  }

  def getLatestShardInfo(
      latestShards: Seq[Shard],
      prevShardsInfo: Seq[ShardInfo],
      initialPosition: InitialKinesisPosition,
      failOnDataLoss: Boolean = true): Seq[ShardInfo] = {

    if (latestShards.isEmpty) {
      return prevShardsInfo
    }
    var prevShardsList = new mutable.HashSet[String]
    var latestShardsList = new mutable.HashSet[String]
    prevShardsInfo.foreach {
      s: ShardInfo => prevShardsList.add(s.shardId)
    }
    latestShards.foreach {
      s: Shard => latestShardsList.add(s.getShardId)
    }
    // check for deleted shards
    val deletedShardsList = prevShardsList.diff(latestShardsList)
    val newShardsInfoMap = new mutable.HashMap[String, ShardInfo]
    val memoizationContext = new mutable.HashMap[ String, Boolean]

    // check for deleted Shards and update newShardInfo if failOnDataLoss is false
    if (deletedShardsList.nonEmpty) {
      if (failOnDataLoss) {
        throw new IllegalStateException(
          s"""
             | Some data may have been lost because ${deletedShardsList.toString()}
             | are not available in Kinesis any more. The shard has been deleted before
             | we have processed all records in it. If you do not want your streaming query
             | to fail on such cases, set the source option "failOnDataLoss" to "false"
           """.stripMargin
        )
      } else {
        log.warn(
          s"""
             | Some data may have been lost because $deletedShardsList are not available in Kinesis
             | any more. The shard has been deleted before we have processed all records in it.
             | If you want your streaming query to fail on such cases, set the source option
             | "failOnDataLoss" to "true"
           """.stripMargin
        )
      }
    }

    // filter the deleted shards
    var filteredPrevShardsInfo = prevShardsInfo.filter {
      s: ShardInfo => !deletedShardsList.contains(s.shardId)
    }

    // check for new shards and fetch ShardInfo for them
    openShards(latestShards).map {
      shardId: String =>
        if (prevShardsList.contains(shardId)) {
          logDebug("Info for shardId " + shardId + " already exists")
        }
        else {
          AddShardInfoForAncestors(shardId,
            latestShards, initialPosition, prevShardsList, newShardsInfoMap, memoizationContext)
          newShardsInfoMap.put(shardId,
            new ShardInfo(shardId, initialPosition.shardPosition(shardId)))
        }
    }
    filteredPrevShardsInfo ++ newShardsInfoMap.values.toSeq
  }

}
