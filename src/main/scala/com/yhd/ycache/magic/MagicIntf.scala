package com.yhd.ycache.magic

/**
 * MagicIntf defines the operation supported by Magic.
 *
 * Created by chenzhiwei on 2/10/2015.
 */
trait MagicIntf {
  /*
    0. init
   */
  var tableName = "magic"
  def setTableName(name: String="magic") { tableName = name}
  /*
    1. Some metric according to memcached stats
   */
  def getGetsHitRate(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Double=
    getGetsHits(poolName, filterOfKey,minLen,maxLen)*1.0/getGets(poolName, filterOfKey, minLen, maxLen)
  def getDeleteHitRate(poolName: String, filterOfKey: String):Double=
    getDeleteHits(poolName, filterOfKey)*1.0/getDeletes(poolName, filterOfKey)
  def getGetsHits(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Long
  def getGets(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Long
  def getGetsFail(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Long
  def getUpdates(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Long
  def getUpdatesFail(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Long
  def getDeletes(poolName: String, filterOfKey: String):Long
  def getDeleteHits(poolName: String, filterOfKey: String):Long
  def getDeletesFail(poolName: String, filterOfKey: String):Long
  def getDeletesHits(poolName: String, filterOfKey: String):Double
  /*
    2. Some  distribution of keys, values, and their propertys
   */
  def getValueLenDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int): Seq[(Int,Long)]
  def getUniqueValueLenDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int): Seq[(Int,Long)]
  def getKeyLenDistribution(poolName: String, filterOfKey: String, minKeyLen: Int, maxKeyLen: Int):Seq[(Int,Long)]
  def getUniqueKeyLenDistribution(poolName: String, filterOfKey: String, minKeyLen: Int, maxKeyLen: Int):Seq[(Int,Long)]
  def getExpireDistribution(poolName: String, filterOfKey: String, minExp: Long, maxExp: Long):Seq[(Long,Long)]
  def getUniqueExpireDistribution(poolName: String, filterOfKey: String, minExp: Long, max: Long):Seq[(Long,Long)]

  /*
    3. Some  distribution of commands, commands' intervals
   */
  //Get the interval between a key update and get operation.
  // This distribution tells us if the expire is reasonable
  def getFirstNGetIntervalDistribution(poolName: String, filterOfKey: String, firstN: Int, start: Double, end: Double): Seq[(Double, Long)]
  /*
    Get the interval between a key miss and last get operation.
    This distribution tells us if the expire is reasonable
    @params: threshold: Int, the max interval we care.
   */
  def getLastGetIntervalDistribution(poolName: String, filterOfKey: String,  start: Double, end: Double): Seq[(Double, Long)]

  /*
    Get the interval between the key update and miss.
    It means how long a key keep alive.
   */
  def getLifeSpanDistribution(poolName: String, filterOfKey: String, start: Double, end: Double): Seq[(Double, Long)]

  /*
    Get the gets number between the key update and miss.
    It means how many gets we are benefited during a key's life
 */
  def getLifeBenefitDistribution(poolName: String, filterOfKey: String, start: Double, end: Double): Seq[(Double, Long)]

  /*
    Get the counter distribution of updating a key with the same value
 */
  def getUpdateSameDistribution(poolName: String, filterOfKey: String, minCnt: Int, maxCnt: Int): Seq[(Int, Long)]


  /*
    Get the interval distribution of updating a key with the same value
   */
  def getUpdateSameIntervalDistribution(poolName: String, filterOfKey: String, start: Double, end: Double): Seq[(Double, Long)]

}
