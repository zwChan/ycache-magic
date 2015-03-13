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
  def getGetsHitRate(poolName: String, filterOfKey: String):Double=0
  def getGetsHits(poolName: String, filterOfKey: String):Double=0
  def getGets(poolName: String, filterOfKey: String):Long=0
  def getUpdates(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Long=0
  def getUpdatesFail(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Long=0
  def getDeletes(poolName: String, filterOfKey: String):Long=0
  def getDeletesHits(poolName: String, filterOfKey: String):Double=0
  /*
    2. Some  distribution of keys, values, and their propertys
   */
  def getValueLenDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Seq[(Int,Long)]
  def getUniqueValueLenDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int)=0
  def getKeyLenDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Seq[(Int,Long)]
  def getUniqueKeyLenDistribution(poolName: String, filterOfKey: String)=0
  def getExpireDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Seq[(Long,Long)]
  def getUniqueExpireDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int)=0

  /*
    3. Some  distribution of commands, commands' intervals
   */
  //Get the interval between a key update and get operation.
  // This distribution tells us if the expire is reasonable
  def getFirstGetIntervalDistribution(poolName: String, filterOfKey: String)={}
  /*
    Get the interval between a key miss and last get operation.
    This distribution tells us if the expire is reasonable
    @params: threshold: Int, the max interval we care.
   */
  def getLastGetIntervalDistribution(poolName: String, filterOfKey: String, threshold: Int)={}

  /*
    Get the interval between the key update and miss.
    It means how long a key keep alive.
   */
  def getLifeSpanDistribution(poolName: String, filterOfKey: String)={}

  /*
    Get the gets number between the key update and miss.
    It means how many gets we are benefited during a key's life
 */
  def getLifeBenefitDistribution(poolName: String, filterOfKey: String)={}

  /*
    Get the counter distribution of updating a key with the same value
 */
  def getUpdateSameDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int)={}


  /*
    Get the interval distribution of updating a key with the same value
   */
  def getUpdateSameIntervalDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int)={}

}
