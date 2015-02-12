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
  //def config(filesOrPath: String)
  /*
    1. Some metric according to memcached stats
   */
  def getGetsHitRate(filterOfKey: String):Double=0
  def getGetsHits(filterOfKey: String):Double=0
  def getGets(filterOfKey: String):Long=0
  def getUpdates(filterOfKey: String, minLen: Int, maxLen: Int):Long=0
  def getUpdatesFail(filterOfKey: String, minLen: Int, maxLen: Int):Long=0
  def getDeletes(filterOfKey: String):Long=0
  def getDeletesHits(filterOfKey: String):Double=0
  /*
    2. Some  distribution of keys, values, and their propertys
   */
  def getValueLenDistribution(filterOfKey: String, minLen: Int, maxLen: Int):Seq[(Int,Long)]
  def getUniqueValueLenDistribution(filterOfKey: String, minLen: Int, maxLen: Int)=0
  def getKeyLenDistribution(filterOfKey: String, minLen: Int, maxLen: Int):Seq[(Int,Long)]
  def getUniqueKeyLenDistribution(filterOfKey: String)=0
  def getExpireDistribution(filterOfKey: String, minLen: Int, maxLen: Int):Seq[(Int,Long)]
  def getUniqueExpireDistribution(filterOfKey: String, minLen: Int, maxLen: Int)=0

  /*
    3. Some  distribution of commands, commands' intervals
   */
  //Get the interval between a key update and get operation.
  // This distribution tells us if the expire is reasonable
  def getFirstGetIntervalDistribution(filterOfKey: String)={}
  /*
    Get the interval between a key miss and last get operation.
    This distribution tells us if the expire is reasonable
    @params: threshold: Int, the max interval we care.
   */
  def getLastGetIntervalDistribution(filterOfKey: String, threshold: Int)={}

  /*
    Get the interval between the key update and miss.
    It means how long a key keep alive.
   */
  def getLifeSpanDistribution(filterOfKey: String)={}

  /*
    Get the gets number between the key update and miss.
    It means how many gets we are benefited during a key's life
 */
  def getLifeBenefitDistribution(filterOfKey: String)={}

  /*
    Get the counter distribution of updating a key with the same value
 */
  def getUpdateSameDistribution(filterOfKey: String, minLen: Int, maxLen: Int)={}


  /*
    Get the interval distribution of updating a key with the same value
   */
  def getUpdateSameIntervalDistribution(filterOfKey: String, minLen: Int, maxLen: Int)={}

}
