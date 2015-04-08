
package com.yhd.ycache.magic

import java.util.{Random, Date}

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.{RDD, HadoopRDD}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.util._


/**
 * Magic class implement the interface @MagicIntf.
 * @param sc
 * @param files The output of "ymemcap"
 * @param poolInfoPath The output of "yconsole ls"
 */
class Magic(@transient sc: SparkContext, files: String, poolInfoPath:String="")  extends Serializable with MagicIntf{
  /*
     0. init
    */
  def POS_GET_KEY = 4
  def POS_GET_FLG = 9
  def POS_GET_LEN = 10
  def POS_GET_RET = 8
  def POS_SET_KEY = 4
  def POS_SET_FLG = 5
  def POS_SET_EXP = 6
  def POS_SET_LEN = 7
  def POS_SET_RET = 8
  def POS_TIME = 0
  def POS_DST  = 1
  def POS_SRC  = 2
  def POS_TYPE = 3
  def POS_CHECKSUM = 11
  def POS_MIN = 12

  // precision of time when out put as X-axis
  var precision = 1
  var minPartition = 7

  var startTime = "1970/01/01 00:00:00"
  var endTime = "2018/01/01 00:00:00"

  var outputLimit = Int.MaxValue

  def str2ts(s: String): Double = {
    try {
      val format = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      format.parse(s).getTime / 1000
    } catch {
      case _: Throwable => println(s"** Input date format is invalid: ${s}"); 0
    }
  }

  @transient private val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  var tableRdd: RDD[Table] = null

  def load(poolNameFilter: String = "all", filterOfKey: String = "all") = {
    val startTs = str2ts(startTime)
    val endTs = str2ts(endTime)
    tableRdd = sc.textFile(files, minPartition).flatMap(_.split("\n"))
      .map(_.split(" ")).filter(_.length >= POS_MIN)
      .map(tokens => {
      def getPoolNameByIpPort(ipPort: String): String = {
        if (poolInfo != null && ipPort != null) {
          for ((k, v) <- poolInfo) {
            if (k.contains(ipPort)) return v
          }
        }
        null
      }
      def getPoolName(tks: Array[String]): String = {
        val p = getPoolNameByIpPort(tks(POS_DST).trim) //use server-ip and port
        if (p == null) {
          val p2 = getPoolNameByIpPort(getIp(tks(POS_SRC)) + ":") //use client-ip only
          if (p2 == null)
            return "default"
          else
            return p2
        } else {
          p
        }
      }
      def getIp(ipPort: String): String = ipPort.split(":")(0)
      def getPort(ipPort: String): Int = string2Int(ipPort.split(":")(1))
      def getKey(tokens: Array[String]): String = {
        tokens(POS_TYPE) match {
          case "get" | "GET" | "gets"  => tokens(POS_GET_KEY)
          case "set" | "SET" | "add" | "ADD"|"replace"|"append" | "prepend"|"cas" => tokens(POS_SET_KEY)
          case "delete" | "DELETE" => tokens(POS_GET_KEY)
          case "incr" | "decr" | "touch" => tokens(POS_GET_KEY)
          case _ => ""
        }
      }
      def getFlag(tokens: Array[String]): Long = {
        tokens(POS_TYPE) match {
          case "get" | "GET" | "gets"  => string2Int(tokens(POS_GET_FLG))
          case "set" | "SET" | "add" | "ADD"|"replace"|"append" | "prepend"|"cas" => string2Long(tokens(POS_SET_FLG))
          case _ => 0
        }
      }
      def getExpire(tokens: Array[String]): Long = {
        tokens(POS_TYPE) match {
          case "set" | "SET" | "add" | "ADD"|"replace"|"append" | "prepend"|"cas" => string2Long(tokens(POS_SET_EXP))
          case _ => 0
        }
      }
      def getLen(tokens: Array[String]):Int = {
        tokens(POS_TYPE) match  {
          case "get" | "GET" | "gets" => string2Int(tokens(POS_GET_LEN))
          case "set" | "SET" | "add" | "ADD"|"replace"|"append" | "prepend"|"cas" => string2Int(tokens(POS_SET_LEN))
          case _ => 0
        }
      }
      def getResult(tokens: Array[String]): String = {
        tokens(POS_TYPE) match {
          case "get" | "GET" | "gets" => tokens(POS_GET_RET)
          case "set" | "SET" | "add" | "ADD"|"replace"|"append" | "prepend"|"cas" => tokens(POS_SET_RET)
          case "delete" | "DELETE" => tokens(POS_SET_RET)
          case "incr" | "decr" | "touch" => tokens(POS_SET_RET)
          case _ => ""
        }
      }
      def getValueCheckSum(tokens: Array[String]): Long = {
        //The checksum is -1 is no value available
        string2Long(tokens(POS_CHECKSUM))
      }
      if (tokens.length > POS_TYPE) {
        Table(Magic.truncateAt(tokens(0).toDouble, 6),
          getPoolName(tokens),
          getIp(tokens(POS_DST)),
          getPort(tokens(POS_DST)),
          getIp(tokens(POS_SRC)),
          getPort(tokens(POS_SRC)),
          tokens(POS_TYPE).toLowerCase(),
          getFlag(tokens),
          getExpire(tokens),
          getLen(tokens),
          getValueCheckSum(tokens),
          getKey(tokens),
          getResult(tokens)
        )
      } else {
        Table(0, "", "", 0, "", 0, "", 0, 0, 0, 0, "", "")
      }
    }).filter(r => r.time >= startTs && r.time <= endTs && filterExec(r,poolNameFilter,filterOfKey)).persist()
  }

  def string2Int(s: String, default: Int=0):Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => println(s"string2Int error: ${s}"); default
    }
  }
  def string2Long(s: String, default: Long=0):Long = {
    try {
      s.toLong
    } catch {
      case e: Exception => println(s"string2Long error: ${s}"); default
    }
  }

  def strMatch(originString: String, filter: String): Boolean = {
    if (filter.length > 0 && filter != "all") {
      if (filter.startsWith("/") && filter.endsWith("/") && filter.length >= 2) {
        originString.matches(filter.substring(1, filter.length - 1))
      } else {
        originString.contains(filter)
      }
    } else {
      true
    }
  }

  /*
    Normalize the filter.
    if format is  /***/, used as *** a regular expression
    else match as a substring
   */
  def filterExec(tbl: Table, poolNameFilter: String = "", filterOfKey: String = "", minLen: Int=0, maxLen: Int=4*1024*1024):Boolean = {
    var ret = true
    ret &&= strMatch(tbl.poolName, poolNameFilter)
    ret &&= strMatch(tbl.key, filterOfKey)
    ret &&= tbl.len >= minLen && tbl.len <= maxLen
    ret
  }

  /*
    1. Some metric according to memcached stats
   */
//  override def getGetsHitRate(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):Double= {
//    val allGets = getGetsRdd(poolName,filterOfKey, minLen, maxLen)
//    allGets.persist()
//    val missNum = allGets.filter(t => t.len == 0).count()
//    val hitNum = allGets.filter(t => t.len > 0).count()
//    allGets.unpersist()
//    if (hitNum + missNum > 0) {
//      missNum * 1.0 / (hitNum + missNum)
//    } else {
//      0
//    }
//  }

  // wrap the operation of getting gets command result's rdd by a key filter.
  def getGetsRdd(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.isGet &&
        filterExec(tbl,poolName,filterOfKey,minLen,maxLen)
    })
  }

  // wrap the operation of setting gets command result's rdd by a key filter.
  def getSetsRdd(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.isSet &&
        filterExec(tbl,poolName,filterOfKey,minLen, maxLen)
    })
  }
  // wrap the operation of setting gets command result's rdd by a key filter.
  def getExpireRdd(poolName: String, filterOfKey: String):RDD[Table]={
    tableRdd.filter(tbl => {
      (tbl.expire > 0) &&
        filterExec(tbl,poolName,filterOfKey)
    })
  }
  // wrap the operation of update command. .
  def getUpdatesRdd(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.isUpdate &&
        filterExec(tbl, poolName, filterOfKey, minLen, maxLen)
    })
  }
  // wrap the operation of getting all delete's rdd from a key filter.
  def getDeleteRdd(poolName: String, filterOfKey: String):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.isDelete &&
        filterExec(tbl, poolName, filterOfKey)
    })
  }
  // wrap the operation of getting all VALUE's rdd from a key filter.
  def getValueLenRdd(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.isWithValue &&
        filterExec(tbl, poolName, filterOfKey, minLen, maxLen)
    })
  }

  override def getGets(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = getGetsRdd(poolName, filterOfKey, minLen, maxLen).count()
  override def getGetsHits(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = getGetsRdd(poolName, filterOfKey, minLen, maxLen).filter(_.result.startsWith("VALUE")).count()
  override def getGetsFail(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = getGetsRdd(poolName, filterOfKey, minLen, maxLen).filter(_.result.contains("ERROR")).count()
  override def getUpdates(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = getUpdatesRdd(poolName, filterOfKey,minLen,maxLen).count()
  override def getUpdatesFail(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = getUpdatesRdd(poolName, filterOfKey,minLen,maxLen).filter(_.result.contains("ERROR")).count()
  override def getDeletes(poolName: String, filterOfKey: String) = getDeleteRdd(poolName, filterOfKey).count()
  override def getDeleteHits(poolName: String, filterOfKey: String) = getDeleteRdd(poolName, filterOfKey).count()
  override def getDeletesFail(poolName: String, filterOfKey: String) = getDeleteRdd(poolName, filterOfKey).filter(_.result.contains("ERROR")).count()
  override def getDeletesHits(poolName: String, filterOfKey: String) = getDeleteRdd(poolName, filterOfKey).filter(_.result.contains("DELETED")).count()

  def showStats(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = {
    var buff = "%table Items\tValues\tItems\tValues"

    buff += "\n gets\t"+getGets(poolName, filterOfKey, minLen, maxLen)
    buff += "\t getHits\t"+getGetsHits(poolName, filterOfKey, minLen, maxLen)
    buff += "\n getHitRate\t"+Magic.ceilAt(4)(getGetsHitRate(poolName, filterOfKey, minLen, maxLen))
    buff += "\t updates\t"+getUpdates(poolName, filterOfKey, minLen, maxLen)
    buff += "\n deletes\t"+getDeletes(poolName, filterOfKey)
    buff += "\t deleteHits\t"+getDeleteHits(poolName, filterOfKey)
    buff += "\n deleteHitRate\t"+getDeleteHitRate(poolName, filterOfKey)
    buff += "\t getFails\t"+getGetsFail(poolName, filterOfKey, minLen, maxLen)
    buff += "\n updateFails\t"+getUpdatesFail(poolName, filterOfKey, minLen, maxLen)
    buff += "\t deleteFails\t"+getDeletesFail(poolName, filterOfKey)

    println(buff)
  }
  /*
    2. Some  distribution of keys, values, and their propertys
   */
  override def getValueLenDistribution(poolName: String, filterOfKey: String="", minLen: Int=0, maxLen: Int=4*1024*1024) = {
    getValueLenRdd(poolName, filterOfKey, minLen, maxLen).map(tokens =>(tokens.len,1L)).
      reduceByKey(_+_).sortByKey().take(outputLimit).toSeq
  }

  def showValueLenDistribution(poolName: String, filterOfKey: String="", unique: Boolean=false, minLen: Int=0, maxLen: Int=4*1024*1024) = {
    var buff = ""
    (if (!unique) {
      getValueLenDistribution(poolName, filterOfKey,minLen,maxLen)
    } else {
      getUniqueValueLenDistribution(poolName, filterOfKey,minLen,maxLen)
    })
      .foreach(item=>buff  += s"\n${item._1}\t${item._2}")
    println("%table Value-len\tCount"+buff)
  }

  override def getUniqueValueLenDistribution(poolName: String, filterOfKey: String, minLen: Int=0, maxLen: Int=4*1024*1024) = {
    /** XXX: (t.value*4*1024*1024 + t.len, t.len) => suposed that if the checksum of the value && the length of the value is equal,
      * then the value is equal.*/
    getValueLenRdd(poolName, filterOfKey, minLen, maxLen).map(t => (t.value*4*1024*1024 + t.len, t.len)).reduceByKey((v1,v2) => (v1+v2)/2)
      .map(vLen => (vLen._2, 1L)).reduceByKey(_+_).take(outputLimit).toSeq
  }

  override def getKeyLenDistribution(poolName: String, filterOfKey: String="", minKeyLen: Int=0, maxKeyLen: Int=250) = {
    tableRdd.filter(tbl => filterExec(tbl, poolName, filterOfKey) && tbl.key.length >= minKeyLen && tbl.key.length <= maxKeyLen)
      .map(tokens =>(tokens.key.length,1L)).reduceByKey(_+_).sortByKey().take(outputLimit).toSeq
  }

  def showKeyLenDistribution(poolName: String, filterOfKey: String="", unique: Boolean=false, minKeyLen: Int=0, maxKeyLen: Int=250) = {
    var buff = ""
    (if (!unique) {
      getKeyLenDistribution(poolName, filterOfKey, minKeyLen, maxKeyLen)
    }else {
      getUniqueKeyLenDistribution(poolName, filterOfKey, minKeyLen, maxKeyLen)
    })
      .foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println("%table Key-len\tCount"+buff)
  }

  override def getUniqueKeyLenDistribution(poolName: String, filterOfKey: String, minKeyLen: Int=0, maxKeyLen: Int=250) = {
    tableRdd.filter(tbl => filterExec(tbl, poolName, filterOfKey) && tbl.key.length >= minKeyLen && tbl.key.length <= maxKeyLen)
      .map(t => (t.key, t.key.length)).reduceByKey((v1,v2) => (v1+v2)/2)
      .map(kLen => (kLen._2, 1L)).reduceByKey(_+_).take(outputLimit).toSeq
  }

  override def getExpireDistribution(poolName: String, filterOfKey: String, minExp: Long, maxExp: Long) = {
    getExpireRdd(poolName, filterOfKey).filter(tokens =>
        tokens.expire >= minExp && tokens.expire <= maxExp
        ).map(tokens =>(tokens.expire,1L)
      ).reduceByKey(_+_).sortByKey().take(outputLimit).toSeq
  }
  def showExpireDistribution(poolName: String, filterOfKey: String="", unique: Boolean=false, minExp: Long=0, maxExp: Long=60*60*24*30) = {
    var buff = ""
    (if (!unique) {
      getExpireDistribution(poolName, filterOfKey, minExp, maxExp)
    }else {
      getUniqueExpireDistribution(poolName, filterOfKey, minExp, maxExp)
    })
      .foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println("%table Expire\tCount"+buff)
  }
  override def getUniqueExpireDistribution(poolName: String, filterOfKey: String, minExp: Long=0, maxExp: Long=60*60*24*30) = {
    getExpireRdd(poolName, filterOfKey).filter(tokens =>
      tokens.expire >= minExp && tokens.expire <= maxExp
    ).map(t => (t.key, t.expire)).reduceByKey((v1,v2) => (v1+v2)/2)
      .map(kExp => (kExp._2, 1L))
      .reduceByKey(_+_).sortByKey().take(outputLimit).toSeq
  }

  /*
    3. Some  distribution of commands, commands' intervals
   */
  //Get the interval between a key update and get operation.
  // This distribution tells us if the expire is reasonable
  override def getFirstNGetIntervalDistribution(poolName: String, filterOfKey: String, firstN: Int=1, start: Double, end: Double): Seq[(Double, Long)] = {
    val ret = getFirstNGetIntervalDistributionData(poolName,filterOfKey,firstN).map(_._2.get(firstN)).filter(_.isDefined).flatMap(_.get)
    ret.map(d => (Magic.truncateAt(d, precision),1L)).reduceByKey(_+_).filter(kv => kv._1>=start && kv._1 <= end).sortByKey().take(outputLimit).toSeq
  }
  def showFirstNGetIntervalDistribution(poolName: String, filterOfKey: String, firstN: Int=1, start: Double, end: Double) = {
    var buff = "%table firstN\tInterval\tCount"
    Range(1,firstN+1).foreach(num => getFirstNGetIntervalDistribution(poolName, filterOfKey, num, start, end).foreach(item=>buff += s"\n${num}\t${item._1}\t${item._2}"))
    println(buff)
  }
  /**
   *
   * @param poolName
   * @param filterOfKey
   * @param number >0: The first N 'get' after a 'update'; 0: life span; -1: last 'get' after a 'update'
   * @return RDD[(key:String, HashMap[number:Int, ArrayBuffer[interval:Double])]
   */
  def getFirstNGetIntervalDistributionData(poolName: String, filterOfKey: String, number: Int) = {
    tableRdd.filter(tbl => { //filter all update and get command
      ( tbl.isUpdate || tbl.isGet ) &&
        filterExec(tbl,poolName,filterOfKey)
    }).groupBy(_.key).map(kv => {
      import scala.collection.mutable._
      var hitUpdate = false
      var updateTime = 0.0
      var lastGetTime = 0.0
      var cnt = 0
      val retMap = new HashMap[Int, ArrayBuffer[Double]]()
      //get the first N  'get' after a 'set/add'
      kv._2.toArray.sortBy(_.time).map( item => {
        if (!hitUpdate) {
          if (item.isUpdate) {
            updateTime = item.time
            cnt = 0
            hitUpdate = true
          }
        } else {
          if (item.isUpdate) {
            // '0' means the interval between two 'update' opreration
            if (number == 0)retMap.getOrElseUpdate(cnt, new ArrayBuffer[Double]) += (item.time - updateTime)
            //'-1' means the last 'get' command
            if (cnt>0 && number == -1)retMap.getOrElseUpdate(cnt, new ArrayBuffer[Double]) += (item.time - lastGetTime)
            updateTime = item.time
            cnt = 0
          } else if (item.isGet) {
            cnt += 1
            //recored the first-N 'get' operation
            if (cnt <= number) retMap.getOrElseUpdate(cnt, new ArrayBuffer[Double]) += (item.time - updateTime)
            lastGetTime = item.time
          }
        }
        if (item.isDelete) {
          updateTime = 0
          cnt = 0
          hitUpdate = false
        }
      })
      (kv._1, retMap)
    })
  }

   /*
    Get the interval between a key miss and last get operation.
    This distribution tells us if the expire is reasonable
    @params: threshold: Int, the max interval we care.
   */
  override def getLastGetIntervalDistribution(poolName: String, filterOfKey: String, start: Double, end: Double) = {
    val ret = getFirstNGetIntervalDistributionData(poolName,filterOfKey,-1).flatMap(_._2).map(_._2).flatMap(r => r)
    ret.map(d => (Magic.truncateAt(d, precision),1L)).reduceByKey(_+_).filter(kv => kv._1>=start && kv._1 <= end).sortByKey().take(outputLimit).toSeq
  }
  def showLastGetIntervalDistribution(poolName: String, filterOfKey: String, threshold: Int, start: Double, end: Double) = {
    var buff = "%table Interval\tCount"
    getLastGetIntervalDistribution(poolName, filterOfKey, start, end).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println(buff)
  }
  /*
    Get the interval between the key update and miss.
    It means how long a key keep alive.
   */
  override def getLifeSpanDistribution(poolName: String, filterOfKey: String,  start: Double, end: Double) = {
    val ret = getFirstNGetIntervalDistributionData(poolName, filterOfKey,0).map(_._2.get(0)).filter(_.isDefined).flatMap(_.get)
    ret.map(d => (Magic.truncateAt(d, precision),1L)).reduceByKey(_+_).filter(kv => kv._1>=start && kv._1 <= end).sortByKey().take(outputLimit).toSeq
  }
  def showLifeSpanDistribution(poolName: String, filterOfKey: String,  start: Double, end: Double) = {
    var buff = "%table LifeSpan\tCount"
    getLifeSpanDistribution(poolName, filterOfKey,start,end).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println(buff)
  }

  /*
    Get the gets number between the key update and miss.
    It means how many gets we are benefited during a key's life
 */
  override def getLifeBenefitDistribution(poolName: String, filterOfKey: String,  start: Double, end: Double)  = {
    val ret = getFirstNGetIntervalDistributionData(poolName, filterOfKey,0).flatMap(kv => kv._2)
      .map(kv => (kv._1,kv._2.map(d => (d,kv._1)))).flatMap(_._2)
    ret.map(kv => (Magic.truncateAt(kv._1, precision), kv._2*1L)).reduceByKey(_+_).filter(kv => kv._1>=start && kv._1 <= end).sortByKey().take(outputLimit).toSeq
  }
  def showLifeBenefitDistribution(poolName: String, filterOfKey: String,  start: Double, end: Double) = {
    var buff = "%table LifeSpan\tGetsCount"
    getLifeBenefitDistribution(poolName, filterOfKey, start, end).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println(buff)
  }
  /*
    Get the counter distribution of updating a key with the same value by value's length
 */
  override def getUpdateSameDistribution(poolName: String, filterOfKey: String,  minCnt: Int, maxCnt: Int) = {
    val ret = getUpdateSameDistributionData(poolName, filterOfKey,0).flatMap(kv => kv._2)
    .map(kv => (kv._1,kv._2.map(d => (kv._1,d)))).flatMap(_._2)
    ret.map(kv => (kv._1, 1L)).reduceByKey(_+_).sortByKey().filter(kv => kv._1>=minCnt && kv._1 <= maxCnt).take(outputLimit).toSeq
  }
  def showUpdateSameDistribution(poolName: String, filterOfKey: String,  minCnt: Int, maxCnt: Int) = {
    var buff = "%table repeatTimes\tCount"
    getUpdateSameDistribution(poolName, filterOfKey,minCnt,maxCnt).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println(buff)
  }

  /**
   *
   * @param poolName
   * @param filterOfKey
   * @param opType 0: to get the number of continue update the same value; 1: to get interval ...
   * @return RDD[(key:String, HashMap[number:Int, ArrayBuffer[interval:Double])]
   */
  def getUpdateSameDistributionData(poolName: String, filterOfKey: String, opType: Int) = {
    tableRdd.filter(tbl => {
      (tbl.isUpdate) &&
        filterExec(tbl,poolName,filterOfKey)
    }).groupBy(_.key).map(kv => {
      import scala.collection.mutable._
      var lastTime = 0.0
      var lastValue = -1L
      var lastLen = 0
      var cnt = 0
      val retMap = new HashMap[Int, ArrayBuffer[Double]]()
      //get the last 'get' after a 'set/add'
      kv._2.toArray.sortBy(_.time).map( item => {
        if (lastValue == item.value && lastLen == item.len) {
          cnt += 1
          if (opType ==1) {
            retMap.getOrElseUpdate(cnt, new ArrayBuffer[Double]) += (item.time - lastTime)
            lastTime  = item.time
          }
        } else {
          if (opType ==0 && cnt > 0) {
            retMap.getOrElseUpdate(cnt, new ArrayBuffer[Double]) += (item.time - lastTime)
          }
          cnt = 0
          lastTime  = item.time
        }
        lastValue = item.value
        lastLen   = item.len
      })
      (kv._1, retMap)
    })
  }


  /*
    Get the interval distribution of updating a key with the same value
   */
  override def getUpdateSameIntervalDistribution(poolName: String, filterOfKey: String, start: Double, end: Double) = {
    val ret = getUpdateSameDistributionData(poolName, filterOfKey,1).flatMap(kv => kv._2).flatMap(_._2);
    ret.map(i => (Magic.truncateAt(i, precision), 1L)).reduceByKey(_+_).filter(kv => kv._1>=start && kv._1 <= end).sortByKey().take(outputLimit).toSeq
  }
  def showUpdateSameIntervalDistribution(poolName: String, filterOfKey: String, start: Double, end: Double) = {
    var buff = "%table Interval\tCount"
    getUpdateSameIntervalDistribution(poolName, filterOfKey, start, end).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println(buff)
  }
  /*
    4. Some operation to support spark SQL
   */
  private var isCreateTable = false
  def createTable(): Unit ={
    //Create only once
    if (isCreateTable)
      return
    else
      isCreateTable = true


    import sqlContext.implicits._
    tableRdd.toDF.registerTempTable(tableName)
    println("####################")
    println("Table name is :" + tableName + "\nschema is:")
    tableRdd.toDF.printSchema()
    println("####################")
  }

  def sql(sql: String) = {
    createTable()
    var sqlTemp = ""
    if (! sql.contains("limit")){
      sqlTemp = sql + " limit 1000"
    } else {
      sqlTemp = sql
    }
    val resultRdd = sqlContext.sql(sqlTemp)
    val result = resultRdd.collect()
    var buff ="%table "
    // get schema as the title
    resultRdd.schema.fieldNames.foreach(buff += _ + "\t")
    // delete last '/t'
    buff = buff.substring(0,buff.length-1)
    //fill the result to buff
    result.foreach(row=>{
      buff += "\n"
      row.toSeq.foreach(item =>buff += s"${item}\t")
      buff = buff.substring(0,buff.length-1)
    })
    println(buff)
  }


  /**
   *  5. Show the distribution of error on specified command.
   */
  def showErrorDistribution(poolName: String, filterOfKey: String, command: String="all", error: String="ERROR",start: Double, end: Double) = {
    var buff = "%table Offset Time\tErrorCount"
    val errorData = tableRdd.filter(t =>
      strMatch(t.poolName,poolName) &&
      strMatch(t.key,filterOfKey) &&
      strMatch(t.command,command) &&
      strMatch(t.result,error))
      .filter(t => t.time>=start && t.time <= end)
      .map(t => (Magic.truncateAt(t.time, precision),1L))
      .reduceByKey(_+_)
      .sortByKey()
      .take(outputLimit)

    errorData.foreach(item=>buff += s"\n${Magic.truncateAt(item._1 - start, precision)}\t${item._2}")
    println(buff)
  }


  /**
   * poolInfo is a map key: value=poolname, key: String=ip1:port1,ip2:port2*/
  val poolInfo = if (poolInfoPath.length > 0) {
    sc.textFile(poolInfoPath).flatMap(_.split("\n")).map(line =>{
      val token = line.trim().split(" ",2)
      if (token.length>=2) {
        (token(1).trim(), token(0).trim())
      } else {
        ("","")
      }
    }).filter(m => m._1.length>0 && m._2.length>0).collect().toMap
  }else {
    null
  }

  /**
   * Write the item to Hbase talbe.
   * @param tableName
   * @param poolName
   * @param filterOfKey
   */
  def writeHbase(tableName: String, poolName: String="all", filterOfKey: String="all"): Unit = {
    import sqlContext.implicits._
    val targetRdd = tableRdd.filter(tbl => {filterExec(tbl,poolName,filterOfKey)})
    val counter = targetRdd.count()
    println(s"number of items is ${counter}")
    if (counter == 0)
      return

    println("schema *****:")
    targetRdd.toDF.schema.fields.foreach(k => println(k.name))
    val filedName = targetRdd.toDF.schema.fields.map(_.name)

    targetRdd.foreachPartition(patition => {
      val hConf = HBaseConfiguration.create()
      print("add resource")
      hConf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
      hConf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
      println("new table")
      val hAdm = new HBaseAdmin(hConf)
      if (!hAdm.tableExists(tableName)) {
        println("create table test")
        try {
          val tableDesc = new HTableDescriptor(tableName)
          filedName.foreach(f =>
            tableDesc.addFamily(new HColumnDescriptor(f))
          )
          hAdm.createTable(tableDesc)
        } catch {
          case _:Throwable => {}
        }
      }

      val hTable = new HTable(hConf, tableName)
      patition.foreach(t => {
        val tSeq = t.toSeq
        val thePut = new Put(Bytes.toBytes(t.time.toString))
        filedName.zipWithIndex.foreach(kv =>
          thePut.add(Bytes.toBytes(kv._1), Bytes.toBytes(kv._1), Bytes.toBytes(tSeq(kv._2).toString))
        )
        hTable.put(thePut)
      })
      hTable.close()
    })
  }


}


object Magic {
  def truncateAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math floor n * s) / s }
  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }
  def ceilAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math ceil  n * s) / s }

  // for test
  def main(args: Array[String]) {
    if (args.length<1){
      println("Error, usage: dataFilename [poolInfoFileName] [filterOfKey] [poolName]")
      return
    }
    val startTime = new Date()
    val sc = new SparkContext(new SparkConf().setAppName("ycache magic"))

    val dataPath = args(0)
    val poolPath = if (args.length>=2) args(1) else ""
    val filter = if (args.length>=3) args(2) else "all"
    val poolName = if (args.length>=4) args(3) else "all"
    val tableName = if (args.length>=5) args(4) else "magic"


    // split each document into words
    //val lines = sc.textFile(args(0),4).flatMap(_.split("\n"))
    // get each line of the record
    //val lines   = sc.textFile("hdfs://heju:8020/user/root/magic/log1.0209.1550",4).flatMap(_.split("\n"))
    // get the lines inclue "VALUE", then split it into array
    //val tokens  = lines.filter(_.contains("VALUE")).map(_.split(" ")).filter(_.length>=7).cache()
    //reduce the length of value
    //val vLens = tokens.filter((y)=>{var ret = true; y(6).foreach((x)=>if (x>='9' || x<='0'){ret = false}); ret}).map(_(6).toInt).map((_,1)).reduceByKey(_+_)
    //reduce the length of key

    val magic = new Magic(sc,dataPath,poolPath)
    magic.load()

    //reduce the
    println("*******result is ******************")
    System.out.println("hitRate = " + magic.getGetsHitRate("", filter, 0, 4*1024*1024))
    System.out.println("valueLen = " + magic.getValueLenDistribution(filter).mkString(","))
    magic.createTable()
    //magic.sql("select * from magic where len > 1000 limit 500")
    //println("poolInfo is : " + magic.poolInfo.mkString(","))

    //magic.writeHbase(tableName,poolName, filter)

    val endTime = new Date()
    System.out.println("###used time: "+(endTime.getTime()-startTime.getTime())+"ms. ###")

  }
}
