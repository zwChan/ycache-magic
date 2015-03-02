
package com.yhd.ycache.magic

import java.util.Date

import org.apache.spark.rdd.{RDD, HadoopRDD}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/*
  Magic class implement the interface @MagicIntf.
 */
class Magic(sc: SparkContext, filesOrPath: String)  extends Serializable with MagicIntf{
  /*
     0. init
    */
  def POS_GET_KEY = 4
  def POS_GET_FLG = 5
  def POS_GET_LEN = 6
  def POS_SET_KEY = 4
  def POS_SET_FLG = 5
  def POS_SET_EXP = 6
  def POS_SET_LEN = 7
  def POS_TIME = 0
  def POS_DST  = 1
  def POS_SRC  = 2
  def POS_TYPE = 3

  private val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val hdRdd= sc.textFile(filesOrPath).flatMap(_.split("\n")).persist()

  def string2Int(s: String, default: Int=0):Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => default
    }
  }

  /*
    Normalize the filter.
    if format is  /***/, used as *** a regular expression
    else match as a substring
    The output is regular expression
   */
  def filterExec(key: String, filterOfKey: String):Boolean = {
    if (filterOfKey==None || filterOfKey.length==0){
      return true
    }
    if (filterOfKey.startsWith("/") && filterOfKey.endsWith("/") && filterOfKey.length>=2){
      return key.matches(filterOfKey.substring(1,filterOfKey.length-1))
    }
    return key.contains(filterOfKey)
  }

  /*
    1. Some metric according to memcached stats
   */
  override def getGetsHitRate(filterOfKey: String):Double= {
    val allGets = getGetsRdd(filterOfKey)
    allGets.persist()
    val missNum = allGets.filter(t => string2Int(t(POS_GET_LEN)) == 0).count()
    val hitNum = allGets.filter(t => string2Int(t(POS_GET_LEN)) > 0).count()
    allGets.unpersist()
    if (hitNum + missNum > 0) {
      missNum * 1.0 / (hitNum + missNum)
    } else {
      0
    }
  }

  // wrap the operation of getting gets command result's rdd by a key filter.
  def getGetsRdd(filterOfKey: String):RDD[Array[String]]={
    hdRdd.map(_.split(" ")).filter(tokens => {
      tokens.length > POS_GET_LEN && tokens(POS_TYPE).startsWith("VALUE") &&
        filterExec(tokens(POS_GET_KEY),filterOfKey)
    })
  }

  // wrap the operation of setting gets command result's rdd by a key filter.
  def getSetsRdd(filterOfKey: String):RDD[Array[String]]={
    hdRdd.map(_.split(" ")).filter(tokens => {
      tokens.length > POS_SET_LEN && tokens(POS_TYPE).startsWith("set") &&
        filterExec(tokens(POS_SET_KEY),filterOfKey)
    })
  }
  // wrap the operation of update command. .
  def getUpdatesRdd(filterOfKey: String, minLen: Int, maxLen: Int):RDD[Array[String]]={
    hdRdd.map(_.split(" ")).filter(tokens => {
      tokens.length > POS_SET_LEN &&
        (((tokens(POS_TYPE).startsWith("set") ||
          tokens(POS_TYPE).startsWith("add") ) &&
          (string2Int(tokens(POS_GET_LEN))>=minLen && string2Int(tokens(POS_GET_LEN))<=maxLen)))||
          ((tokens(POS_TYPE).startsWith("incr") ||
          tokens(POS_TYPE).startsWith("decr")) &&
            (minLen<=22 && maxLen>0) /*FIXME*/) &&
        filterExec(tokens(POS_SET_KEY),filterOfKey)
    })
  }
  // wrap the operation of getting all delete's rdd from a key filter.
  def getDeleteRdd(filterOfKey: String):RDD[Array[String]]={
    hdRdd.map(_.split(" ")).filter(tokens => {
      tokens.length > POS_GET_LEN && tokens(POS_TYPE).startsWith("delete") &&
        filterExec(tokens(POS_GET_KEY),filterOfKey)
    })
  }
  // wrap the operation of getting all VALUE's rdd from a key filter.
  def getValueRdd(filterOfKey: String, minLen: Int, maxLen: Int):RDD[Array[String]]={
    hdRdd.map(_.split(" ")).filter(tokens => {
      tokens.length > POS_GET_LEN && tokens(POS_TYPE).startsWith("VALUE") &&
        ((string2Int(tokens(POS_GET_LEN))>=minLen && string2Int(tokens(POS_GET_LEN))<=maxLen)) &&
        filterExec(tokens(POS_GET_KEY),filterOfKey)
    })
  }

  override def getGets(filterOfKey: String) = getGetsRdd(filterOfKey).count()

  override def getUpdates(filterOfKey: String, minLen: Int, maxLen: Int) = getUpdatesRdd(filterOfKey,minLen,maxLen).count()
  override def getUpdatesFail(filterOfKey: String, minLen: Int, maxLen: Int) = 0
  override def getDeletes(filterOfKey: String) = getDeleteRdd(filterOfKey).count()
  override def getDeletesHits(filterOfKey: String) = 0
  /*
    2. Some  distribution of keys, values, and their propertys
   */
  override def getValueLenDistribution(filterOfKey: String="", minLen: Int=0, maxLen: Int=4*1024*1024) = {
    getValueRdd(filterOfKey,minLen,maxLen).map(tokens =>(string2Int(tokens(POS_GET_LEN)),1L)).
      reduceByKey(_+_).sortByKey().collect().toSeq
  }

  def showValueLenDistribution(filterOfKey: String="", minLen: Int=0, maxLen: Int=4*1024*1024) = {
    var buff = ""
    getValueLenDistribution(filterOfKey,minLen,maxLen).foreach(item=>buff  += s"\n${item._1}\t${item._2}")
    println("%table Value-len\tCount"+buff)
  }

  override def getUniqueValueLenDistribution(filterOfKey: String, minLen: Int, maxLen: Int) =0

  override def getKeyLenDistribution(filterOfKey: String="", minLen: Int=0, maxLen: Int=250) = {
    getValueRdd(filterOfKey,minLen,maxLen).map(tokens =>(tokens(POS_GET_KEY).length,1L)).
      reduceByKey(_+_).sortByKey().collect().toSeq
  }

  def showKeyLenDistribution(filterOfKey: String="", minLen: Int=0, maxLen: Int=250) = {
    var buff = ""
    getValueLenDistribution(filterOfKey,minLen,maxLen).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println("%table Key-len\tCount"+buff)
  }

  override def getUniqueKeyLenDistribution(filterOfKey: String) = 0

  override def getExpireDistribution(filterOfKey: String, minLen: Int, maxLen: Int) = {
    getSetsRdd(filterOfKey).filter(tokens =>
        string2Int(tokens(POS_SET_EXP))>=minLen && string2Int(tokens(POS_SET_EXP))<=maxLen
        ).map(tokens =>(string2Int(tokens(POS_SET_EXP)),1L)
      ).reduceByKey(_+_).sortByKey().collect().toSeq
  }
  def showExpireDistribution(filterOfKey: String="", minLen: Int=0, maxLen: Int=60*60*24*30) = {
    var buff = ""
    getExpireDistribution(filterOfKey,minLen,maxLen).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println("%table Expire\tCount"+buff)
  }
  override def getUniqueExpireDistribution(filterOfKey: String, minLen: Int, maxLen: Int) = 0

  /*
    3. Some  distribution of commands, commands' intervals
   */
  //Get the interval between a key update and get operation.
  // This distribution tells us if the expire is reasonable
  override def getFirstGetIntervalDistribution(filterOfKey: String) = {}
  /*
    Get the interval between a key miss and last get operation.
    This distribution tells us if the expire is reasonable
    @params: threshold: Int, the max interval we care.
   */
  override def getLastGetIntervalDistribution(filterOfKey: String, threshold: Int) ={}

  /*
    Get the interval between the key update and miss.
    It means how long a key keep alive.
   */
  override def getLifeSpanDistribution(filterOfKey: String) = {}

  /*
    Get the gets number between the key update and miss.
    It means how many gets we are benefited during a key's life
 */
  override def getLifeBenefitDistribution(filterOfKey: String) = {}

  /*
    Get the counter distribution of updating a key with the same value
 */
  override def getUpdateSameDistribution(filterOfKey: String, minLen: Int, maxLen: Int) = {}


  /*
    Get the interval distribution of updating a key with the same value
   */
  override def getUpdateSameIntervalDistribution(filterOfKey: String, minLen: Int, maxLen: Int) = {}

  /*
    4. Some operation to support spark SQL
   */
  def createTable(): Unit ={
    val sqlRdd = hdRdd.map(_.split(" ")).map(tokens => {
      def getPoolName(tokens: Array[String]):String = "default"
      def getIp(ipPort: String):String=ipPort.split(":")(0)
      def getPort(ipPort: String):Int=ipPort.split(":")(1).toInt
      def getKey(tokens: Array[String]):String = {
        tokens(POS_TYPE) match  {
          case "VALUE" => tokens(POS_GET_KEY)
          case "set" => tokens(POS_SET_KEY)
          case _ => ""
        }
      }
      def getFlag(tokens: Array[String]):Long = {
        tokens(POS_TYPE) match  {
          case "VALUE" => tokens(POS_GET_FLG).toLong
          case "set" => tokens(POS_SET_FLG).toLong
          case _ => 0
        }
      }
      def getExpire(tokens: Array[String]):Long = {
        tokens(POS_TYPE) match  {
          case "VALUE" => 0
          case "set" => tokens(POS_SET_EXP).toLong
          case _ => 0
        }
      }
      if(tokens.length>POS_TYPE) {
        Table(tokens(0).toFloat,
          getPoolName(tokens),
          getIp(tokens(POS_DST)),
          getPort(tokens(POS_DST)),
          getIp(tokens(POS_SRC)),
          getPort(tokens(POS_SRC)),
          tokens(POS_TYPE),
          getKey(tokens),
          getFlag(tokens),
          getExpire(tokens),
          getLen(tokens)
        )
      } else {
        Table(0,"","",0,"",0,"","",0,0,0)
      }
    })
    import sqlContext.createSchemaRDD
    sqlRdd.registerTempTable(tableName)
    println("####################")
    println("Table name is :" + tableName + "\nschema is:")
    sqlRdd.printSchema()
    println("####################")
  }
  def sql(sql: String) = {
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
      row.foreach(item =>buff += s"${item}\t")
      buff = buff.substring(0,buff.length-1)
    })
    println(buff)
  }

  def getLen(tokens: Array[String]):Int = {
    tokens(POS_TYPE) match  {
      case "VALUE" => tokens(POS_GET_LEN).toInt
      case "set" => tokens(POS_SET_LEN).toInt
      case _ => 0
    }
  }


}


object Magic {
  def main(args: Array[String]) {
    if (args.length<1){
      println("Error, usage: filename [filterOfKey]")
      return
    }
    val startTime = new Date()
    val sc = new SparkContext(new SparkConf().setAppName("ycache magic"))
    val filter = if (args.length>=2) args(1) else ""

    // split each document into words
    //val lines = sc.textFile(args(0),4).flatMap(_.split("\n"))
    // get each line of the record
    //val lines   = sc.textFile("hdfs://heju:8020/user/root/magic/log1.0209.1550",4).flatMap(_.split("\n"))
    // get the lines inclue "VALUE", then split it into array
    //val tokens  = lines.filter(_.contains("VALUE")).map(_.split(" ")).filter(_.length>=7).cache()
    //reduce the length of value
    //val vLens = tokens.filter((y)=>{var ret = true; y(6).foreach((x)=>if (x>='9' || x<='0'){ret = false}); ret}).map(_(6).toInt).map((_,1)).reduceByKey(_+_)
    //reduce the length of key

    val magic = new Magic(sc,args(0))

    //reduce the
    println("*******result is ******************")
    System.out.println("hitRate = " + magic.getGetsHitRate(filter))
    System.out.println("valueLen = " + magic.getValueLenDistribution(filter).mkString(","))
    magic.createTable()
    magic.sql("select * from magic where len > 1000")
    val endTime = new Date()
    System.out.println("###used time: "+(endTime.getTime()-startTime.getTime())+"ms. ###")
  }
}
