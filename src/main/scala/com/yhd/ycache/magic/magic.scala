
package com.yhd.ycache.magic

import java.util.Date

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
class Magic(sc: SparkContext, files: String, poolInfoPath:String="")  extends Serializable with MagicIntf{
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
  val tableRdd = sc.textFile(files,5).flatMap(_.split("\n"))
    .map(_.split(" ")).filter(_.length>POS_TYPE)
    .map(tokens => {
      def getPoolNameByIpPort(ipPort: String): String = {
        if (poolInfo != null && ipPort != null) {
          for ((k, v) <- poolInfo) {
            if (k.contains(ipPort)) return v
          }
        }
        null
      }
      def getPoolName(tks: Array[String]):String = {
        val p = getPoolNameByIpPort(tks(POS_DST).trim)
          if (p == null) {
            val p2 = getPoolNameByIpPort(tks(POS_SRC).trim)
            if (p2 == null)
              return "default"
            else
              return p2
          } else {
            p
          }
      }
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
          getLen(tokens),
          0
        )
      } else {
        Table(0,"","",0,"",0,"","",0,0,0,0)
      }
  }).persist()
  
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
  def filterExec(tbl: Table, poolNameFilter: String, filterOfKey: String):Boolean = {
    var ret = true

    if (poolNameFilter != null && poolNameFilter != "all")
      ret &&= tbl.poolName == poolNameFilter

    if (filterOfKey != null && filterOfKey != "all") {
      if (filterOfKey.startsWith("/") && filterOfKey.endsWith("/") && filterOfKey.length >= 2) {
        ret &&= tbl.key.matches(filterOfKey.substring(1, filterOfKey.length - 1))
      } else {
        ret &&= tbl.key.contains(filterOfKey)
      }
    }
    ret
  }

  /*
    1. Some metric according to memcached stats
   */
  override def getGetsHitRate(poolName: String, filterOfKey: String):Double= {
    val allGets = getGetsRdd(poolName,filterOfKey)
    allGets.persist()
    val missNum = allGets.filter(t => t.len == 0).count()
    val hitNum = allGets.filter(t => t.len > 0).count()
    allGets.unpersist()
    if (hitNum + missNum > 0) {
      missNum * 1.0 / (hitNum + missNum)
    } else {
      0
    }
  }

  // wrap the operation of getting gets command result's rdd by a key filter.
  def getGetsRdd(poolName: String, filterOfKey: String):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.command.startsWith("VALUE") &&
        filterExec(tbl,poolName,filterOfKey)
    })
  }

  // wrap the operation of setting gets command result's rdd by a key filter.
  def getSetsRdd(poolName: String, filterOfKey: String):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.command.startsWith("set") &&
        filterExec(tbl,poolName,filterOfKey)
    })
  }
  // wrap the operation of update command. .
  def getUpdatesRdd(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):RDD[Table]={
    tableRdd.filter(tbl => {
      (((tbl.command.startsWith("set") ||
          tbl.command.startsWith("add") ) &&
        (tbl.len >= minLen && tbl.len <= maxLen))||
        ((tbl.command.startsWith("incr") ||
            tbl.command.startsWith("decr")) &&
            (minLen<=22 && maxLen>0) /*FIXME*/)) &&
        filterExec(tbl, poolName, filterOfKey)
    })
  }
  // wrap the operation of getting all delete's rdd from a key filter.
  def getDeleteRdd(poolName: String, filterOfKey: String):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.command.startsWith("delete") &&
        filterExec(tbl, poolName, filterOfKey)
    })
  }
  // wrap the operation of getting all VALUE's rdd from a key filter.
  def getValueRdd(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int):RDD[Table]={
    tableRdd.filter(tbl => {
      tbl.command.startsWith("VALUE") &&
        (tbl.len >= minLen && tbl.len <= maxLen) &&
        filterExec(tbl, poolName, filterOfKey)
    })
  }

  override def getGets(poolName: String, filterOfKey: String) = getGetsRdd(poolName, filterOfKey).count()

  override def getUpdates(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = getUpdatesRdd(poolName, filterOfKey,minLen,maxLen).count()
  override def getUpdatesFail(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = 0
  override def getDeletes(poolName: String, filterOfKey: String) = getDeleteRdd(poolName, filterOfKey).count()
  override def getDeletesHits(poolName: String, filterOfKey: String) = 0
  /*
    2. Some  distribution of keys, values, and their propertys
   */
  override def getValueLenDistribution(poolName: String, filterOfKey: String="", minLen: Int=0, maxLen: Int=4*1024*1024) = {
    getValueRdd(poolName, filterOfKey, minLen, maxLen).map(tokens =>(tokens.len,1L)).
      reduceByKey(_+_).sortByKey().collect().toSeq
  }

  def showValueLenDistribution(poolName: String, filterOfKey: String="", minLen: Int=0, maxLen: Int=4*1024*1024) = {
    var buff = ""
    getValueLenDistribution(poolName, filterOfKey,minLen,maxLen).foreach(item=>buff  += s"\n${item._1}\t${item._2}")
    println("%table Value-len\tCount"+buff)
  }

  override def getUniqueValueLenDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) =0

  override def getKeyLenDistribution(poolName: String, filterOfKey: String="", minLen: Int=0, maxLen: Int=250) = {
    getValueRdd(poolName, filterOfKey,minLen,maxLen).map(tokens =>(tokens.key.length,1L)).
      reduceByKey(_+_).sortByKey().collect().toSeq
  }

  def showKeyLenDistribution(poolName: String, filterOfKey: String="", minLen: Int=0, maxLen: Int=250) = {
    var buff = ""
    getValueLenDistribution(poolName, filterOfKey,minLen,maxLen).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println("%table Key-len\tCount"+buff)
  }

  override def getUniqueKeyLenDistribution(poolName: String, filterOfKey: String) = 0

  override def getExpireDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = {
    getSetsRdd(poolName, filterOfKey).filter(tokens =>
        tokens.expire >= minLen && tokens.expire <= maxLen
        ).map(tokens =>(tokens.expire,1L)
      ).reduceByKey(_+_).sortByKey().collect().toSeq
  }
  def showExpireDistribution(poolName: String, filterOfKey: String="", minLen: Int=0, maxLen: Int=60*60*24*30) = {
    var buff = ""
    getExpireDistribution(poolName, filterOfKey, minLen, maxLen).foreach(item=>buff += s"\n${item._1}\t${item._2}")
    println("%table Expire\tCount"+buff)
  }
  override def getUniqueExpireDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = 0

  /*
    3. Some  distribution of commands, commands' intervals
   */
  //Get the interval between a key update and get operation.
  // This distribution tells us if the expire is reasonable
  override def getFirstGetIntervalDistribution(poolName: String, filterOfKey: String) = {}
  /*
    Get the interval between a key miss and last get operation.
    This distribution tells us if the expire is reasonable
    @params: threshold: Int, the max interval we care.
   */
  override def getLastGetIntervalDistribution(poolName: String, filterOfKey: String, threshold: Int) ={}

  /*
    Get the interval between the key update and miss.
    It means how long a key keep alive.
   */
  override def getLifeSpanDistribution(poolName: String, filterOfKey: String) = {}

  /*
    Get the gets number between the key update and miss.
    It means how many gets we are benefited during a key's life
 */
  override def getLifeBenefitDistribution(poolName: String, filterOfKey: String) = {}

  /*
    Get the counter distribution of updating a key with the same value
 */
  override def getUpdateSameDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = {}


  /*
    Get the interval distribution of updating a key with the same value
   */
  override def getUpdateSameIntervalDistribution(poolName: String, filterOfKey: String, minLen: Int, maxLen: Int) = {}

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


    import sqlContext.createSchemaRDD
    tableRdd.registerTempTable(tableName)
    println("####################")
    println("Table name is :" + tableName + "\nschema is:")
    tableRdd.printSchema()
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
    val targetRdd = tableRdd.filter(tbl => {
      tbl.command.startsWith("VALUE") &&
        filterExec(tbl,poolName,filterOfKey)
    })
    val counter = targetRdd.count()
    println(s"number of items is ${counter}")
    if (counter == 0)
      return

    import sqlContext.createSchemaRDD
    println("schema *****:")
    targetRdd.schema.fields.foreach(k => println(k.name))
    val filedName = targetRdd.schema.fields.map(_.name)

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
          case e: TableExistsException => {}
        }
      }

      val hTable = new HTable(hConf, tableName)
      patition.foreach(t => {
        val tSeq = t.toSeq()
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

    //reduce the
    println("*******result is ******************")
    System.out.println("hitRate = " + magic.getGetsHitRate("", filter))
    System.out.println("valueLen = " + magic.getValueLenDistribution(filter).mkString(","))
    magic.createTable()
    //magic.sql("select * from magic where len > 1000 limit 500")
    //println("poolInfo is : " + magic.poolInfo.mkString(","))

    magic.writeHbase(tableName,poolName, filter)

    val endTime = new Date()
    System.out.println("###used time: "+(endTime.getTime()-startTime.getTime())+"ms. ###")


  }
}
