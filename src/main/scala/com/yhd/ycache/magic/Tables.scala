package com.yhd.ycache.magic


/**
 *  A case class for spark SQL.
 * Created by chenzhiwei on 2/12/2015.
 */
case class Table(time: Double,
                    poolName: String,
                    serverIp: String,
                    serverPort: Int,
                    clientIp: String,
                    clientPort: Int,
                    command: String,
                    flag: Long,
                    expire: Long,
                    len: Int,
                    value: Long,
                    key: String,
                    result: String
                   ) {

  def isUpdate: Boolean =  isSet || isAdd || isIncr || isDecr || isCas
  def isGet: Boolean =  command.startsWith("get") || command.startsWith("gets")
  def isSet: Boolean =  command.startsWith("set")
  def isAdd: Boolean =  command.startsWith("add")
  def isDelete: Boolean =  command.startsWith("delete")
  def isIncr: Boolean =  command.startsWith("incr")
  def isDecr: Boolean =  command.startsWith("decr")
  def isTouch: Boolean =  command.startsWith("touch")
  def isStats: Boolean =  command.startsWith("stats")
  def isCas: Boolean =  command.startsWith("cas")
  def isWithValue: Boolean = (isSet && len > 0) || isAdd || (isGet && len>0)






  def toSeq() = (time::poolName::serverIp::serverPort::clientIp::clientPort::command::flag::expire::len::value::key::result::Nil).toSeq
}
