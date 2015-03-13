package com.yhd.ycache.magic


/**
 *  A case class for spark SQL.
 * Created by chenzhiwei on 2/12/2015.
 */
case class Table(time: Float,
                    poolName: String,
                    serverIp: String,
                    serverPort: Int,
                    clientIp: String,
                    clientPort: Int,
                    command: String,
                    key: String,
                    flag: Long,
                    expire: Long,
                    len: Int,
                    value: Long
                   ) {
  def toSeq() = (time::poolName::serverIp::serverPort::clientIp::clientPort::command::key::flag::expire::len::value::Nil).toSeq
}
