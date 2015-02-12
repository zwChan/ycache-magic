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
                    len: Int
                   ) {

}
