# ycache-magic
Analyze the behavior pattern of memcached for YHD.com.

##Introduce
  Memcached is one of the most important role in the framework of high performance business website,
  specially when the reading requests are much more than the writing requests. Memcached can take care
  of most of the reading requests, rather than going to read data in database or other "expensive" source.
  That is what we call "Bypass cache system". The "hit-rate" is the key for high performance of such system,
  but unfortunately sometimes the hit-rate is poor because bad access pattern of access, such as using a
  small expire, updating the key frequently and so on.
  Ycache-magic is a tool to analyze the pattern. How to do it?
  - First, we cache the access information using a tool called Ycap. Ycap uses the pcap lib to capture the
  packets accessing your memcached, and records useful information to a log file.
  - Ycache-magic analyze the log file offline using Spark, and show the result using Zeppelin. Both Spark
  and Zeppelin are open source software of Apache.
  - Ycache-magic uses a concept of "pool" to indicate that a group of memcached instances are used only for
    an Application.

##What is ycache-magic interested in?
  For now, ycache-magic interested in the following indications of memcached, and you can specify several
  filters to get the detail indications that you care, e.g. time range, poolName, what kind of keys.

   - Basic statistic. Counter of Gets, Set,..., commands; Hits, hit-rate, fails and so on. I don't want to
     do much for the basic statistic, since there is a lot of tools work pretty well.

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/init-and-basic-statistic.jpg)
   - Show the count of values distribution based on the length of the cache-value. All and Unique(of values)

    X-axis: The length of the value(Int);

    Y-axis: the number of the value on the X-axis specified length.

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/value-len-distribution.jpg)
   - Show the count of keys distribution based on the length of the cache-key. All and Unique (of keys)

    X-axis: The length of the key(Int, (0,250]);

    Y-axis: the number of the keys on the X-axis specified length.

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/key-len-distribution.jpg)
   - Show the count of keys distribution based on the expire . All and Unique (of keys)

    X-axis: The expire of the keys. (Int, [0,30 days]);

    Y-axis: the number of the keys on the X-axis specified length.

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/expire-distribution.jpg)
   - Show the count of commands distribution based on the interval between 'update' and first-N(or last) 'get' the same key .

    X-axis: The interval of two command;

    Y-axis: the number of the command-pairs on the X-axis specified interval.

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/firstN-get-distribution.jpg)
   - Show the count of commands distribution based on the key's life span, and how much benifit(gets) it provide .

    X-axis: The interval of keys' life span;

    Y-axis: the number of the keys/“get” on the X-axis specified interval.

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/lifespan-benifit-distribution.jpg)
   - Show the count of commands distribution based on the interval of update a key with the same value

    X-axis: left is number of continous 'update' for the same value; Right is the interval of 'update' for the same value

    Y-axis: the number of the keys on the X-axis specified value.

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/update-same-value-distribution.jpg)
   - Show result (or error) distribution by time.

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/result-distribution.jpg)
   - Show Data Using A SQL

     ![Init and basic statistics](https://raw.githubusercontent.com/zwChan/ycache-magic/master/example/sql-data.jpg)

## Dependency
 - JDK
 - MAVEN
 - Spark
 - Zeppelin
 - Ycap

##How to use?
  - Download the code from Github: https://github.com/zwChan/ycache-magic.git
  - Make a package using Maven: mvn package, you will get a package named "ycache-magic-0.0.1-SNAPSHOT.jar"
  - Download the Ycap from Github, ?
  - Run Ycap to capture a log file on your memcached server following Ycap's docs.
  - Put the log file to the hadoop hdfs, Ycache-magic will read data from hdfs.
  - Download Zeppelin from Github: https://github.com/NFLabs/zeppelin.git
  - Start your Zeppelin following its docs. Zeppelin use its local Spark service by default, If you want to
    analyze faster, you can attach Zeppelin to a Spark cluster.
  - Add the "ycache-magic-0.0.1-SNAPSHOT.jar" to the configuration of Zeppelin: Add a line
   [export ADD_JARS="/root/scala/event.jar,/root/scala/ycache-magic-0.0.1-SNAPSHOT.jar"] to ${Zeppelin-root}/
   conf/zeppelin-env.sh.
  - Copy the directory [${ycache-magic root}/notebook/2AF7SPXXX] to [${Zeppelin root}/notebook]
  - Restart Zeppelin
  - Go to the website of Zeppelin (http://localhost:8080 by default), get into "Cache Magic" page.
  - Fill the "cache file" with the location of your log file in hdfs, then click the "run" beside the title
    of "Cache Magic"
  - [Option]Specify the poolInfo. The pool info is specify by a file in hdfs. Each line of the file indicates
    the pool that server or client IP/Port belong to. e.g.

  > poolname1 ip1:port1,ip2:port2,ip3:port3,…,

  > poolname2 ip1:port1,ip2:port2,ip3:port3,…,

  - It will search the server [IP:Port+","] first, then search the client [IP+":"]. It don't care the port of client.

## Contributor
  Anyone interested in the project is welcome!


