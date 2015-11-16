## A Spark based utility to decompress .ZIP and .GZIP files ##

This utility provides the functionality to decompress a directory containing compressed (.ZIP and .GZIP) files. Currently spark
provides the capability to natively read gzipped file into RDDs. However, .ZIP compressed files cannot be read natively. In addition to that,
.ZIP files are not splittable and are not ideal for parallelism using mapreduce.

The following sample invocation shows how a directory consisting of compressed files can be uncompressed:

###Source directory:###

    $ hdfs dfs -ls /user/sameer/zipIn
    Found 5 items
    -rw-r--r--   3 sameer developers        191 2015-11-04 22:57 /user/sameer/zipIn/Afile_1.TXT.zip
    -rw-r--r--   3 sameer developers        191 2015-11-04 22:57 /user/sameer/zipIn/Afile_2.TXT.zip
    -rw-r--r--   3 sameer developers        191 2015-11-04 22:57 /user/sameer/zipIn/Afile_3.TXT.zip
    -rw-r--r--   3 sameer developers  283485565 2015-11-10 18:03 /user/sameer/zipIn/abigfile.zip
    -rw-r--r--   3 sameer developers  283485431 2015-11-10 18:03 /user/sameer/zipIn/some-other-part-m.txt.gz

The sample driver is a simple singleton with a main method which takes in two inputs:
  1. An input HDFS directory
  2. An output HDFS directory

```
package com.cloudera.sa
import com.cloudera.sa.deflators.Deflator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

object MainRunner {
  def main (args: Array[String]) {


    val conf = new Configuration with Serializable
    val sparkConf = new SparkConf
    sparkConf.registerKryoClasses(Array(classOf[Deflator]))
    val sc = new SparkContext(sparkConf)

    val hdfsUtilities = new HDFSUtilities(conf)
    val some = hdfsUtilities.getCompressedPathList(args(0))

    val files = sc.parallelize(some,some.length)
    files.foreach { fname =>
      {
        val zu = new Deflator(conf)
        zu.deflate(fname, args(1))
      }
    }
  }
}
```

A more sophisticated driver class can be used or the Deflator classes can be used directly.
Once the mvn package is built, the sample invocation is:

```
$ spark-submit --master yarn --class com.cloudera.sa.MainRunner /tmp/spark-unzip-1.0-SNAPSHOT-jar-with-dependencies.jar /user/sameer/zipIn /user/sameer/zipOut --executor-memory 2G --driver-memory 2G --num-executors 5
```

The `output directory - /user/sameer/zipOut` will have the uncompressed files.

###Output Directory###
```
[sameer@ip-192-168-100-179 ~]$ hdfs dfs -ls /user/sameer/zipOut
Found 5 items
-rw-r--r--   3 sameer developers         26 2015-11-16 12:31 /user/sameer/zipOut/Afile_1.TXT
-rw-r--r--   3 sameer developers         26 2015-11-16 12:31 /user/sameer/zipOut/Afile_2.TXT
-rw-r--r--   3 sameer developers         26 2015-11-16 12:31 /user/sameer/zipOut/Afile_3.TXT
-rw-r--r--   3 sameer developers 1000000000 2015-11-16 12:31 /user/sameer/zipOut/part-m-00000
-rw-r--r--   3 sameer developers 1000000000 2015-11-16 12:31 /user/sameer/zipOut/some-other-part-m.txt
```