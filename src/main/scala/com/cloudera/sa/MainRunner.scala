/**
 * Copyright 2015 Sameer Abhyankar <sameer@cloudera.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sa

import com.cloudera.sa.deflators.Deflator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

object MainRunner {
  def main (args: Array[String]) {


    val conf = new Configuration with Serializable
    val sparkConf = new SparkConf
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[Deflator]))
    val sc = new SparkContext(sparkConf)

    val hdfsUtilities = new HDFSUtilities(conf)
    val some = hdfsUtilities.getCompressedPathList(args(0))

    val files = sc.parallelize(some,some.length)
    files.foreach( fname => {
      val zu = new Deflator(conf)
      zu.deflate(fname,args(1))
    })

  }
}
