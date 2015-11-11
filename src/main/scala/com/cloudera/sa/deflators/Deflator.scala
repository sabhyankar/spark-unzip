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

package com.cloudera.sa.deflators

import com.cloudera.sa.HDFSUtilities
import com.cloudera.sa.simpleExceptions.InvalidCompressionTypeException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Helper class to determine which deflator class and deflate method to use
 */

class Deflator (conf: Configuration) extends Serializable  {

  private val fs: FileSystem = FileSystem.get(conf)
  private val hdfs: HDFSUtilities = new HDFSUtilities(conf)


  def deflate(inputFile: String, outputFolder: String) = {
    if ( !hdfs.isValidPath(inputFile))
      throw new IllegalArgumentException("Invalid path: " + inputFile)

    val lcInputFile = inputFile.toLowerCase

    if ( lcInputFile.endsWith(".zip")) {

      val zipDeflator = new ZipDeflator(fs)
      zipDeflator.deflate(new Path(inputFile),new Path(outputFolder))

    } else if ( lcInputFile.endsWith(".gz")) {
      val gzipDeflator = new GzipDeflator(fs)
      gzipDeflator.deflate(new Path(inputFile),new Path(outputFolder))

    } else {
      throw InvalidCompressionTypeException("Invalid file extension for: " + inputFile)
    }


  }

}