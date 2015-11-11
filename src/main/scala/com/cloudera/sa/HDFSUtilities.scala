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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Helper class with utilities to work with HDFS filesystem.
 *
 */
class HDFSUtilities(conf: Configuration) {

  val fs: FileSystem = FileSystem.get(conf)

  def isValidPath(pathString: String): Boolean = {
    try {
      val path = getPath(pathString)
      fs.exists(path)

    } catch {
      case e: IOException => throw new IOException("IOException accessing path: " + pathString)
    }
  }

  def getPath(pathString: String): Path = {
    try {
      new Path(pathString)

    } catch {
      case e: IllegalArgumentException => throw new IllegalArgumentException("Invalid path: " + pathString)
    }
  }

  def getPathList(pathString: String): Seq[String] = {
    require(isValidPath(pathString))

      val path = getPath(pathString)
      val listStatus = fs.listStatus(path)
      for (fileStatus <- listStatus; if fileStatus.isFile)
        yield fileStatus.getPath.getParent.toString +'/' + fileStatus.getPath.getName

    }

  def isDir(pathString: String): Boolean = {
    try {
      val path = getPath(pathString)
      fs.isDirectory(path)
    } catch {
      case e: IOException => throw new IOException("IOException accessing path: " + pathString)
    }
  }

  def getCompressedPathList(pathString: String): Seq[String] = {
    getPathList(pathString).filter( f => (f.toLowerCase.endsWith(".zip") || f.toLowerCase.endsWith(".gz")))
  }

}
