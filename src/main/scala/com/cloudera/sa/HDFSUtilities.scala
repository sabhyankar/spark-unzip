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

import java.io.{ByteArrayOutputStream, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path, FileSystem}
import org.apache.hadoop.io.IOUtils

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

  def getPathList(pathString: String, filter: String => Boolean): Seq[String] = {
    require(pathString != null, "File path cannot be null")
    require(isValidPath(pathString), "Invalid file path: " + pathString)

      val path = getPath(pathString)
      val listStatus = fs.listStatus(path)
      for {
        fileStatus <- listStatus
        if fileStatus.isFile
        if filter(fileStatus.getPath.getName)
      }
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
    val compressedFilter = (f: String) => f.toLowerCase().endsWith(".zip") || f.toLowerCase().endsWith(".gz")
    getPathList(pathString,compressedFilter)
  }

  def getFileContents(pathString: String): Option[String] = {
    require(pathString != null, "File path cannot be null")
    require(isValidPath(pathString),"Invalid file path: " + pathString)

    var fis: Option[FSDataInputStream] = None
    val bos: Option[ByteArrayOutputStream] = None
    try {
      val path = getPath(pathString)
      val status = fs.getFileStatus(path)
      val len = status.getLen
      val bytes = new Array[Byte](len.toInt)
      fis = Some(fs.open(path))
      IOUtils.readFully(fis.get,bytes,0,len.toInt)
      Some(new String(bytes))

    } catch {
      case e: Exception => throw new Exception("Exception occurred in getFileContents: " + pathString)

    } finally {
      fis match {
        case Some(i) => IOUtils.closeStream(i)
        case None => {}
      }
    }
  }


}
