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

import java.io.{DataInputStream, IOException}

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils


trait Deflatable extends Serializable {

  private final val ReadBufferSize: Int = 1048576
  val fs: FileSystem

  /**
   * The primary deflate method takes in a DataInputStream and writes out an uncompressed file
   * in chunks of ReadBufferSize. This is to allow decompressing large files.
   */

  def deflate(fin: DataInputStream, outputPath: Path) = {

    var fos: FSDataOutputStream = null
    try {
      val buffer = new Array[Byte](ReadBufferSize)
      deleteIfExists(outputPath)

      fos = fs.create(outputPath)
      var bytesRead = 0

      bytesRead = fin.read(buffer)
      while (bytesRead > 0) {
        fos.write(buffer,0,bytesRead)
        bytesRead = fin.read(buffer)
      }

      fos.hflush()

    } catch {
      case e: IOException => throw new IOException("Error in deflating: " + outputPath.toString)

    } finally {
      IOUtils.closeStream(fos)

    }
  }

  private def deleteIfExists(path: Path): Unit = {

    try {
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    } catch {
      case e: IOException => throw new IOException("Unable to delete path: " + path.toString)
    }
  }

  /**
   * Helper method to build a full file Path object from the filename and full path
   */

  def getFullPath(outputFileName: String, outputFilePath: String) = {
    try {
      val newPathName = outputFilePath + "/" + outputFileName
      new Path(newPathName)
    } catch {
      case e: IllegalArgumentException => throw new IllegalArgumentException("Invalid path name")
    }
  }

}
