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

import java.io.IOException
import java.util.zip.{ZipException, ZipInputStream}

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

/**
 * Deflator for ZIP compressed files. This uses the ZipInputStream class to
 * decompress the files. This also accounts for a single .ZIP file containing
 * multiple files.
 */
class ZipDeflator(fileSystem: FileSystem) extends Deflatable{

  val fs = fileSystem

  def deflate(inputPath: Path,outputPath: Path) = {

    var zip: ZipInputStream = null
    var fis: FSDataInputStream = null

    try {

      fis = fs.open(inputPath)
      zip = new ZipInputStream(fis)
      var entry = zip.getNextEntry
      while (entry != null) {
        val finalOutPath = getFullPath(entry.getName,outputPath.toString)
        super.deflate(zip,finalOutPath)
        entry = zip.getNextEntry
      }

    } catch {
      case e: ZipException => e.printStackTrace()
      case o: IOException => o.printStackTrace()

    } finally {
      IOUtils.closeStream(zip)
      IOUtils.closeStream(fis)

    }

  }


}
