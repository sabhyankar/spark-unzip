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

import java.util.zip.GZIPInputStream

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

/**
 * Deflator for GZIP compressed files. This uses the GZIPInputStream class to
 * decompress the files.
 */

class GzipDeflator(fileSystem: FileSystem) extends Deflatable {
  val fs = fileSystem

  def deflate(inputPath: Path,outputPath: Path) = {

    var gzip: Option[GZIPInputStream]= None
    var fis: Option[FSDataInputStream] = None

    try {

      fis = Some(fs.open(inputPath))
      gzip = Some(new GZIPInputStream(fis.get))

      val fileName = FilenameUtils.getBaseName(inputPath.getName)

      val finalOutPath = getFullPath(fileName,outputPath.toString)
      super.deflate(gzip.get,finalOutPath)

    } finally {
      IOUtils.closeStream(gzip.get)
      IOUtils.closeStream(fis.get)

    }

  }

}
