package sbtassembly

import java.io._
import java.util.zip.{ZipEntry, ZipOutputStream, ZipFile}

import sbt.Keys.Classpath
import sbt.classpath.ClasspathUtilities
import sbt.{Attributed, Logger, PackageOption}

import scala.annotation.tailrec

object FastAssembly {
  def apply(out0: File, ao: AssemblyOption, po: Seq[PackageOption], classpath: Classpath, log: Logger): File = {
    println(classpath)

    import collection.JavaConverters._
    def traverseJar(jarFile: File): Seq[ClassPathFile] = {
      val zipFile = new ZipFile(jarFile)
      zipFile.entries().asScala.toVector.filterNot(_.isDirectory).map { entry =>
        val path = entry.getName
        ClassPathFile(path, FromJar(jarFile, path))
      }
    }
    def traverseDir(dir: File): Seq[ClassPathFile] = {
      def rec(entry: File, prefix: String, first: Boolean): Stream[ClassPathFile] = {
        require(entry.isDirectory)
        val newPrefix = if (first) prefix else prefix + entry.getName + "/"
        entry.listFiles().filterNot(d => d.getName == "." || d.getName == "..")
          .toStream
          .flatMap { f =>
            if (f.isFile) Stream(ClassPathFile(prefix + f.getName, FromDir(f)))
            else rec(f, newPrefix, false)
          }
      }

      rec(dir, "", first = true).toVector
    }

    def handleEntry(classpathEntry: Attributed[File]): Seq[ClassPathFile] = {
      val file = classpathEntry.data
      if (file.isDirectory) traverseDir(file)
      else if (ClasspathUtilities.isArchive(file)) traverseJar(file)
      else throw new RuntimeException(s"Unknown file type: $file")
    }

    val allEntries = classpath.flatMap(handleEntry)
    println(s"Found ${allEntries.size} entries!")
    allEntries.streamToFile("entries.txt")

    val newEntries: Seq[(String, Seq[ClassPathFile])] =
      allEntries.groupBy(_.name).toVector

    val zipOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(out0), 1000000))
    try {
      val buffer = new Array[Byte](65536)
      def writeEntry(name: String, files: Seq[ClassPathFile]): Unit = {
        def dataFor(cpf: ClassPathFile): (ZipEntry, InputStream) =
        cpf.origin match {
          case FromDir(file) =>
            val newEntry = new ZipEntry(cpf.name)
            newEntry.setTime(file.lastModified)
            newEntry.setSize(file.length())

            val dataStream = new FileInputStream(file)
            (newEntry, dataStream)
          case FromJar(jar, path) =>
            val zipFile = new ZipFile(jar)
            val oldEntry = zipFile.getEntry(path)
            val zin = zipFile.getInputStream(oldEntry)
            val newEntry = new ZipEntry(oldEntry)
            val res = (newEntry, zin)
            //FIXME: when to do this? zipFile.close()
            res
        }

        def singleEntry(file: ClassPathFile): (ZipEntry, Seq[InputStream]) = {
          val (zipEntry, stream) = dataFor(file)
          (zipEntry, Seq(stream))
        }
        val noEntry = (null, Nil)

        val (entry, streams) =
          files match {
            case single +: Nil => singleEntry(single)
            case doubles =>
              val strategy = ao.mergeStrategy(name)
              println(s"Merging ${doubles.size} entries named '$name' with strategy $strategy found at")
              doubles.foreach(d => println(d.origin))
              strategy match {
                case MergeStrategy.first => singleEntry(doubles.head)
                case MergeStrategy.last => singleEntry(doubles.last)
                case MergeStrategy.discard => noEntry
                case MergeStrategy.concat =>
                  val all = doubles.map(dataFor)
                  val first = all.head._1
                  val newEntry = new ZipEntry(first)
                  newEntry.setSize(all.map(_._1.getSize).sum)
                  newEntry.setCompressedSize(-1)
                  (newEntry, all.map(_._2))
                case MergeStrategy.singleOrError => throw new RuntimeException(s"found multiple files for same target path: $name")
                case _ => singleEntry(doubles.head)
              }
          }

        if (streams.nonEmpty) {
          zipOut.putNextEntry(entry)
          streams.foreach(copy(_, zipOut, buffer))
        }
      }


      newEntries.foreach((writeEntry _).tupled)

    } catch {
      case e: Throwable =>
      e.printStackTrace()
    } finally zipOut.close()

    out0
  }

  def copy(from: InputStream, to: OutputStream, buffer: Array[Byte]): Unit = {
    @tailrec def rec(): Unit = {
      val read = from.read(buffer)
      if (read > 0) {
        to.write(buffer, 0, read)
        rec()
      }
    }

    try rec()
    finally from.close()
  }

  implicit class EnhancedTraversableOnce[T](val trav: TraversableOnce[T]) extends AnyVal {
    def streamToFile(fileName: String): Unit = {
      val outFile = new FileWriter(fileName)
      try trav.foreach { t =>
        outFile.write(t.toString)
        outFile.write('\n')
      }
      finally outFile.close()
    }
  }

  def fileNameFromPath(path: String): String = {
    val i = path.lastIndexOf('/')
    if (i >= 0) path.substring(i + 1)
    else path
  }


  case class ClassPathFile(name: String, origin: FileOrigin)

  sealed trait FileOrigin
  case class FromJar(file: File, path: String) extends FileOrigin
  case class FromDir(file: File) extends FileOrigin
}