package sbtassembly

import sbt._
import Keys._

trait AssemblyKeys {
  lazy val assembly                  = taskKey[File]("Builds a deployable fat jar.")
  lazy val fastAssembly                  = taskKey[File]("Builds a deployable fat jar (fast).")
  lazy val assembleArtifact          = settingKey[Boolean]("Enables (true) or disables (false) assembling an artifact.")
  lazy val assemblyOption            = taskKey[AssemblyOption]("Configuration for making a deployable fat jar.")
  lazy val assembledMappings         = taskKey[Seq[MappingSet]]("Keeps track of jar origins for each source.")

  lazy val assemblyPackageScala      = taskKey[File]("Produces the scala artifact.")
  @deprecated("Use assemblyPackageScala", "0.12.0")
  lazy val packageScala              = assemblyPackageScala

  lazy val assemblyPackageDependency = taskKey[File]("Produces the dependency artifact.")
  @deprecated("Use assemblyPackageDependency", "0.12.0")
  lazy val packageDependency         = assemblyPackageDependency

  lazy val assemblyJarName           = taskKey[String]("name of the fat jar")
  @deprecated("Use assemblyJarName", "0.12.0")
  lazy val jarName                   = assemblyJarName

  lazy val assemblyDefaultJarName    = taskKey[String]("default name of the fat jar")
  @deprecated("Use assemblyDefaultJarName", "0.12.0")
  lazy val defaultJarName            = assemblyDefaultJarName

  lazy val assemblyOutputPath        = taskKey[File]("output path of the fat jar")
  @deprecated("Use assemblyOutputPath", "0.12.0")
  lazy val outputPath                = assemblyOutputPath

  lazy val assemblyExcludedJars      = taskKey[Classpath]("list of excluded jars")
  @deprecated("Use assemblyExcludedJars", "0.12.0")
  lazy val excludedJars              = assemblyExcludedJars

  lazy val assemblyMergeStrategy     = settingKey[String => MergeStrategy]("mapping from archive member path to merge strategy")
  @deprecated("Use assemblyMergeStrategy", "0.12.0")
  lazy val mergeStrategy             = assemblyMergeStrategy

  lazy val assemblyShadeRules        = settingKey[Seq[ShadeRule]]("shading rules backed by jarjar")
}
object AssemblyKeys extends AssemblyKeys

// Keep track of the source package of mappings that come from a jar, so we can
// sha1 the jar instead of the unpacked packages when determining whether to rebuild
case class MappingSet(sourcePackage : Option[File], mappings : Vector[(File, String)]) {
  def dependencyFiles: Vector[File] =
    sourcePackage match {
      case Some(f)  => Vector(f)
      case None     => mappings.map(_._1)
    }
}
