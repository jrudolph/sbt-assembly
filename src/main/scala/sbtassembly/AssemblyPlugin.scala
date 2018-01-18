package sbtassembly

import sbt._
import Keys._
import scala.io.Source

object AssemblyPlugin extends sbt.AutoPlugin {
  override def requires = plugins.JvmPlugin
  override def trigger = allRequirements

  object autoImport extends AssemblyKeys {
    val Assembly = sbtassembly.Assembly
    val MergeStrategy = sbtassembly.MergeStrategy
    val PathList = sbtassembly.PathList
    val baseAssemblySettings = AssemblyPlugin.baseAssemblySettings
    val ShadeRule = sbtassembly.ShadeRule
  }
  import autoImport.{ Assembly => _, baseAssemblySettings => _, _ }

  val defaultShellScript: Seq[String] = Seq("#!/usr/bin/env sh", """exec java -jar "$0" "$@"""") // "

  override lazy val projectSettings: Seq[Def.Setting[_]] = assemblySettings

  lazy val baseAssemblySettings: Seq[sbt.Def.Setting[_]] = Seq(
    assembly := Assembly.assemblyTask(assembly).value,
    fastAssembly := Assembly.assemblyTask2(assembly).value,
    logLevel in assembly := Level.Info,
    assembledMappings in assembly                   := Assembly.assembledMappingsTask(assembly).value,
    assemblyPackageScala                            := Assembly.assemblyTask(assemblyPackageScala).value,
    assembledMappings in assemblyPackageScala       := Assembly.assembledMappingsTask(assemblyPackageScala).value,
    assemblyPackageDependency                       := Assembly.assemblyTask(assemblyPackageDependency).value,
    assembledMappings in assemblyPackageDependency  := Assembly.assembledMappingsTask(assemblyPackageDependency).value,

    // test
    test in assembly := (test in Test).value,
    test in assemblyPackageScala := (test in assembly).value,
    test in assemblyPackageDependency := (test in assembly).value,

    // assemblyOption
    assembleArtifact in packageBin := true,
    assembleArtifact in assemblyPackageScala := true,
    assembleArtifact in assemblyPackageDependency := true,
    assemblyMergeStrategy in assembly := MergeStrategy.defaultMergeStrategy,
    assemblyShadeRules in assembly := Seq(),
    assemblyExcludedJars in assembly := Nil,
    assemblyOption in assembly := {
      val s = streams.value
      AssemblyOption(
        assemblyDirectory  = s.cacheDirectory / "assembly",
        includeBin         = (assembleArtifact in packageBin).value,
        includeScala       = (assembleArtifact in assemblyPackageScala).value,
        includeDependency  = (assembleArtifact in assemblyPackageDependency).value,
        mergeStrategy      = (assemblyMergeStrategy in assembly).value,
        excludedJars       = (assemblyExcludedJars in assembly).value,
        excludedFiles      = Assembly.defaultExcludedFiles,
        cacheOutput        = true,
        cacheUnzip         = true,
        appendContentHash  = false,
        prependShellScript = None,
        shadeRules         = (assemblyShadeRules in assembly).value,
        level              = (logLevel in assembly).value)
    },

    assemblyOption in assemblyPackageScala := {
      val ao = (assemblyOption in assembly).value
      ao.copy(includeBin = false, includeScala = true, includeDependency = false)
    },
    assemblyOption in assemblyPackageDependency := {
      val ao = (assemblyOption in assembly).value
      ao.copy(includeBin = false, includeScala = true, includeDependency = true)
    },

    // packageOptions
    packageOptions in assembly <<= (packageOptions in (Compile, packageBin), mainClass in assembly) map { (os, mainClass) =>
      mainClass map { s =>
        Package.MainClass(s) +: (os filterNot {_.isInstanceOf[Package.MainClass]})
      } getOrElse {os}
    },
    packageOptions in assemblyPackageScala      := (packageOptions in (Compile, packageBin)).value,
    packageOptions in assemblyPackageDependency := (packageOptions in (Compile, packageBin)).value,

    // ouputPath
    assemblyOutputPath in assembly                  := { (target in assembly).value / (assemblyJarName in assembly).value },
    assemblyOutputPath in assemblyPackageScala      := { (target in assembly).value / (assemblyJarName in assemblyPackageScala).value },
    assemblyOutputPath in assemblyPackageDependency := { (target in assembly).value / (assemblyJarName in assemblyPackageDependency).value },
    target in assembly <<= crossTarget,

    assemblyJarName in assembly                   <<= (assemblyJarName in assembly)                  or (assemblyDefaultJarName in assembly),
    assemblyJarName in assemblyPackageScala       <<= (assemblyJarName in assemblyPackageScala)      or (assemblyDefaultJarName in assemblyPackageScala),
    assemblyJarName in assemblyPackageDependency  <<= (assemblyJarName in assemblyPackageDependency) or (assemblyDefaultJarName in assemblyPackageDependency),

    assemblyDefaultJarName in assemblyPackageScala      <<= (scalaVersion) map { (scalaVersion) => "scala-library-" + scalaVersion + "-assembly.jar" },
    assemblyDefaultJarName in assemblyPackageDependency <<= (name, version) map { (name, version) => name + "-assembly-" + version + "-deps.jar" },
    assemblyDefaultJarName in assembly                  <<= (name, version) map { (name, version) => name + "-assembly-" + version + ".jar" },

    mainClass in assembly <<= mainClass or (mainClass in Runtime),

    fullClasspath in assembly <<= fullClasspath or (fullClasspath in Runtime),

    externalDependencyClasspath in assembly <<= externalDependencyClasspath or (externalDependencyClasspath in Runtime)
  )

  lazy val assemblySettings: Seq[sbt.Def.Setting[_]] = baseAssemblySettings
}

case class AssemblyOption(assemblyDirectory: File,
  // include compiled class files from itself or subprojects
  includeBin: Boolean = true,
  includeScala: Boolean = true,
  // include class files from external dependencies
  includeDependency: Boolean = true,
  excludedJars: Classpath = Nil,
  excludedFiles: Seq[File] => Seq[File] = Assembly.defaultExcludedFiles, // use mergeStrategy instead
  mergeStrategy: String => MergeStrategy = MergeStrategy.defaultMergeStrategy,
  cacheOutput: Boolean = true,
  cacheUnzip: Boolean = true,
  appendContentHash: Boolean = false,
  prependShellScript: Option[Seq[String]] = None,
  shadeRules: Seq[ShadeRule] = Seq(),
  level: Level.Value)
