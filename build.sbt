name := "LuciusCore"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.scalatest"      %  "scalatest_2.10"  % "2.2.4"      % "test",
  "org.apache.spark"   %% "spark-core"      % "1.4.1"      % "provided"
)


organization := "com.data-intuitive"
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
bintrayPackageLabels := Seq("scala", "l1000", "spark", "lucius")
