name := "LuciusCore"

version := "1.9.8"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.scalactic"      %% "scalactic"       % "2.2.6"                  ,
  "org.scalatest"      %% "scalatest"       % "2.2.6"      % "test"    ,
  "org.apache.spark"   %% "spark-core"      % "1.6.0"      % "provided"
)

scalacOptions ++= Seq("-unchecked", "-deprecation")

organization := "com.data-intuitive"
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
bintrayPackageLabels := Seq("scala", "l1000", "spark", "lucius")
