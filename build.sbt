name := "OneDotProject"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark"  %%  "spark-core"    % "2.4.0",
  "org.apache.spark"  %%  "spark-sql"     % "2.4.0",
  "org.apache.spark"  %%  "spark-mllib"   % "2.4.0",
  "io.delta"  %%  "delta-core"  %  "0.4.0",
  "com.crealytics" %% "spark-excel" % "0.12.3"
)