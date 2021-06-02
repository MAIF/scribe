import Dependencies._

organization := "fr.maif"

name := "thoth-tck"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.assertj"        % "assertj-core"            % "3.10.0",
  "org.testng"         % "testng"                  % "6.3",
  "com.typesafe.akka"  %% "akka-stream"            % akkaVersion % Test,
  "org.mockito"        % "mockito-core"            % "3.6.28"    % Test,
  "fr.maif"           %% "jooq-async-api"          % jooqAsyncVersion
)

testNGSuites := Seq(((resourceDirectory in Test).value / "testng.xml").absolutePath)
