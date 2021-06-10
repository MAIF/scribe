import sbt.Keys.organization
import Dependencies._

organization := "fr.maif"

name := "thoth-jooq-async"

libraryDependencies ++= Seq(
  "io.vavr"            % "vavr"                    % vavrVersion,
  "org.jooq"           % "jooq"                    % jooqVersion,
  "fr.maif"           %% "jooq-async-api"          % jooqAsyncVersion,
  "fr.maif"           %% "jooq-async-jdbc"         % jooqAsyncVersion % Test,
  "fr.maif"           %% "jooq-async-reactive"     % jooqAsyncVersion % Test,
  "org.projectlombok"  % "lombok"                  % "1.18.18",
  "org.assertj"        % "assertj-core"            % "3.10.0"         % Test,
  "org.postgresql"     % "postgresql"              % "42.2.5"         % Test,
  "org.junit.platform" % "junit-platform-launcher" % "1.4.2"          % Test,
  "org.junit.platform" % "junit-platform-commons"  % "1.4.2"          % Test,
  "org.junit.jupiter"  % "junit-jupiter-engine"    % "5.4.2"          % Test,
  "org.junit.vintage"  % "junit-vintage-engine"    % "5.4.2"          % Test,
  "net.aichler"        % "jupiter-interface"       % "0.9.1"          % Test,
  "org.mockito"        % "mockito-core"            % "2.22.0"    % Test,
  "org.testng"         % "testng"                  % "6.3"       % Test,
  "org.testcontainers" % "postgresql"              % "1.15.0"    % Test,
  "org.testcontainers" % "kafka"                   % "1.15.0"    % Test
)


testNGSuites := Seq(((resourceDirectory in Test).value / "testng.xml").absolutePath)

javacOptions in Compile ++= Seq(
  "-source",
  "8",
  "-target",
  "8",
  "-Xlint:unchecked",
  "-Xlint:deprecation"
)

// Skip the javadoc for the moment
sources in (Compile, doc) := Seq.empty
