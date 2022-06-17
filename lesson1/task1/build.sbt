val scioVersion = "0.11.7"
val beamVersion = "2.38.0"

scalaSource in Compile := baseDirectory.value / "src"
scalaSource in Test := baseDirectory.value / "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-example" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % Test,
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
  "org.slf4j" % "slf4j-simple" % "1.7.36"
)