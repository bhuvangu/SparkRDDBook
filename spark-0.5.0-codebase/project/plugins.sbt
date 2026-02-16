resolvers ++= Seq(
  "sbt-idea-repo" at "https://mpeltonen.github.com/maven/",
  "typesafe-releases" at "https://repo.typesafe.com/typesafe/releases/",
  Classpaths.typesafeResolver
)

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "0.11.0")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse" % "1.4.0")

addSbtPlugin("com.eed3si9n" %% "sbt-assembly" % "0.7.2")
