  
resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.3")

addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.2.16")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.5")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.9"
