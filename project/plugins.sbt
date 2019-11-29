  
resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")

addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.1.0")

addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.2.11")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.3.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.9")
