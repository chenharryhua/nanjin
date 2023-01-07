resolvers ++= Resolver.sonatypeOssRepos("releases")

addDependencyTreePlugin

//addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.1")
addSbtPlugin("com.github.cb372"          % "sbt-explicit-dependencies" % "0.2.16")
addSbtPlugin("de.heikoseeberger"         % "sbt-header"                % "5.9.0")
addSbtPlugin("net.virtual-void"          % "sbt-dependency-graph"      % "0.10.0-RC1")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat"              % "0.4.2")
addSbtPlugin("org.scalameta"             % "sbt-scalafmt"              % "2.5.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.12"

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
