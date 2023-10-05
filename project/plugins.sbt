resolvers ++= Resolver.sonatypeOssRepos("releases")

addDependencyTreePlugin

addSbtPlugin("org.scoverage"     % "sbt-scoverage"             % "2.0.9")
addSbtPlugin("com.github.cb372"  % "sbt-explicit-dependencies" % "0.3.1")
addSbtPlugin("de.heikoseeberger" % "sbt-header"                % "5.10.0")
addSbtPlugin("net.virtual-void"  % "sbt-dependency-graph"      % "0.10.0-RC1")
addSbtPlugin("org.typelevel"     % "sbt-tpolecat"              % "0.5.0")
addSbtPlugin("org.scalameta"     % "sbt-scalafmt"              % "2.5.2")
addSbtPlugin("com.timushev.sbt"  % "sbt-updates"               % "0.6.4")
addSbtPlugin("com.orrsella"      % "sbt-stats"                 % "1.0.7")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.13"

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
