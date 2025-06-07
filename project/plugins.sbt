resolvers ++= Resolver.sonatypeOssRepos("releases")

addDependencyTreePlugin

addSbtPlugin("org.scoverage"     % "sbt-scoverage"             % "2.3.1")
addSbtPlugin("com.github.cb372"  % "sbt-explicit-dependencies" % "0.3.1")
addSbtPlugin("de.heikoseeberger" % "sbt-header"                % "5.10.0")
addSbtPlugin("org.typelevel"     % "sbt-tpolecat"              % "0.5.2")
addSbtPlugin("org.scalameta"     % "sbt-scalafmt"              % "2.5.4")
addSbtPlugin("com.timushev.sbt"  % "sbt-updates"               % "0.6.4")
addSbtPlugin("com.orrsella"      % "sbt-stats"                 % "1.0.7")
addSbtPlugin("com.eed3si9n"      % "sbt-buildinfo"             % "0.13.1")
addSbtPlugin("com.github.sbt"    % "sbt-git"                   % "2.1.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.8")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
