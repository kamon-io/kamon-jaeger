scalaVersion := "2.12.2"
resolvers += Resolver.mavenLocal
resolvers += Resolver.bintrayRepo("kamon-io", "snapshots")
libraryDependencies += "io.jaegertracing" % "jaeger-core" % "0.27.0"
libraryDependencies += "io.kamon" %% "kamon-core" % "1.0.0"
