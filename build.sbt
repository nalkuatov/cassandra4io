name := "cassandra4io"

inThisBuild(
  List(
    organization := "com.ringcentral",
    organizationName := "ringcentral",
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion := crossScalaVersions.value.head,
    crossScalaVersions := Seq("3.3.0", "2.13.10"),
    licenses := Seq(("Apache-2.0", url("https://opensource.org/licenses/Apache-2.0"))),
    homepage := Some(url("https://github.com/ringcentral/cassandra4io")),
    developers := List(
      Developer(id = "narma", name = "Sergey Rublev", email = "alzo@alzo.space", url = url("https://narma.github.io")),
      Developer(
        id = "alexuf",
        name = "Alexey Yuferov",
        email = "aleksey.yuferov@icloud.com",
        url = url("https://github.com/alexuf")
      ),
      Developer(
        id = "nalkuatov",
        name = "Nurlan Alkuatov",
        email = "alkuatovnurlan@gmail.com",
        url = url("https://nalkuatov.kz")
      )
    )
  )
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    IntegrationTest / fork := true,
    libraryDependencies ++= Seq(
      "org.typelevel"   %% "cats-effect"      % "3.5.0",
      "co.fs2"          %% "fs2-core"         % "3.7.0",
      "com.datastax.oss" % "java-driver-core" % "4.15.0"
    ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((3, _)) => Seq("org.typelevel" %% "shapeless3-deriving" % "3.3.0")
      case _            => Seq("com.chuusai" %% "shapeless" % "2.3.10")
    }) ++ Seq(
      "com.disneystreaming" %% "weaver-cats"                    % "0.8.3"   % "it,test",
      "org.testcontainers"   % "testcontainers"                 % "1.18.1"  % "it",
      "com.dimafeng"        %% "testcontainers-scala-cassandra" % "0.40.15" % "it",
      "ch.qos.logback"       % "logback-classic"                % "1.4.7"   % "it,test"
    )
  )

Compile / compile / scalacOptions ++= Seq(
  "-encoding",
  "utf-8",
  "-feature",
  "-unchecked",
  "-deprecation"
) ++
  (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, _)) =>
      List(
        "-Xlint:strict-unsealed-patmat",
        "-Xlint:-serial",
        // "-Ywarn-unused",
        "-Ymacro-annotations",
        "-Yrangepos",
        "-Werror",
        "-explaintypes",
        "-language:higherKinds",
        "-language:implicitConversions",
        "-Xfatal-warnings",
        "-Wconf:any:error"
      )
    case _ => Nil
  })

testFrameworks := Seq(new TestFramework("weaver.framework.CatsEffect"))
