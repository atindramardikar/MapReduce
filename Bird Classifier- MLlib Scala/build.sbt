
lazy val root = (project in file(".")).
    settings(
        name := "CreateModel",
        libraryDependencies += ("org.apache.spark" %% "spark-mllib" % "2.0.0")
    )
