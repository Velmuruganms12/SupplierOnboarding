# Product Onboarding
Application for Onboarding new Supplier Product


compile the project:

$ sbt compile

Run the project:

$ sbt run



Spark Version 2.4.0


Running Spark on Local mode 
 lazy val sparkConf = new SparkConf().setAppName("OnboardingApp").setMaster("local[*]").set("spark.cores.max", "2")
