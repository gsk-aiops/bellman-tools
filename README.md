### Prerequisites:
##### SBT
* [SBT](https://www.scala-sbt.org/) > 1.3.10
* [GIT](https://git-scm.com/downloads)

#### Steps to build tools
* Clone this repo:  
`git clone git@github.com:gsk-aiops/bellman-tools.git`

* CD into directory:  
`cd bellman-tools`

* Build assembly .jar (build usually runs for about 5 minutes, possibly more):  
`sbt 'set test in assembly := {}' clean assembly`

* If assembly was successful, upload assembly .jar to a running cluster in Databricks, or load into a spark-shell session,
or include in a spark-submit job. Assembly .jar can be found in target/scala-2.11/bellman-tools-assembly-0.1.jar

* A demo Databricks notebook can be found in the [notebooks](notebooks) directory. 
