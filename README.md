## Tools for querying Spark Dataframes using Sparql:
These tools provide some convenience methods for working with the [Bellman Sparql Engine](https://github.com/gsk-aiops/bellman) 

### Quick usage overview
1. Import the bellman tools libraries:
```scala
import com.gsk.kg.bellman_tools.Utils
import com.gsk.kg.engine.syntax._
implicit val sqlcntx = spark.sqlContext
import org.apache.jena.riot.Lang
org.apache.jena.query.ARQ.init()
```
2. Create a Dataframe:  
You can either load an RDF .nt file using the convenience method (used just below), or you can bring your own bring your own Dataframe with Columns "s", "p", "o":
```scala
val df = Utils.ntToDf("/path/to/your/rdf.nt")
df.printSchema

|-- s: string (nullable = true)
|-- p: string (nullable = true)
|-- o: string (nullable = true)
```

3. You are now ready to query. The com.gsk.kg.engine.syntax._ import above gives us the df.sparql(q:String) method:
```scala
df.sparql("""
SELECT ?s ?p ?o
WHERE { ?s ?p ?o }
LIMIT 10
""")
``` 
Results will be similar to: 
![Sparql query results](https://github.com/gsk-aiops/bellman-tools/blob/main/images/results-example.png?raw=true)


### How to build the tools:  
#### Prerequisites:
* [SBT](https://www.scala-sbt.org/) > 1.3.10
* [GIT](https://git-scm.com/downloads)
* Scala version: 2.11.12
* Spark version 2.4.x

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
