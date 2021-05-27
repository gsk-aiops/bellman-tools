// Databricks notebook source
/**
* This is a simple disease semantic search demonstration using Sparql and the Bellman libraries
* Instructions for building these tools can be found here: https://github.com/gsk-aiops/bellman-tools
* The Bellman OSS Sparql engine can be found here: https://github.com/gsk-aiops/bellman
* Documentation for the Bellman Sparql engine can be found here: https://gsk-aiops.github.io/bellman/
* After building the assembly jar, you can attach it to your running cluster.
**/

//First import the Bellman jars we need. These tools will give us the df.sparql() syntax for easily querying s,p,o column dataframes using sparql, along with some convenience methods to convert .nt files to s,p,o dataframes
import com.gsk.kg.bellman_tools.Utils
import com.gsk.kg.engine.syntax._
implicit val sqlcntx = spark.sqlContext

import org.apache.jena.riot.Lang
org.apache.jena.query.ARQ.init()

// COMMAND ----------

/**
The Wikimedia data used in this demo can be found here:
https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.bz2

The size is ~30GB so be sure to have enough disk space!

After downloading and then staging the data either in HDFS, or an S3 bucket or an Azure blob store, you must convert the data to parquet format. Bellman tools has a method for converting .nt to a dataframe. You can either query this dataframe directly or save it to Parquet for querying later. Here we are electing to do the latter in case we'd like to come back and read directly from Parquet.
**/
val dfp = Utils.ntToDf("/path/to/your/staged/latest-truthy.nt.bz2")

//Or if you've previously saved the data you can load it from parquet:
//val dfp = spark.read.parquet("/path/to/your/staged/latest-truthy")

// COMMAND ----------

//This will speed up our subsequent queries
dfp.cache
dfp.count

// COMMAND ----------

dfp.printSchema

// COMMAND ----------

//Display first 10 triples in the knowledge graph
display(dfp.select("*").limit(10))

// COMMAND ----------

//Use sparql syntax to view the first 10 triples
display(dfp.sparql("""
SELECT ?s ?p ?o
WHERE { ?s ?p ?o}
LIMIT 10
"""))

// COMMAND ----------

//Find all gene variants that are positive prognostic indicators of pancreatic cancer. While we are at it, also get the gene
//Check against Wikidata sparql endpoint to ensure consistent results
//This query shows us there is one gene variant that has a positiv prognostic association
display(dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT ?variant ?disease ?gene
WHERE
{ 

    ?variant wdt:P3358 wd:Q212961 ; # P3358 Positive prognostic predictor
          wdt:P3433 ?gene . # P3433 biological variant of
    
  BIND(wd:Q212961 AS ?disease)

}
LIMIT 10
"""))

// COMMAND ----------

// MAGIC %md
// MAGIC [Check above query against Wikidata](https://query.wikidata.org/#PREFIX%20%20schema%3A%20%3Chttp%3A%2F%2Fschema.org%2F%3E%0APREFIX%20%20rdf%3A%20%20%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0APREFIX%20%20xml%3A%20%20%3Chttp%3A%2F%2Fwww.w3.org%2FXML%2F1998%2Fnamespace%3E%0APREFIX%20%20xsd%3A%20%20%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23%3E%0APREFIX%20wdt%3A%20%3Chttp%3A%2F%2Fwww.wikidata.org%2Fprop%2Fdirect%2F%3E%0APREFIX%20wd%3A%20%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2F%3E%0A%0ASELECT%20%3Fvariant%20%3Fdisease%20%3Fgene%0AWHERE%0A%7B%20%0A%0A%20%20%20%20%3Fvariant%20wdt%3AP3358%20wd%3AQ212961%20%3B%20%23%20P3358%20Positive%20prognostic%20predictor%0A%20%20%20%20%20%20%20%20%20%20wdt%3AP3433%20%3Fgene%20.%20%23%20P3433%20biological%20variant%20of%0A%20%20%20%20%0A%20%20BIND%28wd%3AQ212961%20AS%20%3Fdisease%29%0A%0A%7D%0ALIMIT%2010)

// COMMAND ----------

//Generalize and give me all gene variants that have a positive prognostic indicator, and also give me the gene.
display(dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT ?variant ?disease ?gene
WHERE
{ 
    ?variant wdt:P3358 ?disease ; # P3358 positive prognostic indicator
      wdt:P3433 ?gene .    
}
"""))


// COMMAND ----------

//Give me all gene variants that have a positive therapeudic indicator. Also give me the gene
display(dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT ?variant ?disease ?gene
WHERE
{ 

    ?variant wdt:P3354 ?disease ; # P3354 positive therapeudic indicator
      wdt:P3433 ?gene .    
}
"""))

// COMMAND ----------

//Give me all treatments for all diseases
display(dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>

SELECT ?disease ?treatment
WHERE
{ 

    ?disease wdt:P2176 ?treatment ; # P2176 drug used for treatment

}
"""))

// COMMAND ----------

//Put it all together. Create the subgraph we'd like to query. Use publicly available ontologies for standardizing our mappings.
val graphDf = dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX ensembl: <http://www.ensembl.org/id/> #Ensembl URI. Genome browser
PREFIX dm: <http://id.gsk.com/>
PREFIX do: <http://purl.obolibrary.org/obo/DOID_> #Disease ontology
PREFIX skos: <http://www.w3.org/2004/02/skos/core#> #Simple Knowledge Organization Namespace

CONSTRUCT {
  #Here we are using the disease ontology and SKOS ontologies to align some of our entities
  ?disease rdf:type do:4 . # Use the disease ontology for classifying disease
  ?disease skos:exactMatch ?diseaseOntologyId .
  ?disease rdf:label ?diseaseLabel .
  ?disease dm:associated_gene ?gene .
  ?disease dm:associated_gene_variant ?variant .
  ?disease dm:associated_treatment ?treatment .
}

WHERE
{    
  ?variant wdt:P3358 ?disease . # That also may have a positive prognostic indicator
  ?variant wdt:P3354 ?treatment . # That also may have a positive therapeudic predictor
  ?variant wdt:P3433 ?gene . #That have associated genes    
  ?gene wdt:P703 ?Q15978631 . #Get only homo-sapien genes
  ?gene wdt:P594 ?geneEnsemblId  . #That have an ensemblIdentifier for each gene match

  ?treatment wdt:P2175 ?disease . #And include only treatments that have a positive therapeudiic and positive prognostic indicator
  ?disease wdt:P699 ?doid . # Get disease ontology classifications that match the disease
  ?disease rdf:label ?diseaseLabel
  FILTER regex(?diseaseLabel, "@en$")
     
  BIND(URI(CONCAT("http://www.ensembl.org/id/", ?geneEnsemblId)) as ?geneEnsemblIdURI) .
  BIND(URI(CONCAT("https://disease-ontology.org/?id=", ?doid)) as ?diseaseOntologyId) .  
} 
""").cache

// COMMAND ----------

display(graphDf)

// COMMAND ----------

//Define our disease search function

import org.apache.spark.sql.DataFrame

def litSearch(q:String, g:DataFrame):DataFrame = {
  val str = "\"" + q + "\"@en" 
  val query = s"""
    PREFIX  schema: <http://schema.org/>
    PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
    PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX ensembl: <http://www.ensembl.org/id/>
    PREFIX so: <http://purl.obolibrary.org/obo/SO_>
    PREFIX dm: <http://id.gsk.com/>
    PREFIX do: <http://purl.obolibrary.org/obo/DOID_>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT DISTINCT ?disease ?diseaseName ?associatedGene ?associatedGeneVariant ?associatedGeneVariantTreatment
    {
      ?disease rdf:type do:4 .
      ?disease rdf:label ?diseaseName .
      ?disease dm:associated_gene_variant ?associatedGeneVariant .
      ?disease dm:associated_gene ?associatedGene .
      ?disease dm:associated_treatment ?associatedGeneVariantTreatment .
      FILTER regex(?diseaseName, $str)
    }"""
  
  g.sparql(query)
}

// COMMAND ----------

display(litSearch("pancreatic cancer", graphDf))

// COMMAND ----------

display(litSearch("breast cancer", graphDf))
