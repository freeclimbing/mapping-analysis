Explanation "Workflow compute Connected Components with flink" 
+ Visualization of results with Neo4j
+ Download+Import Bioportal data
+ ggf. Clique computation


0) create a database and configure db.properties
1) run DownloadBioportalMappings (includes: run the given create table statements to save the links) 
2) run DownloadBioportalOntologies (includes: run the given create table statements to save ontology metadata)
OR SKIP 1) - 2) if files are already downloaded

3) run ImportMappingsToDB - see docu in class
4) run ImportOntologiesToDB - see docu in class
5) Bioportal gives us URIs, but we need integer to run CC algorithm in flink 

  Run GenerateConceptAndLinkTableWitIhDs.java

OR SKIP 3) - 5) and import data somehow different (but use same db schema as used in 3) + 4) + 5))
    
6) get integer IDs for edges and vertices and save to two files

-- edges
SELECT CONCAT(l.`srcID`, " ", l.`trgID`) 
FROM `linksWithIDs` l;

//some links might be contained in more than one mapping (different linking methods), e.g. SAME_URI, LOOM, CUI 
//linking method could be filtered by using mapping_id and other mapping metadata 
//(e.g. WHERE mappings.method = "LOOM" AND mappings.mapping_id = linksWithIDs.mapping_id_fk)
		 

-- vertices 
SELECT DISTINCT l.id 
FROM 
(SELECT DISTINCT srcID AS id FROM `linksWithIDs` 
UNION 
SELECT DISTINCT trgID AS id FROM `linksWithIDs` ) l

7) run org.apache.flink.examples.java.graph.ConnectedComponents.java 
with program arguments: 

vertices path
edges path
result path
max number of iterations 

8) import flink CC results into db

run ImportFlinkResults.java

9) With ComputeCliques.java you can compute the set of cliques for each (or some) CC

ggf. in VM arguments increase memory -Xmx 

10) visualization with neo4j
To create Neo 4J Statement: 
run GenerateNeo4JQuery.java

neo4J queries
-- GET ALL NODES AND EDGES = wie "SELECT * ..." wenn nur CC of interest in Neo4J-DB enthalten ist
MATCH (p:uri) RETURN p; 

-- DELETE ALL NODES AND EDGES 
MATCH (n)
OPTIONAL MATCH (n)-[r]-()
DELETE n,r

-- CREATE NODES AND EDGES 
= output of GenerateNeo4JQuery.java

