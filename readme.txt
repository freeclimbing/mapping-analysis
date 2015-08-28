Erklaerung zum "Workflow compute Connected Components with flink"
+ Download+Import Daten Bioportal
+  ggf. Cliquenberechnung


0) create a database and change dbURL, user, pw in ImportMappingsToDB and ImportOntologiesToDB
1) run DownloadBioportalMappings (includes: run the given create table statements to save the links) 
2) run DownloadBioportalOntologies (includes: run the given create table statements to save ontology metadata)
3) run ImportMappingsToDB - see docu in class
4) run ImportOntoliesToDB - see docu in class

OR SKIP 1) - 4) and import data somehow different

5) Bioportal gives us URIs, but we need integer to run CC algorithm in flink 

  CREATE TABLE `concept` (
	 `url` varchar(150) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
	 `id` int(50) NOT NULL AUTO_INCREMENT,
	  PRIMARY KEY (`url`),
	  KEY `id` (`id`)
  );

  INSERT INTO concept(url) 
  SELECT DISTINCT url FROM 
    (SELECT DISTINCT srcURL AS url FROM links 
    UNION 
      SELECT DISTINCT trgURL AS url FROM links 
    UNION 
      SELECT DISTINCT url AS url FROM `concept_attributes` 
    ) s;
    
    
6) get integer IDs for edges and vertices and save to two files

-- edges
SELECT CONCAT(src.id, " ", trg.id) 
FROM links l, concept src, concept trg
WHERE l.`srcURL` = src.url 
AND l.`trgURL` = trg.url; 

-- vertices 
SELECT DISTINCT c.id 
FROM 
(SELECT DISTINCT srcURL AS url FROM links 
UNION 
SELECT DISTINCT trgURL AS url FROM links) l, concept c 
WHERE l.url = c.url;

7) run org.apache.flink.examples.java.graph.ConnectedComponents.java 
with program arguments: 

vertices path
edges path
result path
max number of iterations 

8) import flink CC results into db

CREATE TABLE `connectedComponents` (
  `conceptID` int(11) DEFAULT NULL,
  `ccID` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- import result files from flink 1, 2, 3, 4, ...
LOAD DATA LOCAL INFILE 
"D:/Daten/workspace/BioportalMappingAnalysis/data/flink_data/CCresult/1"
INTO TABLE `connectedComponents` 
FIELDS TERMINATED BY " ";
LOAD DATA LOCAL INFILE 
"D:/Daten/workspace/BioportalMappingAnalysis/data/flink_data/CCresult/2"
INTO TABLE `connectedComponents` 
FIELDS TERMINATED BY " ";
LOAD DATA LOCAL INFILE 
"D:/Daten/workspace/BioportalMappingAnalysis/data/flink_data/CCresult/3"
INTO TABLE `connectedComponents` 
FIELDS TERMINATED BY " ";
LOAD DATA LOCAL INFILE 
"D:/Daten/workspace/BioportalMappingAnalysis/data/flink_data/CCresult/4"
INTO TABLE `connectedComponents` 
FIELDS TERMINATED BY " ";

9) Furthermore: 
With ComputeCliques.java you can compute the set of cliques for each CC


ggf. in VM arguments increase memory -Xmx