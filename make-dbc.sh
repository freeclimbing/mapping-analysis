#!/bin/bash

# clean before?
mvn clean
mvn -pl !'property-enrichment,!analysis-clique' package -DskipTests &&
#mvn package -DskipTests &&
scp analysis-examples/target/Mapping-Analysis.jar markus@dbc0.informatik.intern.uni-leipzig.de:jars
