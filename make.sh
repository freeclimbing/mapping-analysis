#!/bin/bash

mvn package -DskipTests &&
scp analysis-examples/target/Mapping-Analysis.jar markus@dbc0.informatik.intern.uni-leipzig.de:settlement-benchmark
