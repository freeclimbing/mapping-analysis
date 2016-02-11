package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.mappinganalysis.model.ObjectMap;

public class TypeGroupBy {

//  public TypeGroupBy(Graph<Long, ObjectMap, ObjectMap> graph, Integer maxIterations) {
//    execute(graph, maxIterations);
//  }

  public Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, ObjectMap> graph, Integer maxIterations) {
    VertexCentricConfiguration tbcParams = new VertexCentricConfiguration();
    tbcParams.setName("Type-based Cluster Generation Iteration");
    tbcParams.setDirection(EdgeDirection.ALL);

    // assign non-type vertices to best matching
    return graph.runVertexCentricIteration(
        new TypeGroupByVertexUpdateFunction(),
        new TypeGroupByMessagingFunction(), maxIterations, tbcParams);
  }

  public String getName() {
    return TypeGroupBy.class.getName();
  }
}
