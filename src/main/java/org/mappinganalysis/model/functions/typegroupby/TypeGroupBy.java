package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.mappinganalysis.model.ObjectMap;

public class TypeGroupBy {

  /**
   * For a given graph, assign all vertices with no type to the component where the best similarity can be found.
   * @param graph input graph
   * @param maxIterations maximal count vertex centric iterations
   * @return graph where non-type vertices are assigned to best matching component
   */
  public Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, ObjectMap> graph, Integer maxIterations) {
    VertexCentricConfiguration tbcParams = new VertexCentricConfiguration();
    tbcParams.setName("Type-based Cluster Generation Iteration");
    tbcParams.setDirection(EdgeDirection.ALL);

    return graph.runVertexCentricIteration(
        new TypeGroupByVertexUpdateFunction(),
        new TypeGroupByMessagingFunction(), maxIterations, tbcParams);
  }

  public String getName() {
    return TypeGroupBy.class.getName();
  }
}
