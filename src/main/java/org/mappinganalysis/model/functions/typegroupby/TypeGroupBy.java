package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class TypeGroupBy {

  /**
   * For a given graph, assign all vertices with no type to the component where the best similarity can be found.
   * @param graph input graph
   * @param processingMode if default, typeGroupBy is executed
   * @param maxIterations maximal count vertex centric iterations  @return graph where non-type vertices are assigned to best matching component
   */
  public Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, ObjectMap> graph,
                                                   String processingMode, Integer maxIterations) throws Exception {
    if (processingMode.equals(Utils.DEFAULT_VALUE)) {
      VertexCentricConfiguration tbcParams = new VertexCentricConfiguration();
      tbcParams.setName("Type-based Cluster Generation Iteration");
      tbcParams.setDirection(EdgeDirection.ALL);
      tbcParams.setSolutionSetUnmanagedMemory(true);

      return graph.runVertexCentricIteration(
          new TypeGroupByVertexUpdateFunction(),
          new TypeGroupByMessagingFunction(), maxIterations, tbcParams);
    } else {
      return graph;
    }
  }

  public String getName() {
    return TypeGroupBy.class.getName();
  }
}
