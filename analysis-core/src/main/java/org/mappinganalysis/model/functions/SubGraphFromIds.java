package org.mappinganalysis.model.functions;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Create sub graph based on existing graph based on a list of
 * vertex ids read from file.
 */
public class SubGraphFromIds
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, NullValue>> {
  private IncrementalConfig config;

  /**
   * Default constructor
   * @param config config to retrieve id list from
   */
  public SubGraphFromIds(IncrementalConfig config) {
    this.config = config;
  }

  @Override
  public Graph<Long, ObjectMap, NullValue> run(
      Graph<Long, ObjectMap, NullValue> input)
      throws Exception {
    return  Graph.fromDataSet(
        input.run(new SubGraphVertexExtraction(config)),
        input.getEdges(),
        config.getExecutionEnvironment());
  }

}
