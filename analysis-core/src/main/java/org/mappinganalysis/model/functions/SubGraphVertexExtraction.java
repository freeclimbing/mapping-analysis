package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Extract vertices from graph based on a list of vertex ids read from file.
 */
public class SubGraphVertexExtraction
    implements GraphAlgorithm<Long, ObjectMap, NullValue, DataSet<Vertex<Long, ObjectMap>>> {
  private IncrementalConfig config;

  /**
   * Default constructor
   * @param config config to retrieve id list from
   */
  public SubGraphVertexExtraction(IncrementalConfig config) {
    this.config = config;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(
      Graph<Long, ObjectMap, NullValue> input)
      throws Exception {
    DataSource<Long> initialLongs = config
      .getExecutionEnvironment()
      .readFileOfPrimitives(
          config.getSubGraphVerticesPath(),
          Long.class);

    return input
        .getVertices()
        .join(initialLongs)
        .where(0).equalTo((KeySelector<Long, Long>) value -> value)
        .with((vertex, id) -> vertex)
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});
  }
}
