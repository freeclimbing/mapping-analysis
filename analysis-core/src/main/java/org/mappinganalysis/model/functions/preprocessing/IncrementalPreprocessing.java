package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;

public class IncrementalPreprocessing
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, NullValue>> {
  private static final Logger LOG = Logger.getLogger(DefaultPreprocessing.class);

  private final ExecutionEnvironment env;

  /**
   * Preprocessing with optional link filter.
   */
  public IncrementalPreprocessing(ExecutionEnvironment env) {
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, NullValue> run(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    return graph
        .mapVertices(new InternalTypeMapFunction())
        .mapVertices(new DataSourceMapFunction()) // compatibility only
        .run(new TypeMisMatchCorrection(env));
  }

  /**
   * Temporary map function for compatibility with old 'ontology' values.
   */
  private static class DataSourceMapFunction
      implements MapFunction<Vertex<Long,ObjectMap>, ObjectMap> {
    @Override
    public ObjectMap map(Vertex<Long, ObjectMap> vertex) throws Exception {
      vertex.getValue().getDataSource();

      return vertex.getValue();
    }
  }
}
