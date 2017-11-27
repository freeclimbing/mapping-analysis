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
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.util.Constants;

public class IncrementalPreprocessing
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(DefaultPreprocessing.class);

  private final ExecutionEnvironment env;

  /**
   * Preprocessing with optional link filter.
   */
  public IncrementalPreprocessing(ExecutionEnvironment env) {
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    return graph
        .mapVertices(new InternalTypeMapFunction())
        .mapVertices(new DataSourceMapFunction()) // compatibility only
        .run(new TypeMisMatchCorrection(env))
        // TODO remove sim comp
        .run(new BasicEdgeSimilarityComputation(Constants.DEFAULT_VALUE, env));
  }

  /**
   * Temporary map function for compatibility with old 'ontology' values.
   */
  private static class DataSourceMapFunction implements MapFunction<Vertex<Long,ObjectMap>, ObjectMap> {
    @Override
    public ObjectMap map(Vertex<Long, ObjectMap> vertex) throws Exception {
      vertex.getValue().getDataSource();

      return vertex.getValue();
    }
  }
}
