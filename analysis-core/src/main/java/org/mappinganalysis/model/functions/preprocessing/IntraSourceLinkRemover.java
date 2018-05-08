package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.EdgeIdsSourcesTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Remove links where source and target dataset name are equal, remove duplicate links
 */
public class IntraSourceLinkRemover
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, NullValue>> {
  private final ExecutionEnvironment env;
  private IncrementalConfig config = null;

  /**
   * Remove links where source and target dataset name are equal, remove duplicate links
   */
  @Deprecated
  public IntraSourceLinkRemover(ExecutionEnvironment env) {
    this.env = env;
  }

  /**
   * Remove links where source and target dataset name are equal, remove duplicate links
   *
   * input links are not (yet) given for incremental setting we don't create intra source links
   */
  public IntraSourceLinkRemover(IncrementalConfig config) {
    this.env = config.getExecutionEnvironment();
    this.config = config;
  }

  @Override
  public Graph<Long, ObjectMap, NullValue> run(Graph<Long, ObjectMap, NullValue> input) throws Exception {
    if (config == null || !config.isIncremental()) {
//      LOG.info("config not incremental");
      DataSet<Edge<Long, NullValue>> newEdges = getEdgeIdSourceValues(input.getEdgeIds(), input.getVertices())
          .filter(edge -> !edge.getSrcSource().equals(edge.getTrgSource()))
          .map(value -> new Edge<>(value.f0, value.f1, NullValue.getInstance()))
          .returns(new TypeHint<Edge<Long, NullValue>>() {
          })
          .distinct();

      return Graph.fromDataSet(input.getVertices(), newEdges, env);
    } else {
      return input;
    }
  }

  /**
   * Create a dataset of edge ids with the associated dataset source values
   * like "23L, 42L, http://dbpedia.org/, http://geonames.org/"
   */
  private static DataSet<EdgeIdsSourcesTuple> getEdgeIdSourceValues(
      DataSet<Tuple2<Long, Long>> edgeIds,
      DataSet<Vertex<Long, ObjectMap>> vertices) {
    return edgeIds
        .map(edge -> new EdgeIdsSourcesTuple(edge.f0, edge.f1, "", ""))
        .returns(new TypeHint<EdgeIdsSourcesTuple>() {})
        .join(vertices)
        .where(0)
        .equalTo(0)
        .with((tuple, vertex) -> {
          tuple.checkSideAndUpdate(0, vertex.getValue().getDataSource());
          return tuple;
        })
        .returns(new TypeHint<EdgeIdsSourcesTuple>() {})
        .join(vertices)
        .where(1)
        .equalTo(0)
        .with((tuple, vertex) -> {
          tuple.checkSideAndUpdate(1, vertex.getValue().getDataSource());
          return tuple;
        })
        .returns(new TypeHint<EdgeIdsSourcesTuple>() {});
  }
}
