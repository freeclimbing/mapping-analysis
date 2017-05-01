package org.mappinganalysis.corruption;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

/**
 * Corrupt (currently) settlement bechmark data reproducible.
 */
public class DataCorruption
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, NullValue>> {
  private static final Logger LOG = Logger.getLogger(DataCorruption.class);

  private ExecutionEnvironment env;

  public DataCorruption(ExecutionEnvironment env) {
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, NullValue> run(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {

    LOG.info("oldEdges: " + graph.getEdgeIds().count());

    DataSet<Edge<Long, NullValue>> newEdges = graph
        .getVertexIds()
        .mapPartition(new EdgeCreateCorruptionFunction(4));

    DataSet<Edge<Long, NullValue>> resultEdges = graph.getEdges().union(newEdges);

    LOG.info("newEdges: " + newEdges.count());
//    System.out.println("newEdges: " + newEdges.count());

    LOG.info("resultEdges: " + resultEdges.count());
//    System.out.println("resultEdges: " + resultEdges.count());
    return Graph.fromDataSet(
        graph.getVertices(),
        resultEdges,
//        graph.getEdges().mapPartition(new EdgeRemoveCorruptionFunction(6)),
        env
    );
  }

}
