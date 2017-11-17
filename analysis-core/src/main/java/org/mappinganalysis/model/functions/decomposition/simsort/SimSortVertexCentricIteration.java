package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

/**
 * Execute SimSort procedure based on vertex-centric-iteration
 */
public class SimSortVertexCentricIteration
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(SimSort.class);

  private final ExecutionEnvironment env;
  private final Double minSimilarity;

  public SimSortVertexCentricIteration(Double minSimilarity, ExecutionEnvironment env) {
    this.minSimilarity = minSimilarity;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {
     // set solution set unmanaged in order to reduce out of memory exception on non-cluster setup
//    aggParameters.setSolutionSetUnmanagedMemory(true);

    DataSet<Vertex<Long, SimSortVertexTuple>> workingVertices = graph
        .getUndirected()
        .run(new SimSortInputGraphCreator(env))
        .runVertexCentricIteration(new SimSortComputeFunction(minSimilarity),
            new SimSortMessageCombiner(),
            Integer.MAX_VALUE)
        .getVertices();

    DataSet<Vertex<Long, ObjectMap>> resultingVertices = graph
        .getVertices()
        .join(workingVertices)
        .where(0)
        .equalTo(0)
        .with((vertex, workingVertex) -> {
//          LOG.info("v: " + vertex.toString() + " wv: " + workingVertex.toString());
          vertex.getValue().setHashCcId(workingVertex.getValue().getHash());
          if (workingVertex.getValue().getOldHash() != Long.MIN_VALUE) {
            vertex.getValue().setOldHashCcId(workingVertex.getValue().getOldHash());
          }
          vertex.getValue().setVertexStatus(workingVertex.getValue().isActive());

          return vertex;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    return Graph.fromDataSet(resultingVertices, graph.getEdges(), env);
  }

}
