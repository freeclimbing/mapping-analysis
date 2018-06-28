package org.mappinganalysis.model.functions.decomposition;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.LinkFilter;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * Methods needed for (initial) clustering.
 */
@Deprecated
public class Clustering {
  private static final Logger LOG = Logger.getLogger(Clustering.class);


  /**
   * Connected components and minor refinement steps.
   */
  @Deprecated
  public static Graph<Long, ObjectMap, ObjectMap> createInitialClustering(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) throws Exception {

    LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setRemoveIsolatedVertices(true)
        .setStrategy(LinkFilterStrategy.CLUSTERING)
        .build();

    return Clustering
        .computeTransitiveClosureEdgeSimilarities(graph, env)
        .run(linkFilter);
  }

  /**
   *
   * REPLACE
   *
   * Create edges from transitive closure and compute edge similarity
   *
   * 1. Compute transitive closure for a given graph and add the computed edges to the graph.
   * Direction of edges may change, two vertices have exactly one edge.
   * 2. For each of the computed edges, we compute a similarity value based on
   * label and geo coordinates.
   */
  @Deprecated
  public static Graph<Long, ObjectMap, ObjectMap> computeTransitiveClosureEdgeSimilarities(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) throws Exception {

    graph = graph.run(new ConnectedComponentIdAdder<>(env));

    // CcIdKeySelector!
    final DataSet<Edge<Long, NullValue>> distinctEdges = graph
        .getVertices()
        .runOperation(new EdgeComputationOnVerticesForKeySelector(new CcIdKeySelector()));

    return Graph.fromDataSet(graph.getVertices(), distinctEdges, env)
        .run(new BasicEdgeSimilarityComputation(Constants.COSINE_TRIGRAM, Constants.GEO, env));
  }
}
