package org.mappinganalysis.model.functions.decomposition;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.LinkFilter;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * Methods needed for (initial) clustering.
 */
public class Clustering {
  private static final Logger LOG = Logger.getLogger(Clustering.class);


  /**
   * Connected components and minor refinement steps.
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, ObjectMap> createInitialClustering(
      Graph<Long, ObjectMap, ObjectMap> graph,
      String verbosity,
      ExampleOutput out,
      ExecutionEnvironment env) throws Exception {

    graph = Clustering.computeTransitiveClosureEdgeSimilarities(graph, env);

    LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setStrategy(LinkFilterStrategy.CLUSTERING)
        .build();

    graph.run(linkFilter);

    if (verbosity.equals(Constants.DEBUG)) {
      out.addPreClusterSizes("2 intial cluster sizes", graph.getVertices(), Constants.CC_ID);
    }

    return graph;
  }

  /**
   * Create edges from transitive closure and compute edge similarity
   *
   * 1. Compute transitive closure for a given graph and add the computed edges to the graph.
   * Direction of edges may change, two vertices have exactly one edge.
   * 2. For each of the computed edges, we compute a similarity value based on
   * label and geo coorodinates.
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, ObjectMap> computeTransitiveClosureEdgeSimilarities(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) throws Exception {

    graph = GraphUtils.addCcIdsToGraph(graph, env);

    final DataSet<Edge<Long, NullValue>> distinctEdges = GraphUtils
        .getTransitiveClosureEdges(graph.getVertices(), new CcIdKeySelector());
    final DataSet<Edge<Long, ObjectMap>> simEdges = SimilarityComputation
        .computeGraphEdgeSim(Graph.fromDataSet(graph.getVertices(), distinctEdges, env),
            Constants.SIM_GEO_LABEL_STRATEGY);
    graph = Graph.fromDataSet(graph.getVertices(), simEdges, env);
    return graph;
  }
}
