package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.functions.keyselector.HashCcIdKeySelector;

public class SimSort
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(SimSort.class);
  private Config config;

  /**
   * Constructor - provide a DataDomain (geo or music) and min similarity
   */
  public SimSort(DataDomain domain,
                 String metric,
                 Double minSimilarity,
                 ExecutionEnvironment env) {
    this.config = new Config(domain, env);
    this.config.setMetric(metric);
    this.config.setSimSortSimilarity(minSimilarity);
  }

  public SimSort(Config config) {
    this.config = config;
  }

  /**
   * Execute SimSort procedure based on vertex-centric-iteration, as preparation:
   * create all missing edges, addGraph default vertex sim values
   */
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {

    DataSet<Edge<Long, NullValue>> distinctEdges = graph
        .getVertices()
//          .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector())); // TODO perhaps add hash cc id before simsort??
        .runOperation(new EdgeComputationOnVerticesForKeySelector(new HashCcIdKeySelector()));

    graph = Graph
        .fromDataSet(graph.getVertices(),
            distinctEdges,
            config.getExecutionEnvironment())
        .run(new BasicEdgeSimilarityComputation(config.getMetric(),
            config.getMode(),
            config.getExecutionEnvironment()))
        .run(new SimSortVertexCentricIteration(
            config.getSimSortSimilarity(),
            config.getExecutionEnvironment()));

    return Graph.fromDataSet(graph.getVertices(),
        graph.getEdges(),
        config.getExecutionEnvironment());
  }
}
