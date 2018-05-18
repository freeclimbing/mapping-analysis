package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.SourceExtraction;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.decomposition.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Compute similarities based on the existing vertex properties,
 * save aggregated similarity as edge property
 */
public class BasicEdgeSimilarityComputation
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(BasicEdgeSimilarityComputation.class);

  private final ExecutionEnvironment env;
  private final SimilarityFunction<Triplet<Long, ObjectMap, NullValue>,
      Triplet<Long, ObjectMap, ObjectMap>> simFunction;
  private final String mode;
  private IncrementalConfig config = null;

  /**
   * Compute similarities based on the existing vertex properties,
   * save aggregated similarity as edge property
   * @param metric metric for similarity computation
   * @param mode relevant: Constants.MUSIC, Constants.NC, Constants.GEO
   * @param env env
   */
  public BasicEdgeSimilarityComputation(String metric, String mode, ExecutionEnvironment env) {
    this.env = env;
    this.mode = mode;
    switch (mode) {
      case Constants.MUSIC:
        this.simFunction = new MusicSimilarityFunction(metric);
        break;
      case Constants.NC:
        this.simFunction = new NcSimilarityFunction(metric);
        break;
      case Constants.GEO:
        this.simFunction = new EdgeSimilarityFunction(
            metric,
            mode,
            Constants.MAXIMAL_GEO_DISTANCE);
        break;
      default:
        this.simFunction = null;
        break;
    }
  }

  public BasicEdgeSimilarityComputation(IncrementalConfig config) {
    this.config = config;
    this.env = config.getExecutionEnvironment();
    this.mode = config.getMode();

    if (config.getDataDomain() == DataDomain.MUSIC) {
      this.simFunction = new MusicSimilarityFunction(config.getMetric());
    } else if (config.getDataDomain() == DataDomain.NC) {
      this.simFunction = new NcSimilarityFunction(config.getMetric());
    } else if (config.getDataDomain() == DataDomain.GEOGRAPHY) {
      this.simFunction = new EdgeSimilarityFunction(
          config.getMetric(),
          config.getMode(),
          Constants.MAXIMAL_GEO_DISTANCE);
    } else {
      this.simFunction = null;
    }
  }

  /**
   * Run basic edge similarity computation.
   * @param graph input graph
   * @return graph with edge similarities
   */
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, NullValue> graph)
      throws Exception {
    SimilarityComputation<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.EDGE_SIM)
        .build();

    boolean checkSourceOverlap = false;
    if (config != null
        && (config.getStep() == ClusteringStep.CLUSTER_ADDITION
        || config.getStep() == ClusteringStep.SOURCE_ADDITION)) {
      checkSourceOverlap = true;
    }

    DataSet<Edge<Long, ObjectMap>> edges = graph.getTriplets()
        .filter(new DataSourceOverlapCheckFilterFunction(checkSourceOverlap))
        .withBroadcastSet(graph.run(new SourceExtraction<>()), "dataSources")
        .runOperation(similarityComputation)
        .map(new TripletToEdgeMapFunction())
        .map(new AggSimValueEdgeMapFunction())
        .filter(edge -> edge.getValue().getEdgeSimilarity() > 0.6); // music 0.6 or 0.65 could be better for f1

    return Graph.fromDataSet(graph.getVertices(), edges, env);
  }

}
