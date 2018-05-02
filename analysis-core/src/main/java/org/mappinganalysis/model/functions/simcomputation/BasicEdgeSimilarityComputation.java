package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.Config;

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

    DataSet<Edge<Long, ObjectMap>> edges = graph.getTriplets()
        .runOperation(similarityComputation)
        .map(new TripletToEdgeMapFunction());

    if (mode.equals(Constants.GEO)) {
      edges = edges.map(new AggSimValueEdgeMapFunction(true)); // old mean function
    } else if (mode.equals(Constants.MUSIC)){
      edges = edges.map(new AggSimValueEdgeMapFunction(Constants.MUSIC));
    } else if (mode.equals(Constants.NC)){
      edges = edges.map(new AggSimValueEdgeMapFunction(Constants.NC));
    }

    return Graph.fromDataSet(graph.getVertices(), edges, env);
  }

}
