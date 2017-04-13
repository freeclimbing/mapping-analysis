package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;

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
  private final String matchCombination;

  /**
   * Compute similarities based on the existing vertex properties,
   * save aggregated similarity as edge property
   * @param matchCombination relevant: Utils.SIM_GEO_LABEL_STRATEGY or Utils.DEFAULT_VALUE
   * @param env env
   */
  public BasicEdgeSimilarityComputation(String matchCombination, ExecutionEnvironment env) {
    this.env = env;
    this.matchCombination = matchCombination;
    if (matchCombination.equals(Constants.MUSIC)) {
      System.out.println("using MusicSimilarityFunction");
      this.simFunction = new MusicSimilarityFunction();
    } else {
      this.simFunction = new EdgeSimilarityFunction(
          matchCombination,
          Constants.MAXIMAL_GEO_DISTANCE);
    }
  }

  /**
   * Run basic edge similarity computation.
   * @param graph input graph
   * @return graph with edge similarities
   * @throws Exception
   */
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, NullValue> graph)
      throws Exception {
//    EdgeSimilarityFunction simFunction = new EdgeSimilarityFunction(
//        matchCombination,
//        Constants.MAXIMAL_GEO_DISTANCE); // todo agg mode?

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

    if (!matchCombination.equals(Constants.MUSIC)) {
      edges.map(new AggSimValueEdgeMapFunction(true)); // old mean function
    }

    return Graph.fromDataSet(graph.getVertices(), edges, env);
  }
}
