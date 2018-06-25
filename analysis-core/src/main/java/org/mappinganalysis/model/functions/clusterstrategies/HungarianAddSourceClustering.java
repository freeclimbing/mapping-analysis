package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.incremental.BlockingKeySelector;
import org.mappinganalysis.model.functions.incremental.HungarianAlgorithmReduceFunction;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.simcomputation.EdgeSimilarityFunction;
import org.mappinganalysis.model.functions.simcomputation.MusicSimilarityFunction;
import org.mappinganalysis.model.functions.simcomputation.NcSimilarityFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.keyselector.BlockingKeyFromAnyElementKeySelector;

class HungarianAddSourceClustering implements CustomUnaryOperation<Vertex<Long,ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(HungarianAddSourceClustering.class);

  private DataSet<Vertex<Long, ObjectMap>> input;
  private IncrementalConfig config;

  public HungarianAddSourceClustering(IncrementalConfig config) {
    this.config = config;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> dataSet) {
    input = dataSet;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {
    // replace TODO have simple function call for source addition
    final SimilarityFunction<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>> simFunction;
    if (config.getDataDomain() == DataDomain.MUSIC) {
      simFunction = new MusicSimilarityFunction(config.getMetric());
    } else if (config.getDataDomain() == DataDomain.NC) {
      simFunction = new NcSimilarityFunction(config.getMetric());
    } else if (config.getDataDomain() == DataDomain.GEOGRAPHY) {
      simFunction = new EdgeSimilarityFunction(
          config.getMetric(),
          config.getMode(),
          Constants.MAXIMAL_GEO_DISTANCE);
    } else {
      simFunction = null;
    }
    SimilarityComputation<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.EDGE_SIM)
        .build();

    return input
        .groupBy(new BlockingKeySelector())
        .reduceGroup(new HungarianTripletCreator(config.getNewSource()))
        .runOperation(similarityComputation)
        .map(new TripletMeanAggregationFunction())
        .groupBy(new BlockingKeyFromAnyElementKeySelector())
        .reduceGroup(new HungarianAlgorithmReduceFunction())
        .flatMap(new HungarianDualVertexMergeFlatMapFunction(
            config.getDataDomain())) // TODO manual threshold
        .runOperation(new RepresentativeCreator(config));
      // Todo simsort?
  }
}
