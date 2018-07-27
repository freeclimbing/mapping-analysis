package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.blocksplit.BlockSplitTripletCreator;
import org.mappinganalysis.model.functions.incremental.HungarianAlgorithmReduceFunction;
import org.mappinganalysis.model.functions.incremental.MatchStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.MergeMusicTupleCreator;
import org.mappinganalysis.model.functions.simcomputation.MusicTripletSimilarityFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.functions.stats.StatisticsCountElementsRichMapFunction;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.filter.MinThresholdFilterFunction;
import org.mappinganalysis.util.functions.keyselector.BlockingKeyFromAnyElementKeySelector;

/**
 * Implementations of clustering for the use case of adding a new knowledge base to
 * an existing set of clusters.
 *
 * MAX_BOTH default
 * Currently, MAX_BOTH and HUNGARIAN can be used
 * TODO support different blocking strategies
 * TODO support different data domain
 */
class SourceAdditionClustering
    implements CustomUnaryOperation<Vertex<Long,ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(SourceAdditionClustering.class);

  private DataSet<Vertex<Long, ObjectMap>> input;
  private IncrementalConfig config;

  /**
   * Default constructor.
   */
  SourceAdditionClustering(IncrementalConfig config) {
    this.config = config;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> dataSet) {
    input = dataSet;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {
    SimilarityComputation<MergeMusicTriplet,
        MergeMusicTriplet> simCompMusic
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeMusicTriplet,
        MergeMusicTriplet>()
        .setSimilarityFunction(new MusicTripletSimilarityFunction(config.getMetric()))
        .setStrategy(SimilarityStrategy.MUSIC)
        .build();

    /*
      block split triplet creator
     */
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> simTriplets = input
        .map(new MergeMusicTupleCreator(config))
        .runOperation(new BlockSplitTripletCreator(config.getDataDomain(),
            config.getNewSource()))
        .rebalance()
        .runOperation(simCompMusic)
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.SIM_TRIPLET_ACCUMULATOR))
        .filter(new MinThresholdFilterFunction<>(config.getMinResultSimilarity()))
        .map(new StatisticsCountElementsRichMapFunction<>(
            Constants.THRESHOLD_TRIPLET_ACCUMULATOR))
        .map(new MusicTripletToTripletFunction(config.getDataDomain(),
            config.getNewSource()));

    /*
      hungarian
     */
    if (config.getMatchStrategy() != null
        && config.getMatchStrategy() == MatchStrategy.HUNGARIAN) {
      return simTriplets
          .groupBy(new BlockingKeyFromAnyElementKeySelector())
          .reduceGroup(new HungarianAlgorithmReduceFunction())
          .flatMap(new DualVertexMergeFlatMapper(
              config.getDataDomain(),
              config.getMinResultSimilarity()))
          .runOperation(new RepresentativeCreator(config));
    } else
      /*
      max both
       */
      if (config.getMatchStrategy() != null
        && config.getMatchStrategy() == MatchStrategy.MAX_BOTH) {
      DataSet<Triplet<Long, ObjectMap, ObjectMap>> maxBothTriplets = simTriplets
          .runOperation(new MaxBothSelection());

      DistinctOperator<Tuple1<Long>> handledVertexIds = maxBothTriplets
          .flatMap(new FlatMapFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple1<Long>>() {
        @Override
        public void flatMap(Triplet<Long, ObjectMap, ObjectMap> triplet, Collector<Tuple1<Long>> out) throws Exception {
          out.collect(Tuple1.of(triplet.getSrcVertex().getId()));
          out.collect(Tuple1.of(triplet.getTrgVertex().getId()));
        }
      }).distinct();

      // some vertices have no partner (from beginning), here we add them back for next step
      DataSet<Vertex<Long, ObjectMap>> notHandledVertices = input
          .leftOuterJoin(handledVertexIds)
          .where(0).equalTo(0)
          .with(new LeftMinusRightSideJoinFunction<>());

      return maxBothTriplets
          .flatMap(new DualVertexMergeFlatMapper(
              config.getDataDomain(),
              config.getMinResultSimilarity()))
          .map(new StatisticsCountElementsRichMapFunction<>(
              Constants.MAX_BOTH_CREATE_ACCUMULATOR))
          .union(notHandledVertices)
          .runOperation(new RepresentativeCreator(config));

    } else {
      throw new IllegalArgumentException("Unsupported match strategy: " + config.getMatchStrategy());
    }
  }

}
