package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.blocksplit.BlockSplitTupleCreator;
import org.mappinganalysis.model.functions.incremental.HungarianAlgorithmReduceFunction;
import org.mappinganalysis.model.functions.incremental.MatchingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.MergeMusicTupleCreator;
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

    DataSet<Triplet<Long, ObjectMap, ObjectMap>> simTriplets = input
        // blocking TODO add proper handling for different blocking methods
        .map(new MergeMusicTupleCreator())
        .runOperation(new BlockSplitTupleCreator())
        .map(new MusicTripletToTripletFunction(config.getDataDomain()))
        // old begin
//        .groupBy(new BlockingKeySelector())
//        .reduceGroup(new HungarianTripletCreator(config.getNewSource())) // TODO CHECK THIS LINE (first round)
        // old end
        .runOperation(similarityComputation)
        .map(new TripletMeanAggregationFunction());

    if (config.getMatchStrategy() != null
        && config.getMatchStrategy() == MatchingStrategy.HUNGARIAN) {
      return simTriplets
          .groupBy(new BlockingKeyFromAnyElementKeySelector())
          .reduceGroup(new HungarianAlgorithmReduceFunction())
          .flatMap(new HungarianDualVertexMergeFlatMapFunction(
          config.getDataDomain())) // TODO manual threshold
          .runOperation(new RepresentativeCreator(config));
    } else if (config.getMatchStrategy() != null
        && config.getMatchStrategy() == MatchingStrategy.MAX_BOTH) {
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
          .with(new FlatJoinFunction<Vertex<Long, ObjectMap>, Tuple1<Long>, Vertex<Long, ObjectMap>>() {
        @Override
        public void join(Vertex<Long, ObjectMap> vertex, Tuple1<Long> tuple, Collector<Vertex<Long, ObjectMap>> out) throws Exception {
          if (tuple == null) {
            out.collect(vertex);
          }
        }
      });

      // we get duplicate entries for multiple runs
      //{"id":1279572,"data":{"number":"2","blockingLabel":" ele","artist":"Some Electric Noise","year":2010,"album":"Blackout","artistTitleAlbum":"some electric noise some electric the death of the radio son blackout","length":235,"language":"no_or_minor_lang","label":"Some Electric - The Death of the Radio Son","dataSources":["1","2","3","4","5"],"clusteredVertices":[1315540,1389969,1279572,1349788,1549336]}}
//      {"id":1279572,"data":{"number":"2","blockingLabel":" ele","artist":"Some Electric Noise","year":2010,"album":"Blackout","artistTitleAlbum":"some electric noise some electric the death of the radio son blackout","length":235,"language":"no_or_minor_lang","label":"Some Electric - The Death of the Radio Son","dataSources":["1","2","3","5"],"clusteredVertices":[1315540,1279572,1349788,1549336]}}
//      {"id":1411798,"data":{"number":"1","blockingLabel":" me ","artist":"Love Me Destroyer","year":2007,"album":"The Things Around Us Burn (2007)","artistTitleAlbum":"love me destroyer 001 choked and charmed the things around us burn 2007","length":142,"language":"english","label":"001-Choked and Charmed","dataSources":["4","5"],"clusteredVertices":[1411798,1598308]}}
//      {"id":1411798,"data":{"number":"1","blockingLabel":" me ","artist":"Love Me Destroyer","year":2007,"album":"The Things Around Us Burn (2007)","artistTitleAlbum":"love me destroyer 001 choked and charmed the things around us burn 2007","length":142,"language":"english","label":"001-Choked and Charmed","dataSources":["4","5"],"clusteredVertices":[1411798,1598308]}}

      return maxBothTriplets
          .flatMap(new HungarianDualVertexMergeFlatMapFunction(config.getDataDomain()))
          .union(notHandledVertices)
          .runOperation(new RepresentativeCreator(config));

    } else {
      throw new IllegalArgumentException("Unsupported match strategy: " + config.getMatchStrategy());
    }
  }

}
