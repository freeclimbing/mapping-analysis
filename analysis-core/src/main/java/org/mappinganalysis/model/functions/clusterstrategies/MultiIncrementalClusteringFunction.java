package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.AllEdgesCreateGroupReducer;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CandidateCreator;
import org.mappinganalysis.model.functions.incremental.BlockingKeySelector;
import org.mappinganalysis.model.functions.incremental.HungarianAlgorithmReduceFunction;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.*;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;


/**
 * Incremental clustering with multiple sources.
 */
public class MultiIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private static final Logger LOG = Logger.getLogger(MultiIncrementalClusteringFunction.class);
  private String source;
  private int sourcesCount;
  private DataSet<Vertex<Long, ObjectMap>> toBeMergedElements;
  private IncrementalConfig config;

  MultiIncrementalClusteringFunction(
      DataSet<Vertex<Long, ObjectMap>> toBeMergedElements,
      IncrementalConfig config) {
    super();
    this.config = config;
    this.source = config.getNewSource();
    this.sourcesCount = config.getExistingSourcesCount();
    this.toBeMergedElements = toBeMergedElements
        .runOperation(new RepresentativeCreator(config));
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(
      Graph<Long, ObjectMap, NullValue> input) throws Exception {

    SimilarityComputation<MergeGeoTriplet,
        MergeGeoTriplet> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeGeoTriplet,
        MergeGeoTriplet>()
        .setSimilarityFunction(new MergeGeoSimilarity(config.getMetric())) // TODO check sim function
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.7)
        .build();

    // create representatives for following blocking key extraction
    DataSet<Vertex<Long, ObjectMap>> baseClusters = input.getVertices()
        .runOperation(new RepresentativeCreator(config));

    DataSet<Edge<Long, NullValue>> edges = baseClusters
        .groupBy(new BlockingKeySelector())
        .reduceGroup(new AllEdgesCreateGroupReducer<>());


     input.getVertices()
        .map(new AddShadingTypeMapFunction())
        .map(new MergeGeoTupleCreator(blockingStrategy))
        .groupBy(7)
        .reduceGroup(new MergeGeoTripletCreator(
            sourceCount, newSource, true))
        .distinct(0, 1)
        .runOperation(similarityComputation)
        .groupBy(5)
        .reduceGroup(new HungarianAlgorithmReduceFunction());



    DataSet<Vertex<Long, ObjectMap>> baseClusters = input.getVertices()
        .runOperation(new RepresentativeCreator(config));

    return baseClusters.union(toBeMergedElements)
        .runOperation(new CandidateCreator(config, source, sourcesCount))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0).equalTo(0)
        .with(new FinalMergeGeoVertexCreator()) // TODO really needed?
//        .map(x -> {
//          if (x.getValue().getVerticesList().contains(298L)
//              || x.getValue().getVerticesList().contains(299L)
//              || x.getValue().getVerticesList().contains(5013L)
//              || x.getValue().getVerticesList().contains(5447L)) {
//            LOG.info("FinalMergeGeoVertex: " + x.toString());
//          }
//
//          return x;
//        })
//        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .runOperation(new RepresentativeCreator(config));
  }
}
