package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.AllEdgesCreateGroupReducer;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.incremental.BlockingKeySelector;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.config.IncrementalConfig;


/**
 * Incremental clustering with multiple sources.
 */
public class MultiIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private static final Logger LOG = Logger.getLogger(MultiIncrementalClusteringFunction.class);
  private String source;
  private DataSet<Vertex<Long, ObjectMap>> toBeMergedElements = null;
  private IncrementalConfig config;

  MultiIncrementalClusteringFunction(
      DataSet<Vertex<Long, ObjectMap>> toBeMergedElements,
      IncrementalConfig config) {
    super();
    this.config = config;
    this.source = config.getNewSource();
    this.toBeMergedElements = toBeMergedElements
        .runOperation(new RepresentativeCreator(config));
  }

  MultiIncrementalClusteringFunction(IncrementalConfig config) {
    super();
    this.config = config;
    this.source = config.getNewSource();
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(
      Graph<Long, ObjectMap, NullValue> input) throws Exception {

    DataSet<Vertex<Long, ObjectMap>> vertices = input
        .getVertices()
        .map(new BlockingKeyMapFunction(config.getConfigNoEnv()));

    DataSet<Edge<Long, NullValue>> edges = vertices
        .groupBy(new BlockingKeySelector())
        .reduceGroup(new AllEdgesCreateGroupReducer<>());

    Graph<Long, ObjectMap, ObjectMap> preprocGraph = Graph
        .fromDataSet(vertices,
            edges,
            config.getExecutionEnvironment())
        .run(new DefaultPreprocessing(config));

    Graph<Long, ObjectMap, ObjectMap> run = preprocGraph
        .run(new SimSort(config));

    /*
      representative creation based on hash cc ids, most likely only for initial clustering
     */
    return run
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(config.getDataDomain()));

//    input.getVertices()
//        .map(new AddShadingTypeMapFunction())
//        .map(new MergeGeoTupleCreator(config.getBlockingStrategy()))
//        .groupBy(7)
//        .reduceGroup(new MergeGeoTripletCreator(
//            config.getExistingSourcesCount(),
//            config.getNewSource(),
//            true))
//        .distinct(0, 1)
//        .runOperation(similarityComputation)
//        .groupBy(5)
//        .reduceGroup(new HungarianAlgorithmReduceFunction());
//
//
//
//    DataSet<Vertex<Long, ObjectMap>> baseClusters = input.getVertices()
//        .runOperation(new RepresentativeCreator(config));
//
//    return baseClusters.union(toBeMergedElements)
//        .runOperation(new CandidateCreator(config, source, sourcesCount))
//        .flatMap(new DualMergeGeographyMapper(false))
//        .leftOuterJoin(baseClusters)
//        .where(0).equalTo(0)
//        .with(new FinalMergeGeoVertexCreator())


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



//        .runOperation(new RepresentativeCreator(config));
  }

}
