package org.mappinganalysis.model.functions.clusterstrategies;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.AllEdgesCreateGroupReducer;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.incremental.BlockingKeySelector;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;


/**
 * Incremental clustering with multiple sources.
 */
public class MultiIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private static final Logger LOG = Logger.getLogger(MultiIncrementalClusteringFunction.class);
  private DataSet<Vertex<Long, ObjectMap>> toBeMergedElements = null;
  private IncrementalConfig config;

  MultiIncrementalClusteringFunction(
      DataSet<Vertex<Long, ObjectMap>> toBeMergedElements,
      IncrementalConfig config) {
    super();
    this.config = config;
    this.toBeMergedElements = toBeMergedElements
        .runOperation(new RepresentativeCreator(config));
  }

  MultiIncrementalClusteringFunction(IncrementalConfig config) {
    super();
    this.config = config;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(
      Graph<Long, ObjectMap, NullValue> input) throws Exception {
    DataSet<Vertex<Long, ObjectMap>> vertices = input
        .getVertices()
        .map(new BlockingKeyMapFunction(config.getConfigNoEnv()));

    /*
    INITIAL CLUSTERING
     */
    if (config.getStep() == ClusteringStep.INITIAL_CLUSTERING) {
      LOG.info(ClusteringStep.INITIAL_CLUSTERING.toString());

      DataSet<Edge<Long, NullValue>> edges = vertices
          .groupBy(new BlockingKeySelector())
          .reduceGroup(new AllEdgesCreateGroupReducer<>());

      Graph<Long, ObjectMap, ObjectMap> preprocGraph = Graph
          .fromDataSet(vertices,
              edges,
              config.getExecutionEnvironment())
          .run(new DefaultPreprocessing(config));

    /*
      representative creation based on hash cc ids, most likely only for initial clustering
     */
      return preprocGraph
          .run(new SimSort(config))
          .getVertices()
          .runOperation(new RepresentativeCreatorMultiMerge(config.getDataDomain()));
    } else
      /*
      VERTEX ADDITION
       */
      if (config.getStep() == ClusteringStep.VERTEX_ADDITION) {
        LOG.info(ClusteringStep.VERTEX_ADDITION.toString());

        /*
          add type settlement for entities without type
         */
        if (config.getDataDomain() == DataDomain.GEOGRAPHY) {
          toBeMergedElements = toBeMergedElements
              .map(vertex -> {
                if (vertex.getValue().getTypesIntern().contains(Constants.NO_TYPE)) {
                  vertex.getValue()
                      .setTypes(Constants.TYPE_INTERN, Sets.newHashSet(Constants.S));
                }
                return vertex;
              })
              .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});
        }

        DataSet<Vertex<Long, ObjectMap>> clusterWorkset = input
            .getVertices()
            .union(toBeMergedElements);

        //TODO  get source count total

        DataSet<Edge<Long, NullValue>> edges = clusterWorkset
            .groupBy(new BlockingKeySelector())
            .reduceGroup(new AllEdgesCreateGroupReducer<>());

        Graph<Long, ObjectMap, NullValue> preprocGraph = Graph
            .fromDataSet(clusterWorkset,
                edges,
                config.getExecutionEnvironment());

        Graph<Long, ObjectMap, ObjectMap> preprocStep = preprocGraph
            .run(new DefaultPreprocessing(config));

        return preprocStep
            .run(new SimSort(config))
            .getVertices()
            .runOperation(new RepresentativeCreatorMultiMerge(config.getDataDomain()));
      } else {
        return null;
      }
  }
}
