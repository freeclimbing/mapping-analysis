package org.mappinganalysis.model.functions.preprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Default (geographic) preprocessing: remove duplicate links, add cc ids,
 * type mismatch correction, link similarity
 *
 * At the end of preprocessing, neither ccId nor hashCcId is correct!! Do not group on these keys.
 */
public class DefaultPreprocessing
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(DefaultPreprocessing.class);

  private IncrementalConfig config;

  /**
   * Default constructor, link filter enabled by default.
   */
  public DefaultPreprocessing(IncrementalConfig config) {
    this.config = config;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    /*
    link filter enabled by default
     */
    LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(config.getExecutionEnvironment())
        .setRemoveIsolatedVertices(false)
        .setDataSources(config.getSourcesList())
        .setStrategy(LinkFilterStrategy.BASIC)
        .build();

    return graph
//        .mapVertices(new InternalTypeMapFunction())
        .run(new IntraSourceLinkRemover(config)) // optional
//        .mapVertices(new AddSettlementTypeMapFunction(config.getDataDomain())) // TODO FIX add type settlement for entities without type
//        .run(new TypeMisMatchCorrection(env)) // old, commented for SettlementBenchmark
        .run(new BasicEdgeSimilarityComputation(config))
        .run(linkFilter)
        .run(new TypeOverlapCcCreator(config)); // hash cc id, each vertex has "no type" with music/nc dataset
  }

  /**
   * only for incremental geography. if type is missing, correct cc's are not
   * found. therefore, "settlement" is added, if needed.
   */
  private static class AddSettlementTypeMapFunction implements MapFunction<Vertex<Long, ObjectMap>, ObjectMap> {
    private DataDomain dataDomain;

    public AddSettlementTypeMapFunction(DataDomain config) {
      this.dataDomain = config;
    }

    @Override
    public ObjectMap map(Vertex<Long, ObjectMap> vertex) throws Exception {
      if (dataDomain == DataDomain.GEOGRAPHY) {
        if (vertex.getValue().getTypesIntern().contains(Constants.NO_TYPE)) {
          vertex.getValue().setTypes(Constants.TYPE_INTERN, Sets.newHashSet(Constants.S));
        }
      }
      return vertex.getValue();
    }
  }
}
