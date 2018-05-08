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
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Default (geographic) preprocessing: remove duplicate links, add cc ids,
 * type mismatch correction, link similarity
 */
public class DefaultPreprocessing
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(DefaultPreprocessing.class);

  private final ExecutionEnvironment env;
  private final boolean linkFilterEnabled;
  private IncrementalConfig config = null;
  private String metric;
  private final DataDomain domain;

  /**
   * Basic preprocessing: link filter enabled
   * At the end of preprocessing, neither ccId nor hashCcId is correct!! Do not group on these keys.
   */
  @Deprecated
  public DefaultPreprocessing(ExecutionEnvironment env) {
    this(true, env);
  }

  /**
   * Preprocessing with optional link filter.
   */
  @Deprecated
  public DefaultPreprocessing(boolean isBasicLinkFilterEnabled, ExecutionEnvironment env) {
    this.linkFilterEnabled = isBasicLinkFilterEnabled;
    this.env = env;
    this.domain = DataDomain.GEOGRAPHY;
  }

  /**
   * Music constructor, link filter enabled by default.
   */
  public DefaultPreprocessing(DataDomain domain, ExecutionEnvironment env) {
    this.domain = domain;
    this.env = env;
    this.linkFilterEnabled = true;
    this.metric = Constants.COSINE_TRIGRAM;
  }

  public DefaultPreprocessing(String metric, DataDomain domain, ExecutionEnvironment env) {
    this.metric = metric;
    this.domain = domain;
    this.env = env;
    this.linkFilterEnabled = true;
  }

  public DefaultPreprocessing(IncrementalConfig config) {
    this.config = config;
    this.metric = config.getMetric();
    this.domain = config.getDataDomain();
    this.env = config.getExecutionEnvironment();
    this.linkFilterEnabled = true;
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
//        .mapVertices(new InternalTypeMapFunction() // TODO fix only needed for big geography
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
    private IncrementalConfig config;

//    public AddSettlementTypeMapFunction(DataDomain config) {
//      this.config = config;
//    }

    @Override
    public ObjectMap map(Vertex<Long, ObjectMap> vertex) throws Exception {
      if (config != null && config.getDataDomain() == DataDomain.GEOGRAPHY) {
        if (vertex.getValue().getTypesIntern().contains(Constants.NO_TYPE)) {
          vertex.getValue().setTypes(Constants.TYPE_INTERN, Sets.newHashSet(Constants.S));
        }
      }
      return vertex.getValue();
    }
  }
}
