package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.typegroupby.HashCcIdOverlappingFunction;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * Default preprocessing: remove duplicate links, add cc ids,
 * type mismatch correction, link similarity
 */
public class DefaultPreprocessing
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(DefaultPreprocessing.class);

  private final ExecutionEnvironment env;
  private final boolean linkFilterEnabled;

  /**
   * Basic preprocessing
   */
  public DefaultPreprocessing(ExecutionEnvironment env) {
    this.linkFilterEnabled = true;
    this.env = env;
  }

  /**
   * Preprocessing with optional link filter.
   */
  public DefaultPreprocessing(boolean isBasicLinkFilterEnabled, ExecutionEnvironment env) {
    this.linkFilterEnabled = isBasicLinkFilterEnabled;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    Graph<Long, ObjectMap, ObjectMap> result = graph
        .mapVertices(new InternalTypeMapFunction())
        .run(new EqualDataSourceLinkRemover(env))
        .run(new TypeMisMatchCorrection(env))
        .run(new BasicEdgeSimilarityComputation(Constants.DEFAULT_VALUE, env));

    if (linkFilterEnabled) {
      LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setRemoveIsolatedVertices(true)
        .setStrategy(LinkFilterStrategy.BASIC)
        .build();

      return result.run(linkFilter)
          .run(new TypeOverlapCcCreator(env));
    } else {
      return result;
    }
  }
}
