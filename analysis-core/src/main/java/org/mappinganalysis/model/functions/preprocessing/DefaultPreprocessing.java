package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.Constants;

/**
 * Default preprocessing: remove duplicate links, add cc ids,
 * type mismatch correction, link similarity
 */
public class DefaultPreprocessing
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, ObjectMap>> {
  private final ExecutionEnvironment env;
  private final boolean isBasicLinkFilterEnabled;

  /**
   * Basic preprocessing
   */
  public DefaultPreprocessing(ExecutionEnvironment env) {
    this.isBasicLinkFilterEnabled = false;
    this.env = env;
  }

  /**
   * Preprocessing with additional basic link filter.
   */
  public DefaultPreprocessing(boolean isBasicLinkFilterEnabled, ExecutionEnvironment env) {
    this.isBasicLinkFilterEnabled = isBasicLinkFilterEnabled;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    //restrict graph to direct links with matching type information
    TypeMisMatchCorrection typeMisMatchCorrection = new TypeMisMatchCorrection
        .TypeMisMatchCorrectionBuilder()
        .setEnvironment(env)
        .build();

    Graph<Long, ObjectMap, ObjectMap> result = graph
        .mapVertices(new InternalTypeMapFunction())
        .run(new EqualDataSourceLinkRemover(env))
        .run(typeMisMatchCorrection)
        .run(new BasicEdgeSimilarityComputation(Constants.DEFAULT_VALUE, env));

    if (isBasicLinkFilterEnabled) {
      LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setRemoveIsolatedVertices(true)
        .setStrategy(LinkFilterStrategy.BASIC)
        .build();

      return result.run(linkFilter);
    } else {
      return result;
    }
  }
}
