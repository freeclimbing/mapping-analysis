package org.mappinganalysis.model;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.functions.preprocessing.EqualDataSourceLinkRemover;
import org.mappinganalysis.model.functions.preprocessing.TypeMisMatchCorrection;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.util.Constants;

/**
 * Preprocessing.
 */
@Deprecated
public class Preprocessing {
  private static final Logger LOG = Logger.getLogger(Preprocessing.class);

  /**
   * Execute all preprocessing steps with the given options
   */
  @Deprecated
  public static Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, NullValue> graph,
                                                          String verbosity,
                                                          ExampleOutput out,
                                                          ExecutionEnvironment env) throws Exception {
    graph = graph.mapVertices(new InternalTypeMapFunction())
        .run(new EqualDataSourceLinkRemover(env));

    // todo stats still needed?
    // stats start
    // TODO cc computation produces memory an out exception, dont use
    if (verbosity.equals(Constants.DEBUG)) {
      graph = graph.run(new ConnectedComponentIdAdder<>(env)); // only needed for stats
      out.addPreClusterSizes("1 cluster sizes input graph", graph.getVertices(), Constants.CC_ID);
    }
    // stats end

    return graph
        .run(new TypeMisMatchCorrection(env))
        .run(new BasicEdgeSimilarityComputation(Constants.DEFAULT_VALUE, env));
  }
}
