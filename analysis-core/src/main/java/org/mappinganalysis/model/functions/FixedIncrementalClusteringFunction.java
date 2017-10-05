package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;

public class FixedIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private ExecutionEnvironment env;

  public FixedIncrementalClusteringFunction(ExecutionEnvironment env) {
    super();
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, ObjectMap> input) throws Exception {

    input.getVertices()
        .runOperation(new RepresentativeCreator(DataDomain.GEOGRAPHY));

    return null;
  }
}
