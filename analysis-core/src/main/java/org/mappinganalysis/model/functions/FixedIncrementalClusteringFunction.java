package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.incremental.SourceSelectFilter;
import org.mappinganalysis.util.Constants;

public class FixedIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private ExecutionEnvironment env;

  public FixedIncrementalClusteringFunction(ExecutionEnvironment env) {
    super();
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, ObjectMap> input) throws Exception {

    DataSet<Vertex<Long, ObjectMap>> baseClusters = input.getVertices()
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

    // split to relevant sources
    // todo source select TreeSet?
    // reduce search space
    DataSet<Vertex<Long, ObjectMap>> first = baseClusters
        .filter(new SourceSelectFilter(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> second = baseClusters
        .filter(new SourceSelectFilter(Constants.NYT_NS));

    // todo union or param?
//    DataSet<Vertex<Long, ObjectMap>> result =
        first.union(second)
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY));
        // TODO merge 2 sources
        // TODO provenance
//        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.GEOGRAPHY)); // todo rename

    return null;
  }
}
