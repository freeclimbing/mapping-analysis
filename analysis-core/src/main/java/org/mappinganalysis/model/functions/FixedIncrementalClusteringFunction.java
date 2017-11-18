package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.DualMergeGeographyMapper;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

public class FixedIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private ExecutionEnvironment env;

  public FixedIncrementalClusteringFunction(ExecutionEnvironment env) {
    super();
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, ObjectMap> input) throws Exception {

    DataSet<Vertex<Long, ObjectMap>> baseClusters = input
        .mapVertices(new InternalTypeMapFunction())
        .getVertices()
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

    // split to relevant sources
    // todo source select TreeSet?
    // reduce search space
    DataSet<Vertex<Long, ObjectMap>> first = baseClusters
        .filter(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> second = baseClusters
        .filter(new SourceFilterFunction(Constants.NYT_NS));

//    DataSet<Vertex<Long, ObjectMap>> result =
        first.union(second)
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY))
        // TODO merge 2 sources
        // TODO provenance
        .flatMap(new DualMergeGeographyMapper(false));
//            .

//        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.GEOGRAPHY));

    // TODO NEXT WORK
    // add/fix merge for 2 sources
    // add other fixed data sources

    return null;
  }
}
