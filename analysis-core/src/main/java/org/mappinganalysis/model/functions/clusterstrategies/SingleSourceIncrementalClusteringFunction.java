package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CandidateCreator;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.DualMergeGeographyMapper;
import org.mappinganalysis.model.functions.merge.FinalMergeGeoVertexCreator;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.IncrementalConfig;

public class SingleSourceIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private static final Logger LOG = Logger.getLogger(SingleSourceIncrementalClusteringFunction.class);
  private BlockingStrategy blockingStrategy;
  private String source;
  private int sourcesCount;
  private DataSet<Vertex<Long, ObjectMap>> toBeMergedElements;
  private ExecutionEnvironment env;
  private String metric = Constants.COSINE_TRIGRAM;
  private IncrementalConfig config;

  SingleSourceIncrementalClusteringFunction(
      BlockingStrategy blockingStrategy,
      DataSet<Vertex<Long, ObjectMap>> toBeMergedElements,
      String metric,
      String source,
      int sourcesCount,
      ExecutionEnvironment env) {
    super();
    this.blockingStrategy = blockingStrategy;
    this.metric = metric;
    this.source = source;
    this.sourcesCount = sourcesCount;
    this.toBeMergedElements = toBeMergedElements
        .runOperation(new RepresentativeCreator(DataDomain.GEOGRAPHY, blockingStrategy));
    this.env = env;
  }

  SingleSourceIncrementalClusteringFunction(
      DataSet<Vertex<Long, ObjectMap>> toBeMergedElements,
      IncrementalConfig config) {
    super();
    this.config = config;
    this.source = config.getNewSource();
    this.toBeMergedElements = toBeMergedElements
        .runOperation(new RepresentativeCreator(config));
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(
      Graph<Long, ObjectMap, NullValue> input) throws Exception {

    DataSet<Vertex<Long, ObjectMap>> baseClusters = input.getVertices()
        .runOperation(new RepresentativeCreator(config));

    return baseClusters.union(toBeMergedElements)
        .runOperation(new CandidateCreator(config, source, sourcesCount))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0).equalTo(0)
        .with(new FinalMergeGeoVertexCreator()) // TODO really needed?
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
        .runOperation(new RepresentativeCreator(config));
  }
}
