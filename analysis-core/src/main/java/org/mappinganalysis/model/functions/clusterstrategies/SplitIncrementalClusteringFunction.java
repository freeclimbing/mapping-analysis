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
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;


/**
 * not good anymore
 */
public class SplitIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private static final Logger LOG = Logger.getLogger(SplitIncrementalClusteringFunction.class);

  private final IncrementalConfig config;
  private String part;

  SplitIncrementalClusteringFunction(IncrementalConfig config, String part) {
    super();
    this.config = config;
    this.part = part;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(
      Graph<Long, ObjectMap, NullValue> input) throws Exception {
    DataSet<Vertex<Long, ObjectMap>> inputClusters = input
        .mapVertices(new InternalTypeMapFunction())
        .getVertices()
        .runOperation(new RepresentativeCreator(config));

    LOG.info("inputClusters: " + inputClusters.count());

    DataSet<Vertex<Long, ObjectMap>> gn = inputClusters
        .filter(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> nyt = inputClusters
        .filter(new SourceFilterFunction(Constants.NYT_NS));
    DataSet<Vertex<Long, ObjectMap>> dbp = inputClusters
        .filter(new SourceFilterFunction(Constants.DBP_NS));
      DataSet<Vertex<Long, ObjectMap>> fb = inputClusters
          .filter(new SourceFilterFunction(Constants.FB_NS));

      // TODO ??? why reduced - remove soon
//    DataSet<Vertex<Long, ObjectMap>> reducedBaseClusters = inputClusters
//        .leftOuterJoin(gn)
//        .where(0).equalTo(0)
//        .with(new LeftMinusRightSideJoinFunction<>())
//        .leftOuterJoin(nyt)
//        .where(0).equalTo(0)
//        .with(new LeftMinusRightSideJoinFunction<>())
//        .leftOuterJoin(dbp)
//        .where(0).equalTo(0)
//        .with(new LeftMinusRightSideJoinFunction<>())
//        .leftOuterJoin(fb)
//        .where(0).equalTo(0)
//        .with(new LeftMinusRightSideJoinFunction<>());
//
//    LOG.info("reducedBaseClusters: " + reducedBaseClusters.count());

    if (part.equals("eighty")) {
      DataSet<Vertex<Long, ObjectMap>> result = gn.union(nyt)
          .runOperation(new CandidateCreator(config, Constants.NYT_NS,2))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));

      result = result.union(dbp)
          .runOperation(new CandidateCreator(config, Constants.DBP_NS, 3))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));

      return result;
    } else if (part.equals("plusTen")) {
      inputClusters = inputClusters.union(gn)
          .runOperation(new CandidateCreator(config, Constants.GN_NS, 3))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));

      inputClusters = inputClusters.union(nyt)
          .runOperation(new CandidateCreator(config, Constants.NYT_NS, 3))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));

      inputClusters = inputClusters.union(dbp)
          .runOperation(new CandidateCreator(config, Constants.DBP_NS, 3))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));

      return inputClusters;
    } else if (part.equals("fb")) {
      return inputClusters
          .runOperation(new CandidateCreator(config, Constants.FB_NS, 4))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));
    } else if (part.equals("final")) {
      inputClusters = inputClusters.union(gn)
          .runOperation(new CandidateCreator(config, Constants.GN_NS, 4))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));

      inputClusters = inputClusters.union(nyt)
          .runOperation(new CandidateCreator(config, Constants.NYT_NS, 4))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));

      inputClusters = inputClusters.union(dbp)
          .runOperation(new CandidateCreator(config, Constants.DBP_NS, 4))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(inputClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(config));

      return inputClusters;
    }

    return null;
  }
}
