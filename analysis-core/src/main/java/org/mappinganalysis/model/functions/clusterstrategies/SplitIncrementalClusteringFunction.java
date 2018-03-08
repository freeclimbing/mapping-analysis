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
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;


/**
 * hardcoded only, not nice, but working for test.
 */
public class SplitIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private static final Logger LOG = Logger.getLogger(SplitIncrementalClusteringFunction.class);

  private BlockingStrategy blockingStrategy;
  private String metric;
  private String part;
  private ExecutionEnvironment env;

  SplitIncrementalClusteringFunction(BlockingStrategy blockingStrategy,
                                     String metric,
                                     String part,
                                     ExecutionEnvironment env) {
    super();
    this.blockingStrategy = blockingStrategy;
    this.metric = metric;
    this.part = part;
    this.env = env;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(
      Graph<Long, ObjectMap, NullValue> input) throws Exception {
    DataSet<Vertex<Long, ObjectMap>> baseClusters = input
        .mapVertices(new InternalTypeMapFunction())
        .getVertices()
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            blockingStrategy));

    DataSet<Vertex<Long, ObjectMap>> gn = baseClusters
        .filter(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> nyt = baseClusters
        .filter(new SourceFilterFunction(Constants.NYT_NS));
    DataSet<Vertex<Long, ObjectMap>> dbp = baseClusters
        .filter(new SourceFilterFunction(Constants.DBP_NS));
      DataSet<Vertex<Long, ObjectMap>> fb = baseClusters
          .filter(new SourceFilterFunction(Constants.FB_NS));

    DataSet<Vertex<Long, ObjectMap>> reducedBaseClusters = baseClusters.leftOuterJoin(gn)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(nyt)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(dbp)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(fb)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>());

    if (part.equals("eighty")) {
      DataSet<Vertex<Long, ObjectMap>> result = gn.union(nyt)
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.NYT_NS,
              2,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(baseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));

      result = result.union(dbp)
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.DBP_NS, 3,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(baseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));

      return result;
    } else if (part.equals("plusTen")) {
      reducedBaseClusters = reducedBaseClusters.union(gn)
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.GN_NS,
              3,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(reducedBaseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));

      reducedBaseClusters = reducedBaseClusters.union(nyt)
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.NYT_NS,
              3,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(reducedBaseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));

      reducedBaseClusters = reducedBaseClusters.union(dbp)
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.DBP_NS,
              3,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(reducedBaseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));

      return reducedBaseClusters;
    } else if (part.equals("fb")) {
      return baseClusters
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.FB_NS,
              4,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(baseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));
    } else if (part.equals("final")) {
      reducedBaseClusters = reducedBaseClusters.union(gn)
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.GN_NS,
              4,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(reducedBaseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));

      reducedBaseClusters = reducedBaseClusters.union(nyt)
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.NYT_NS,
              4,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(reducedBaseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));

      reducedBaseClusters = reducedBaseClusters.union(dbp)
          .runOperation(new CandidateCreator(
              blockingStrategy,
              DataDomain.GEOGRAPHY,
              metric, Constants.DBP_NS,
              4,
              env))
          .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(reducedBaseClusters)
          .where(0)
          .equalTo(0)
          .with(new FinalMergeGeoVertexCreator())
          .runOperation(new RepresentativeCreator(
              DataDomain.GEOGRAPHY,
              blockingStrategy));

      return reducedBaseClusters;
    }

    return null;
  }
}
