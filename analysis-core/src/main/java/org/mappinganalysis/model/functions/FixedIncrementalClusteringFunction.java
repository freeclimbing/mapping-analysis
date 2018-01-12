package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.DualMergeGeographyMapper;
import org.mappinganalysis.model.functions.merge.FinalMergeGeoVertexCreator;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

public class FixedIncrementalClusteringFunction
    extends IncrementalClusteringFunction {
  private static final Logger LOG = Logger.getLogger(FixedIncrementalClusteringFunction.class);

  private BlockingStrategy blockingStrategy;
  private ExecutionEnvironment env;

  FixedIncrementalClusteringFunction(BlockingStrategy blockingStrategy, ExecutionEnvironment env) {
    super();
    this.blockingStrategy = blockingStrategy;
    this.env = env;
  }

  // todo source select TreeSet?
  // TODO provenance
  // TODO sources count is fixed atm, how to have variable source count
  // TODO optimize: remove transition between vertices and tuples between merge steps
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
    DataSet<Vertex<Long, ObjectMap>> lgd = baseClusters
        .filter(new SourceFilterFunction(Constants.LGD_NS));

    DataSet<Vertex<Long, ObjectMap>> result = gn.union(nyt)
        .runOperation(new CandidateCreator(
            blockingStrategy,
            DataDomain.GEOGRAPHY,
            Constants.NYT_NS,
            2, env))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .map(x -> {
          if (x.getValue().getVerticesList().contains(2478L)
              || x.getValue().getVerticesList().contains(2479L)
              || x.getValue().getVerticesList().contains(3640L)) {
            LOG.info("gn+nyt FinalMergeGeoVertex: " + x.toString());
          }

          return x;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            blockingStrategy))
        .map(x -> {
          if (x.getValue().getVerticesList().contains(2478L)
              || x.getValue().getVerticesList().contains(2479L)
              || x.getValue().getVerticesList().contains(3640L)) {
            LOG.info("gn+nyt repr: " + x.toString());
          }

          return x;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    result = result.union(dbp)
        .map(x -> {
          if (x.getValue().getVerticesList().contains(2478L)
              || x.getValue().getVerticesList().contains(2479L)
              || x.getValue().getVerticesList().contains(3640L)) {
            LOG.info("2-3: " + x.toString());
          }

          return x;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .runOperation(new CandidateCreator(
            blockingStrategy,
            DataDomain.GEOGRAPHY,
            Constants.DBP_NS,
            3,
            env))
        .map(x -> {
          if (x.getSrcTuple().getId() == 2478L
              || x.getSrcTuple().getId() == 2479L
              || x.getSrcTuple().getId() == 3640L
              || x.getTrgTuple().getId() == 2478L
              || x.getTrgTuple().getId() == 2479L
              || x.getTrgTuple().getId() == 3640L) {
            LOG.info("cluster contained: " + x.toString());
          }
          if (x.getSrcTuple().getClusteredElements().contains(2478L)
              || x.getSrcTuple().getClusteredElements().contains(2479L)
              || x.getSrcTuple().getClusteredElements().contains(3640L)
              || x.getTrgTuple().getClusteredElements().contains(2479L)
              || x.getTrgTuple().getClusteredElements().contains(3640L)
              || x.getTrgTuple().getClusteredElements().contains(3640L)) {
            LOG.info("2-3 cand: " + x.toString());
          }

          return x;
        })
        .returns(new TypeHint<MergeGeoTriplet>() {})
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .map(x -> {
          if (x.getValue().getVerticesList().contains(2478L)
              || x.getValue().getVerticesList().contains(2479L)
              || x.getValue().getVerticesList().contains(3640L)) {
            LOG.info("gn+nyt+dbp FinalMergeGeoVertex: " + x.toString());
          }

          return x;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            blockingStrategy));

    result = result.union(fb)
        .runOperation(new CandidateCreator(
            blockingStrategy,
            DataDomain.GEOGRAPHY,
            Constants.FB_NS,
            4,
            env))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .map(x -> {
          if (x.getValue().getVerticesList().contains(2478L)
              || x.getValue().getVerticesList().contains(2479L)
              || x.getValue().getVerticesList().contains(3640L)) {
            LOG.info("gn+nyt+dbp+fb FinalMergeGeoVertex: " + x.toString());
          }

          return x;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            blockingStrategy));

//    DataSet<Vertex<Long, ObjectMap>> finalResult = result.union(lgd)
//        .runOperation(new CandidateCreator(
//            blockingStrategy, DataDomain.GEOGRAPHY, Constants.LGD_NS, 5))
//        .flatMap(new DualMergeGeographyMapper(false))
//        .leftOuterJoin(baseClusters)
//        .where(0)
//        .equalTo(0)
//        .with(new FinalMergeGeoVertexCreator())
//        .runOperation(new RepresentativeCreator(
//            DataDomain.GEOGRAPHY,
//            blockingStrategy));

    return result;
//    return result.leftOuterJoin(finalResult)
//        .where(0)
//        .equalTo(0)
//        .with((FlatJoinFunction<Vertex<Long, ObjectMap>,
//            Vertex<Long, ObjectMap>,
//            Vertex<Long, ObjectMap>>)
//            (first, second, out) -> {
//          if (second == null) {
//            LOG.info(first.toString());
//            out.collect(first);
//          }
//        })
//        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});
  }
}
