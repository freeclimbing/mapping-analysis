package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.DualMergeGeographyMapper;
import org.mappinganalysis.model.functions.merge.FinalMergeGeoVertexCreator;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

public class FixedIncrementalClusteringFunction extends IncrementalClusteringFunction {
  private static final Logger LOG = Logger.getLogger(FixedIncrementalClusteringFunction.class);

  private ExecutionEnvironment env;

  public FixedIncrementalClusteringFunction(ExecutionEnvironment env) {
    super();
    this.env = env;
  }

  // todo source select TreeSet?
  // TODO provenance
  // TODO sources count is fixed atm, how to have variable source count
  // TODO optimize: remove transition between vertices and tuples between merge steps
  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(
      Graph<Long, ObjectMap, ObjectMap> input) throws Exception {

    DataSet<Vertex<Long, ObjectMap>> baseClusters = input
        .mapVertices(new InternalTypeMapFunction())
        .getVertices()
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

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
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY, Constants.NYT_NS, 2))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

    result = result.union(dbp)
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY, Constants.DBP_NS, 3))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

    result = result.union(fb)
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY, Constants.FB_NS, 4))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

    DataSet<Vertex<Long, ObjectMap>> finalResult = result.union(lgd)
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY, Constants.LGD_NS, 5))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

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
