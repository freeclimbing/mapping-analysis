package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.blocking.lsh.LshCandidateCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.CandidateMergeTripletCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.SwitchMapFunction;
import org.mappinganalysis.model.functions.blocking.lsh.utils.VertexWithNewObjectMapFunction;
import org.mappinganalysis.model.functions.incremental.HungarianAlgorithmReduceFunction;
import org.mappinganalysis.model.functions.merge.MergeGeoSimilarity;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;

// TODO candidates based on blocking strategy
// TODO restrict candidates to needed properties!?

/**
 * Candidate Creator prepares MergeTuples for a domain (currently: geographic), distinguishes
 * elements into blocks according to blocking strategy and computes similarity according to
 * similarity strategy for all candidates which are named MergeTriplets. Finally, with
 * HungarianAlgorithm a selection of best candidates is done.
 */
public class CandidateCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, MergeGeoTriplet> {
  private static final Logger LOG = Logger.getLogger(CandidateCreator.class);
  private BlockingStrategy blockingStrategy;
  private DataDomain domain;
  private String newSource;
  private int sourceCount;
  private ExecutionEnvironment env;
  private DataSet<Vertex<Long, ObjectMap>> inputVertices;

  /**
   * Constructor for incremental clustering, ids are not
   */
  public CandidateCreator(
      BlockingStrategy blockingStrategy,
      DataDomain domain,
      String newSource,
      int sourceCount,
      ExecutionEnvironment env) {
    this.blockingStrategy = blockingStrategy;
    this.domain = domain; // TODO USE domain
    this.newSource = newSource;
    this.sourceCount = sourceCount;
    this.env = env;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> inputData) {
    inputVertices = inputData;
  }

  @Override
  public DataSet<MergeGeoTriplet> createResult() {
    // TODO check sim comp
    SimilarityComputation<MergeGeoTriplet,
        MergeGeoTriplet> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeGeoTriplet,
        MergeGeoTriplet>()
        .setSimilarityFunction(new MergeGeoSimilarity()) // TODO check sim function
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.4) // TODO check
        .build();

    if (blockingStrategy.equals(BlockingStrategy.LSH_BLOCKING)) {
      boolean isIdfOptimizeEnabled = true;
      boolean isLogEnabled = false;
      DataSet<Tuple2<Long, Long>> lshCandidates = inputVertices
//          .map(new TempLogSourceCountPrintFunction(sourceCount)) // delete after test complete
//          .map(x -> {
//            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L)) {
//              LOG.info("pre LSH: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
          .runOperation(new LshCandidateCreator(isIdfOptimizeEnabled));

      DataSet<MergeGeoTuple> geoTuples = inputVertices
          .map(new AddShadingTypeMapFunction())
          .map(new MergeGeoTupleCreator(BlockingStrategy.NO_BLOCKING))
//          .map(x -> {
//            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L)) {
//              LOG.info("MergeTupleCreator: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<MergeGeoTuple>() {})
          ;

      DataSet<MergeGeoTriplet> mergeGeoTriplets = lshCandidates
//          .map(x -> {
//            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
//                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
//              LOG.info("after LSH: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<Tuple2<Long, Long>>() {
//          })
          .map(candidate -> new MergeGeoTriplet(candidate.f0, candidate.f1))
          .returns(new TypeHint<MergeGeoTriplet>() {})
          .join(geoTuples)
          .where(0)
          .equalTo(0)
          .with(new CandidateMergeTripletCreator(0))
          .join(geoTuples)
          .where(1)
          .equalTo(0)
          .with(new CandidateMergeTripletCreator(1))
          .map(new SwitchMapFunction(newSource))
//          .map(x -> {
//            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
//                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
//              LOG.info("pre sim comp: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<MergeGeoTriplet>() {})
          .runOperation(similarityComputation)
//          .map(x -> {
//            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
//                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
//              LOG.info("sim: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<MergeGeoTriplet>() {})
          ;

      DataSet<Tuple1<Long>> coveredTuples = mergeGeoTriplets
          .flatMap(new FlatMapFunction<MergeGeoTriplet, Tuple1<Long>>() {
            @Override
            public void flatMap(MergeGeoTriplet triplet, Collector<Tuple1<Long>> out) throws Exception {
              for (Long src : triplet.getSrcTuple().getClusteredElements()) {
                out.collect(new Tuple1<>(src));
              }
              for (Long trg : triplet.getTrgTuple().getClusteredElements()) {
                out.collect(new Tuple1<>(trg));
              }
            }
          })
          .distinct()
//          .map(x -> {
//            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L)) {
//              LOG.info("covered Tuple: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<Tuple1<Long>>() {})
          ;

      DataSet<Tuple2<Long, Long>> flatMappedGeoTuples = geoTuples
          .flatMap(new FlatMapFunction<MergeGeoTuple, Tuple2<Long, Long>>() {
            @Override
            public void flatMap(MergeGeoTuple cluster, Collector<Tuple2<Long, Long>> out) throws Exception {
              for (Long vertex : cluster.getClusteredElements()) {
//                LOG.info("flat geo tuple ITER: " + cluster.toString());
                out.collect(new Tuple2<>(cluster.f0, vertex));
              }
            }
          })
          .distinct();

      DataSet<MergeGeoTriplet> recovered = flatMappedGeoTuples
          .leftOuterJoin(coveredTuples)
          .where(1)
          .equalTo(0)
          .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple1<Long>, Tuple2<Long, Long>>() {
            @Override
            public void join(Tuple2<Long, Long> left, Tuple1<Long> right, Collector<Tuple2<Long, Long>> out) throws Exception {
              if (right == null) {
//                LOG.info("flat geo tuple: " + left.toString());
//                if (left.f1 == 298L || left.f1 == 299L || left.f1 == 5013L || left.f1 == 5447L) {
//                  LOG.info("loj: " + left.toString());
//                }
                out.collect(left); // cluster, containedVertex
              }
            }
          })
          .join(geoTuples)
          .where(0)
          .equalTo(0)
          .with((left, right) -> right)
          .returns(new TypeHint<MergeGeoTuple>() {})
          .map(tuple -> {
//            LOG.info("rec tuple: " + tuple.toString());
            return new MergeGeoTriplet(tuple, tuple, 0d);
          })
          .returns(new TypeHint<MergeGeoTriplet>() {});

      mergeGeoTriplets = mergeGeoTriplets.union(recovered)
//          .map(x -> {
//            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
//                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)
//                && x.f0 == x.f1.longValue()) {
//              LOG.info("recovered tuple: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<MergeGeoTriplet>() {})
      .distinct(0,1);

      DataSet<Edge<Long, NullValue>> edges = mergeGeoTriplets
          .map(x -> {
            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
              LOG.info("edge: " + x.toString());
            }
//              if (candidate.f0 == 3335L || candidate.f1 == 3335L) {
//                LOG.info("edge in CandidateCreator: " + candidate.toString());
//              }
              return new Edge<>(x.f0, x.f1, NullValue.getInstance());
          })
          .returns(new TypeHint<Edge<Long, NullValue>>() {});

      DataSet<Vertex<Long, ObjectMap>> vertices = null;
      try {
        vertices = Graph.fromDataSet(edges, env)
            .mapVertices(new VertexWithNewObjectMapFunction())
            .run(new ConnectedComponentIdAdder<>(env))
            .getVertices();
      } catch (Exception e) {
        e.printStackTrace();
      }

      assert vertices != null;
      return mergeGeoTriplets.join(vertices)
          .where(0)
          .equalTo(0)
          .with(new JoinFunction<MergeGeoTriplet, Vertex<Long,ObjectMap>, MergeGeoTriplet>() {
            @Override
            public MergeGeoTriplet join(MergeGeoTriplet triplet, Vertex<Long, ObjectMap> vertex) throws Exception {
              triplet.setBlockingLabel(vertex.getValue().getCcId().toString());
              triplet.getSrcTuple().setBlockingLabel(vertex.getValue().getCcId().toString());
              triplet.getTrgTuple().setBlockingLabel(vertex.getValue().getCcId().toString());

              return triplet;
            }
          })
          .groupBy(5)
          .reduceGroup(new HungarianAlgorithmReduceFunction());
    } else {

      return inputVertices
          .map(new AddShadingTypeMapFunction())
          .map(new MergeGeoTupleCreator(blockingStrategy))
//          .map(x -> {
//            if (x.getId() == 237L) {
//              LOG.info("pre: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<MergeGeoTuple>() {})
          .groupBy(7)
          .reduceGroup(new MergeGeoTripletCreator(
              sourceCount, newSource, true))
          .distinct(0, 1)
          .runOperation(similarityComputation)
//          .map(x -> {
//            if (x.getSrcId() == 237L || x.getTrgId() == 237L) {
//              LOG.info("CandidateCreator: " + x.toString());
//            }
//            return x;
//          })
//          .returns(new TypeHint<MergeGeoTriplet>() {})
          .groupBy(5)
          .reduceGroup(new HungarianAlgorithmReduceFunction());
    }
  }

  private static class TempLogSourceCountPrintFunction
      implements MapFunction<Vertex<Long,ObjectMap>, Vertex<Long, ObjectMap>> {
    private int sourceCount;

    public TempLogSourceCountPrintFunction(int sourceCount) {
      this.sourceCount = sourceCount;
    }

    @Override
    public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> value) throws Exception {
      if (value.getId() == 0L) {
        LOG.info("sourceCount: " + sourceCount);
      }
      return value;
    }
  }
}
