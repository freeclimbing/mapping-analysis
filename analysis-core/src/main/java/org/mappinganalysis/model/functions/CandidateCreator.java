package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
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
import org.mappinganalysis.model.functions.blocking.lsh.utils.CandidateGeoMergeTripletCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.SwitchMapFunction;
import org.mappinganalysis.model.functions.blocking.lsh.utils.VertexWithNewObjectMapFunction;
import org.mappinganalysis.model.functions.incremental.HungarianAlgorithmReduceFunction;
import org.mappinganalysis.model.functions.merge.MergeGeoSimilarity;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.config.IncrementalConfig;

// TODO candidates based on blocking strategy
// TODO restrict candidates to needed properties!?

/**
 * INCREMENTAL
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
  private String metric;
  private String newSource;
  private int sourceCount;
  private ExecutionEnvironment env;
  private DataSet<Vertex<Long, ObjectMap>> inputVertices;
  private IncrementalConfig config;

  /**
   * Constructor for incremental clustering
   */
  public CandidateCreator(Config config, String newSource, int sourceCount) {
    this.blockingStrategy = config.getBlockingStrategy();
    this.domain = config.getDataDomain();
    this.metric = config.getMetric();
    this.env = config.getExecutionEnvironment();
    this.newSource = newSource;
    this.sourceCount = sourceCount;
  }

  public CandidateCreator(IncrementalConfig config) {
    this.config = config;
    this.blockingStrategy = config.getBlockingStrategy();
    this.domain = config.getDataDomain();
    this.metric = config.getMetric();
    this.env = config.getExecutionEnvironment();
    this.newSource = config.getNewSource();
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
        .setSimilarityFunction(new MergeGeoSimilarity(metric)) // TODO check sim function
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.7)
        .build();

     if (!blockingStrategy.equals(BlockingStrategy.LSH_BLOCKING)) {
      /*
        STANDARD BLOCKING / BLOCK SPLIT
       */
      return inputVertices
          .map(new AddShadingTypeMapFunction())
          .map(new MergeGeoTupleCreator(blockingStrategy))
          .groupBy(7)
          .reduceGroup(new MergeGeoTripletCreator(
              sourceCount, newSource, true))
          .distinct(0, 1)
          .runOperation(similarityComputation)
          .groupBy(5)
          .reduceGroup(new HungarianAlgorithmReduceFunction());

    } else {
    /*
    TODO LSH MUCH CODE refactor
     */
      boolean isIdfOptimizeEnabled = true;
      boolean isLogEnabled = false;
      DataSet<Tuple2<Long, Long>> lshCandidates = inputVertices
          .map(vertex -> new Tuple2<>(vertex.getId(),
              Utils.simplify(vertex.getValue().getLabel())))
          .returns(new TypeHint<Tuple2<Long, String>>() {})
          .runOperation(new LshCandidateCreator(isIdfOptimizeEnabled));

      DataSet<MergeGeoTuple> geoTuples = inputVertices
          .map(new AddShadingTypeMapFunction())
          .map(new MergeGeoTupleCreator(BlockingStrategy.NO_BLOCKING));

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
          .where(0).equalTo(0)
          .with(new CandidateGeoMergeTripletCreator(0))
          .join(geoTuples)
          .where(1).equalTo(0)
          .with(new CandidateGeoMergeTripletCreator(1))
          .map(new SwitchMapFunction(newSource))
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
          .distinct();

      DataSet<Tuple2<Long, Long>> flatMappedGeoTuples = geoTuples
          .flatMap(new FlatMapFunction<MergeGeoTuple, Tuple2<Long, Long>>() {
            @Override
            public void flatMap(MergeGeoTuple cluster, Collector<Tuple2<Long, Long>> out) throws Exception {
              for (Long vertex : cluster.getClusteredElements()) {
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
          .where(0).equalTo(0)
          .with((left, right) -> right)
          .returns(new TypeHint<MergeGeoTuple>() {})
          .map(tuple -> {
//            LOG.info("rec tuple: " + tuple.toString());
            return new MergeGeoTriplet(tuple, tuple, 0d);
          })
          .returns(new TypeHint<MergeGeoTriplet>() {});

      mergeGeoTriplets = mergeGeoTriplets
          .union(recovered)
        .distinct(0,1);

      DataSet<Edge<Long, NullValue>> edges = mergeGeoTriplets
          .map(x -> {
            if (isLogEnabled && (x.f0 == 298L || x.f0 == 299L || x.f0 == 5013L || x.f0 == 5447L) &&
                (x.f1 == 298L || x.f1 == 299L || x.f1 == 5013L || x.f1 == 5447L)) {
              LOG.info("edge: " + x.toString());
            }
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
    }
  }
}
