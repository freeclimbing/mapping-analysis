package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.incremental.SourceSelectFilter;
import org.mappinganalysis.model.functions.incremental.StableMarriageReduceFunction;
import org.mappinganalysis.model.functions.merge.MergeGeoSimilarity;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.Representative;
import org.mappinganalysis.model.impl.RepresentativeMap;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;

import static org.junit.Assert.assertEquals;

public class IncrementalClusteringTest {
  private static final Logger LOG = Logger.getLogger(org.mappinganalysis.model.functions.decomposition.simsort.SimSortTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  @Test
  public void fixedStrategyIncClusteringTest() throws Exception {
    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.FIXED)
        .build();

    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph()
            .run(clustering);
  }

  @Test
  public void createReprTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
          .getGraph();

    DataSet<Vertex<Long, ObjectMap>> reps = graph.getVertices()
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

    DataSet<Vertex<Long, ObjectMap>> first = reps
        .filter(new SourceSelectFilter(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> second = reps
        .filter(new SourceSelectFilter(Constants.NYT_NS));

    // temp
    SimilarityFunction<MergeGeoTriplet, MergeGeoTriplet> simFunction =
        new MergeGeoSimilarity();

    // TODO check sim comp
    SimilarityComputation<MergeGeoTriplet,
            MergeGeoTriplet> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeGeoTriplet,
        MergeGeoTriplet>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.5)
        .build();

    DataSet<MergeGeoTriplet> triplets = first.union(second)
        .map(new AddShadingTypeMapFunction())
        .map(new MergeGeoTupleCreator())
        .groupBy(7)
        .reduceGroup(new MergeGeoTripletCreator(4))
        .runOperation(similarityComputation)
        .map(x->{
          LOG.info(x.toString());
          return x;
        })
        .returns(new TypeHint<MergeGeoTriplet>() {})
        // todo remove 2 lines
//        .filter(triplet -> triplet.getBlockingLabel().equals("nor"))
//        .returns(new TypeHint<MergeGeoTriplet>() {})
        .groupBy(5)
        .reduceGroup(new StableMarriageReduceFunction());
    // temp

    triplets = triplets.first(100);

    LOG.info("triplets");
    triplets.print();
    LOG.info("first");
//        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY));

    DataSet<Tuple2<Long, Tuple1<Long>>> uniqueLeftMatrixIds = DataSetUtils
        .zipWithUniqueId(triplets
            .<Tuple1<Long>>project(0) // TODO tuple1 -> long??
            .distinct());

    LOG.info("second");
    DataSet<Tuple2<Long, Tuple1<Long>>> uniqueRightMatrixIds = DataSetUtils
        .zipWithUniqueId(triplets
            .<Tuple1<Long>>project(1)
            .distinct());

    triplets.rightOuterJoin(uniqueLeftMatrixIds)
        .where(0)
        .equalTo(1)
        .with(new JoinFunction<MergeGeoTriplet, Tuple2<Long, Tuple1<Long>>, MergeGeoTriplet>() {
          @Override
          public MergeGeoTriplet join(MergeGeoTriplet first, Tuple2<Long, Tuple1<Long>> second) throws Exception {
            LOG.info(first + " ### " + second);
            return first;
          }
        }).print();

    // todo check result
  }

  /**
   * TODO faulty test, fix Representative and ReprMap
   * TODO more likely, fix ObjectMap implementing Map not properly!?
   */
  @Test
  public void customReprTest() throws Exception {
        String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
          .getGraph();

    DataSet<Representative> output = graph.getVertices()
        .first(1)
        .map(new MapFunction<Vertex<Long, ObjectMap>, Representative>() {
          @Override
          public Representative map(Vertex<Long, ObjectMap> value) throws Exception {
            LOG.info("####");
            Representative representative = new Representative(value, DataDomain.GEOGRAPHY);
            LOG.info("rep: " + representative.toString());

            RepresentativeMap props = representative.getValue();
            LOG.info(props.size());
//            props.setBlockingKey(BlockingStrategy.STANDARD_BLOCKING);
//            LOG.info(props.size());
            LOG.info("props: " + props);

            return representative;
          }
        });

        output.print();

  }

  @Test
  public void incCountDataSourceElementsTest() throws Exception {
    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.MINSIZE)
        .build();

    String graphPath = IncrementalClusteringTest.class
//        .getResource("/data/preprocessing/oneToMany").getFile();
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph()
            .run(clustering);

    for (Vertex<Long, ObjectMap> vertex : graph.getVertices().collect()) {
      ObjectMap properties = vertex.getValue();

      if (properties.getDataSource().equals(Constants.GN_NS)) {
        assertEquals(749L, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.NYT_NS)) {
        assertEquals(755, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.DBP_NS)) {
        assertEquals(774, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.FB_NS)) {
        assertEquals(776, properties.getDataSourceEntityCount().longValue());
      }
    }
  }

}