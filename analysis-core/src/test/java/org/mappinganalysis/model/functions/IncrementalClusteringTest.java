package org.mappinganalysis.model.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.integration.IncrementalGeoClusteringTest;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.DualMergeGeographyMapper;
import org.mappinganalysis.model.functions.merge.FinalMergeGeoVertexCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

import java.util.*;

import static org.junit.Assert.*;

public class IncrementalClusteringTest {
  private static final Logger LOG = Logger.getLogger(IncrementalClusteringTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  @Test
  public void sourceSelectCountTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getVertices();

    DataSet<Vertex<Long, ObjectMap>> gn = vertices
        .filter(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> nyt = vertices
        .filter(new SourceFilterFunction(Constants.NYT_NS));

    assertEquals(749L, gn.count());
    assertEquals(755, nyt.count());
  }

  @Test
  public void createValidTripletsTest() throws Exception {
    // one source (some entities are only available in one source, here: GeoNames)
    DataSet<MergeGeoTriplet> sai = getGnNytVertices()
        .filter(vertex -> vertex.getValue().getBlockingKey().equals("sai"))
        .map(new MergeGeoTupleCreator())
        .groupBy(7) // tuple blocking key
        .reduceGroup(new MergeGeoTripletCreator(
            2,
            Constants.NYT_NS,
            true))
        .map(triplet -> {
          Assert.assertEquals(triplet.getSrcId(), triplet.getTrgId());
          return triplet;
        });
    Assert.assertEquals(5, sai.count());

    // mixed sources
    // if multiple sources are available, created triplets never have the same source and target id
    DataSet<MergeGeoTriplet> cha = getGnNytVertices()
        .filter(vertex -> vertex.getValue().getBlockingKey().equals("cha"))
        .map(new MergeGeoTupleCreator())
        .groupBy(7) // tuple blocking key
        .reduceGroup(new MergeGeoTripletCreator(
            2,
            Constants.NYT_NS,
            true))
        .map(triplet -> {
          Assert.assertNotEquals(triplet.getSrcId(), triplet.getTrgId());
          return triplet;
        });
    Assert.assertEquals(25, cha.count());
  }

  @Test
  public void createReprTest() throws Exception {
    Config config = new Config(DataDomain.GEOGRAPHY, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setMetric(Constants.COSINE_TRIGRAM);

    DataSet<MergeGeoTriplet> result = getGnNytVertices()
        .runOperation(new CandidateCreator(config,
            Constants.NYT_NS,
            2));
//        .map(new MergeGeoTupleCreator()) // 1504 correct

    /*
      isolated elements because no match in blocking step
     */
    DataSet<MergeGeoTriplet> singleEntities = result.join(result)
        .where(0)
        .equalTo(1)
        .with((left, right) -> left)
        .returns(new TypeHint<MergeGeoTriplet>() {})
        .distinct(0, 1);
    Assert.assertEquals(749, result.count());
    Assert.assertEquals(59, singleEntities.count());

    getGnNytVertices().leftOuterJoin(result)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(result)
        .where(0)
        .equalTo(1)
        .with(new LeftMinusRightSideJoinFunction<>())
        .print();
  }

  @Test
  public void noBlockingTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);

    IncrementalConfig config = new IncrementalConfig(DataDomain.GEOGRAPHY, env);
    config.setBlockingStrategy(BlockingStrategy.NO_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.FIXED_SEQUENCE);
    config.setMetric(Constants.COSINE_TRIGRAM);

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder(config)
        .build();

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph
        .run(clustering);

    new JSONDataSink(graphPath.concat("/output-no-blocking/"), "test")
        .writeVertices(resultVertices);

    QualityUtils.printGeoQuality(resultVertices, config);
    resultVertices.print();
    // TODO TEST
  }

  /**
   * Playground example for incremental clustering GEOGRAPHY
   *
   * less than 20 vertices
   */
  @Test
  public void playgroundIncrementalGeoTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/incremental/clusters").getFile();
    /*
    input graph without edges
     */
    Graph<Long, ObjectMap, NullValue> baseClusterGraph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);

    String newVerticesPath = IncrementalClusteringTest.class
        .getResource("/data/incremental/newVertices").getFile();
    DataSet<Vertex<Long, ObjectMap>> newVertices =
        new JSONDataSource(newVerticesPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class)
            .getVertices();

    IncrementalConfig config = new IncrementalConfig(DataDomain.GEOGRAPHY, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(ClusteringStep.VERTEX_ADDITION);
    config.setSimSortSimilarity(0.7);

    IncrementalClustering vertexAddClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .build();

    DataSet<Vertex<Long, ObjectMap>> result = baseClusterGraph
        .run(vertexAddClustering);

    Collection<Vertex<Long, ObjectMap>> representatives = Lists.newArrayList();
    result.output(new LocalCollectionOutputFormat<>(representatives));
    env.execute();

    for (Vertex<Long, ObjectMap> representative : representatives) {
      Set<Long> verticesList = representative.getValue().getVerticesList();
      if (representative.getId() == 2484L) {
        assertTrue(verticesList.contains(2484L));
        assertTrue(verticesList.contains(2485L));
        assertTrue(verticesList.contains(6748L));
      } else if (representative.getId() == 23L) {
        assertTrue(verticesList.contains(4800L));
        assertTrue(verticesList.contains(23L));
        assertTrue(verticesList.contains(24L));
        assertTrue(verticesList.contains(25L));
      } else {
        assertFalse(true);
      }
    }
  }

  @Test
  public void incCountDataSourceElementsTest() throws Exception {
    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.MULTI)
        .build();

    String graphPath = IncrementalGeoClusteringTest.class
        .getResource("/data/geography").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class)
            .run(clustering);

    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
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

  /**
   * Used for optimization.
   */
  // TODO check sim comp strat
  // TODO last join with FinalMergeGeoVertexCreator unneeded?
  // TODO RepresentativeCreator only adds blocking label, remove and use map function
  @Test
  public void multiSourceTest() throws Exception {
    IncrementalConfig config = new IncrementalConfig(DataDomain.GEOGRAPHY, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setMetric(Constants.COSINE_TRIGRAM);

    DataSet<Vertex<Long, ObjectMap>> baseClusters = getGnNytVertices();

    DataSet<Vertex<Long, ObjectMap>> tmp = baseClusters
        .runOperation(new CandidateCreator(config, Constants.NYT_NS,2))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .runOperation(new RepresentativeCreator(config));

    DataSet<Vertex<Long, ObjectMap>> reps = getInputGeoGraph();

    DataSet<Vertex<Long, ObjectMap>> plusDbp = tmp
        .union(reps.filter(new SourceFilterFunction(Constants.DBP_NS)))
        .runOperation(new CandidateCreator(config, Constants.DBP_NS,3))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

//    plusDbp.print();
//    LOG.info(plusDbp.count());

    DataSet<MergeGeoTriplet> tupleResult = plusDbp
        .union(reps.filter(new SourceFilterFunction(Constants.FB_NS)))
        .runOperation(new CandidateCreator(config, Constants.FB_NS,4));

    DataSet<MergeGeoTriplet> singleEntities = tupleResult.join(tupleResult)
        .where(0)
        .equalTo(1)
        .with((left, right) -> left)
        .returns(new TypeHint<MergeGeoTriplet>() {
        })
        .distinct(0, 1);

    DataSet<Vertex<Long, ObjectMap>> plusFb = tupleResult
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator());

//    LOG.info(singleEntities.count());
//    LOG.info(plusFb.count());
    assertEquals(152,singleEntities.count());
    assertEquals(696,plusFb.count());

    plusFb.print();

    config.setProperty(Constants.DATASET, "incremental test");
    QualityUtils.printGeoQuality(plusFb, config);

  }

  /**
   * Get all vertices from gn and nyt.
   */
  private DataSet<Vertex<Long, ObjectMap>> getGnNytVertices() throws Exception {
    DataSet<Vertex<Long, ObjectMap>> reps = getInputGeoGraph();

    DataSet<Vertex<Long, ObjectMap>> first = reps
        .filter(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> second = reps
        .filter(new SourceFilterFunction(Constants.NYT_NS));

    return first.union(second);
  }

  /**
   * Get all vertices from geo graph with basic representatives.
   */
  private DataSet<Vertex<Long, ObjectMap>> getInputGeoGraph() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph();

    return graph
        .mapVertices(new InternalTypeMapFunction())
        .getVertices()
        .map(new AddShadingTypeMapFunction())
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));
  }

//  @Test
//  public void splitCreation() throws Exception {
//    String graphPath = IncrementalClusteringTest.class
//        .getResource("/data/geography").getFile();
//    Graph<Long, ObjectMap, NullValue> graph =
//        new JSONDataSource(graphPath, true, env)
//            .getGraph(ObjectMap.class, NullValue.class);
//
//    FilterOperator<Vertex<Long, ObjectMap>> verts = graph
//        .getVertices()
//        .filter(new SourceFilterFunction(Constants.FB_NS));
//
//    List<Vertex<Long, ObjectMap>> vertices = verts.collect();
//    Collections.shuffle(vertices);
//
//    String first80gn = "80-db: ";
//    String add10gn = "10-add-db: ";
//    String final10gn = "10-final-db: ";
//    int counter = 0;
//    int all = 0;
//    int first = 0;
//    int add = 0;
//    int rest = 0;
//    for (Vertex<Long, ObjectMap> vertex : vertices) {
//      if (counter <= 7) {
//        first80gn = first80gn.concat(vertex.getId().toString())
//            .concat("L, ");
//        ++first;
//      } else if (counter == 8) {
//        add10gn = add10gn.concat(vertex.getId().toString())
//            .concat("L, ");
//        ++add;
//      } else if (counter == 9) {
//        final10gn = final10gn.concat(vertex.getId().toString())
//            .concat("L, ");
//        ++rest;
//      }
//
//      ++counter;
//      ++all;
//      if (counter == 10) {
//        counter = 0;
//      }
//    }
//
//    LOG.info(first80gn);
//    LOG.info(add10gn);
//    LOG.info(final10gn);
//    LOG.info(first + " " + add + " " + rest + " " + all);
//  }
}