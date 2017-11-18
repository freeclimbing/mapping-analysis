package org.mappinganalysis.model.functions;

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
import org.junit.Assert;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.DualMergeGeographyMapper;
import org.mappinganalysis.model.functions.merge.FinalMergeGeoVertexCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.impl.Representative;
import org.mappinganalysis.model.impl.RepresentativeMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

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

  /**
   * Pre-test for createReprTest, 1x all elements one source, 1x mixed
   */
  @Test
  public void manyOneSourceCreateTripletsTest() throws Exception {
    // one source
    DataSet<MergeGeoTriplet> sai = getGnNytVertices()
        .filter(vertex -> vertex.getValue().getBlockingKey().equals("sai"))
        .map(new AddShadingTypeMapFunction())
        .map(new MergeGeoTupleCreator())
        .groupBy(7) // tuple blocking key
        .reduceGroup(new MergeGeoTripletCreator(2, true))
        .map(triplet -> {
          Assert.assertEquals(triplet.getSrcId(), triplet.getTrgId());
          return triplet;
        });
    Assert.assertEquals(5, sai.count());

    // mixed sources
    DataSet<MergeGeoTriplet> cha = getGnNytVertices()
        .filter(vertex -> vertex.getValue().getBlockingKey().equals("cha"))
        .map(new AddShadingTypeMapFunction())
        .map(new MergeGeoTupleCreator())
        .groupBy(7) // tuple blocking key
        .reduceGroup(new MergeGeoTripletCreator(2, true))
        .map(triplet -> {
//          LOG.info(triplet.toString());
          Assert.assertNotEquals(triplet.getSrcId(), triplet.getTrgId());
          return triplet;
        });
    Assert.assertEquals(25, cha.count());
  }

  /**
   * Get all vertices from gn and nyt.
   */
  private DataSet<Vertex<Long, ObjectMap>> getGnNytVertices() {
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
  private DataSet<Vertex<Long, ObjectMap>> getInputGeoGraph() {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph();

    return graph
        .mapVertices(new InternalTypeMapFunction())
        .getVertices()
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));
  }

  @Test
  public void createReprTest() throws Exception {
    DataSet<MergeGeoTriplet> result = getGnNytVertices()
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY));
//        .map(new MergeGeoTupleCreator()) // 1504 correct
//        // vertices having no 2. vertex in their block -> still candidate created
//        .distinct(0,1); // TODO why is this not distinct in the first place!?
////    LOG.info(tmp.count()); // 2400
////    LOG.info(tmp.distinct(0,1).count()); // 2348

    DataSet<MergeGeoTriplet> singleEntities = result.join(result)
        .where(0)
        .equalTo(1)
        .with((left, right) -> left)
        .returns(new TypeHint<MergeGeoTriplet>() {
        })
        .distinct(0, 1);

    Assert.assertEquals(82, singleEntities.count());
    Assert.assertEquals(793, result.count());
  }

  // TODO WIP
  @Test
  public void multiSourceTest() throws Exception {
    DataSet<Vertex<Long, ObjectMap>> baseClusters = getGnNytVertices();

    DataSet<Vertex<Long, ObjectMap>> tmp = baseClusters
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY))
        .flatMap(new DualMergeGeographyMapper(false))
          .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .runOperation(new RepresentativeCreator( // TODO CHECK THIS, is it single vertex only?
        DataDomain.GEOGRAPHY,                    // needed to recompute representatives!?
        BlockingStrategy.STANDARD_BLOCKING));

    DataSet<Vertex<Long, ObjectMap>> reps = getInputGeoGraph();

    DataSet<Vertex<Long, ObjectMap>> plusDbp = tmp
        .union(reps.filter(new SourceFilterFunction(Constants.DBP_NS)))
        .filter(vertex -> vertex.getValue().getBlockingKey().equals("ber"))
        .runOperation(new CandidateCreator(DataDomain.GEOGRAPHY))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator());

    // TODO dbpedia is not merged to the existing 2 data sources. candidate creator?

    plusDbp.print();
    LOG.info(plusDbp.count());
  }

  @Test
  @Deprecated
  public void depr() throws Exception {
    DataSet<MergeGeoTriplet> triplets = null;

    // why zip here??? TODO check minimize ids for STABLE MARRIAGE
    DataSet<Tuple2<Long, Tuple1<Long>>> uniqueLeftMatrixIds = DataSetUtils
        .zipWithUniqueId(triplets
            .<Tuple1<Long>>project(0) // TODO tuple1 -> long??
            .distinct());

//    LOG.info("second");
    DataSet<Tuple2<Long, Tuple1<Long>>> uniqueRightMatrixIds = DataSetUtils
        .zipWithUniqueId(triplets
            .<Tuple1<Long>>project(1)
            .distinct());
    // stats missing elements
//    intermediate.leftOuterJoin(result)
//        .where(0)
//        .equalTo(0)
//        .with((FlatJoinFunction<MergeGeoTuple, MergeGeoTriplet, MergeGeoTuple>) (left, right, out) -> {
//          if (right == null) {
//            out.collect(left);
//          }
//        })
//        .returns(new TypeHint<MergeGeoTuple>() {})
//        .leftOuterJoin(result)
//        .where(0)
//        .equalTo(1)
//        .with((FlatJoinFunction<MergeGeoTuple, MergeGeoTriplet, MergeGeoTuple>) (left, right, out) -> {
//          if (right == null) {
//            LOG.info("missing elements in result: " + left.toString());
//            out.collect(left);
//          }
//        })
//        .returns(new TypeHint<MergeGeoTuple>() {})
//        .collect();
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