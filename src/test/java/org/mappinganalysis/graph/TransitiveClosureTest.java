package org.mappinganalysis.graph;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mappinganalysis.MySQLToFlink;
import org.mappinganalysis.utils.Utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by markus on 11/25/15.
 */
public class TransitiveClosureTest {
  @Test
  public void transitiveClosureTest() throws Exception {

    DataSet<Tuple2<Long, Long>> edges = MySQLToFlink
        .getInputGraph(Utils.GEO_FULL_NAME)
        .getEdges()
        .project(0, 1);

    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();

    List<Tuple2<Long, Long>> foo = Lists.newArrayList();
    foo.add(new Tuple2<>(3L, 1L));
    foo.add(new Tuple2<>(3L, 2L));
    foo.add(new Tuple2<>(3L, 4L));

    DataSet<Tuple2<Long, Long>> syntEdges = environment.fromCollection(foo);

    syntEdges.print();

    DataSet<Tuple2<Long, Long>> edges2 = syntEdges.project(1, 0);
    DataSet<Tuple2<Long, Long>> edgesJoined = syntEdges.union(edges2);


    IterativeDataSet<Tuple2<Long,Long>> paths = syntEdges.iterate(10);

    DataSet<Tuple2<Long,Long>> nextPaths = paths
        .join(syntEdges)
        .where(1)
        .equalTo(0)
        .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          /**
           left: Path (z,x) - x is reachable by z
           right: Edge (x,y) - edge x-->y exists
           out: Path (z,y) - y is reachable by z
           */
          public Tuple2<Long, Long> join(Tuple2<Long, Long> left, Tuple2<Long, Long> right) throws Exception {
            return new Tuple2<>(left.f0, right.f1);
          }
        }).withForwardedFieldsFirst("0").withForwardedFieldsSecond("1")
        .union(paths)
        .groupBy(0, 1)
        .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
            out.collect(values.iterator().next());
          }
        }).withForwardedFields("0;1");

    DataSet<Tuple2<Long,Long>> newPaths = paths
        .coGroup(nextPaths)
        .where(0).equalTo(0)
        .with(new CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          Set<Tuple2<Long,Long>> prevSet = new HashSet<>();
          @Override
          public void coGroup(Iterable<Tuple2<Long, Long>> prevPaths, Iterable<Tuple2<Long, Long>> nextPaths, Collector<Tuple2<Long, Long>> out) throws Exception {
            for (Tuple2<Long,Long> prev : prevPaths) {
              prevSet.add(prev);
            }
            for (Tuple2<Long,Long> next: nextPaths) {
              if (!prevSet.contains(next)) {
                out.collect(next);
              }
            }
          }
        }).withForwardedFieldsFirst("0").withForwardedFieldsSecond("0");

    DataSet<Tuple2<Long, Long>> transitiveClosure = paths.closeWith(nextPaths, newPaths);

    System.out.println(transitiveClosure.count());
    System.out.println(transitiveClosure.distinct().count());
    transitiveClosure.print();
  }
}
