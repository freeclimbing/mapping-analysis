package org.mappinganalysis.model.functions.refinement;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Triplet;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.Set;

public class CollectExcludeTuplesGroupReduceFunction implements GroupReduceFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple3<Long, Long, Long>> {
  private final int column;

  public CollectExcludeTuplesGroupReduceFunction(int column) {
    this.column = column;
  }

  @Override
  public void reduce(Iterable<Triplet<Long, ObjectMap, ObjectMap>> triplets,
                     Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
    Set<Tuple2<Long, Long>> tripletIds = Sets.newHashSet();

    Long clusterRefineId = fillIdsCheckDuplicateOntology(triplets, tripletIds, column);
    if (clusterRefineId == null) {
//              LOG.info("Exclude all case" + tripletIds.toString());
      for (Tuple2<Long, Long> tripletId : tripletIds) {
        collector.collect(new Tuple3<>(tripletId.f0, tripletId.f1, Long.MIN_VALUE));
      }
    } else {
//              LOG.info("Exclude none + enrich" + tripletIds.toString());
      for (Tuple2<Long, Long> tripletId : tripletIds) {
        collector.collect(new Tuple3<>(tripletId.f0, tripletId.f1, clusterRefineId));
      }
    }
  }

  /**
   * quick'n'dirty fix haha later todo
   */
  private Long fillIdsCheckDuplicateOntology(Iterable<Triplet<Long, ObjectMap, ObjectMap>> triplets,
                                             Set<Tuple2<Long, Long>> tripletIds, int column) {
    Set<String> ontologies = Sets.newHashSet();
    Long minimumId = null;

    for (Triplet<Long, ObjectMap, ObjectMap> triplet : triplets) {
      Long srcId = triplet.getSrcVertex().getId();
      Long trgId = triplet.getTrgVertex().getId();
      tripletIds.add(new Tuple2<>(srcId, trgId));
      Long tmpId = srcId < trgId ? srcId : trgId;
      if (minimumId == null || minimumId > tmpId) {
        minimumId = tmpId;
      }

      Set<String> tripletOnts;
      if (column == 1) {
        tripletOnts = (Set<String>) triplet.getTrgVertex().getValue().get(Utils.ONTOLOGIES);
      } else {
        tripletOnts = (Set<String>) triplet.getSrcVertex().getValue().get(Utils.ONTOLOGIES);
      }

      for (String ont : tripletOnts) {
        if (!ontologies.add(ont)) {
//                  LOG.info("problem duplicate ontology triplet: " + triplet);
          minimumId = null;
          break;
        }
      }
    }
    return minimumId;
  }
}
