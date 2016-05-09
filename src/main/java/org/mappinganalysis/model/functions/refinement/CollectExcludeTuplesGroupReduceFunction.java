package org.mappinganalysis.model.functions.refinement;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Triplet;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.Set;

public class CollectExcludeTuplesGroupReduceFunction
    implements GroupReduceFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple4<Long, Long, Long, Double>> {
  private final int column;

  public CollectExcludeTuplesGroupReduceFunction(int column) {
    this.column = column;
  }

  @Override
  public void reduce(Iterable<Triplet<Long, ObjectMap, ObjectMap>> triplets,
                     Collector<Tuple4<Long, Long, Long, Double>> collector) throws Exception {
    Set<Tuple2<Long, Long>> tripletIds = Sets.newHashSet();

    Tuple2<Long, Double> clusterRefineId = fillIdsCheckDuplicateOntology(triplets, tripletIds, column);
    if (clusterRefineId == null) {
//              LOG.info("Exclude all case" + tripletIds.toString());
      for (Tuple2<Long, Long> tripletId : tripletIds) {
        collector.collect(new Tuple4<>(tripletId.f0, tripletId.f1, Long.MIN_VALUE, 0D));
      }
    } else {
//              LOG.info("Exclude none + enrich" + tripletIds.toString());
      for (Tuple2<Long, Long> tripletId : tripletIds) {
        collector.collect(new Tuple4<>(tripletId.f0, tripletId.f1, clusterRefineId.f0, clusterRefineId.f1));
      }
    }
  }

  /**
   * quick'n'dirty fix haha later todo
   */
  private Tuple2<Long, Double> fillIdsCheckDuplicateOntology(Iterable<Triplet<Long, ObjectMap, ObjectMap>> triplets,
                                                             Set<Tuple2<Long, Long>> tripletIds, int column) {
    Set<String> ontologies = Sets.newHashSet();
    Tuple2<Long, Double> minimumIdAndSim = null;

    for (Triplet<Long, ObjectMap, ObjectMap> triplet : triplets) {
      Long srcId = triplet.getSrcVertex().getId();
      Long trgId = triplet.getTrgVertex().getId();
      tripletIds.add(new Tuple2<>(srcId, trgId));
      Long tmpId = srcId < trgId ? srcId : trgId;
      if (minimumIdAndSim == null || minimumIdAndSim.f0 > tmpId) {
        minimumIdAndSim = new Tuple2<>(tmpId, triplet.getEdge().getValue().getSimilarity());
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
          minimumIdAndSim = null;
          break;
        }
      }
    }
    return minimumIdAndSim;
  }
}
