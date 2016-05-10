package org.mappinganalysis.model.functions.refinement;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Triplet;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

import java.util.Set;

public class CollectExcludeTuplesGroupReduceFunction
    implements GroupReduceFunction<Triplet<Long, ObjectMap, ObjectMap>, Tuple4<Long, Long, Long, Double>> {
  private static final Logger LOG = Logger.getLogger(CollectExcludeTuplesGroupReduceFunction.class);

  private final int column;

  public CollectExcludeTuplesGroupReduceFunction(int column) {
    this.column = column;
  }

  @Override
  public void reduce(Iterable<Triplet<Long, ObjectMap, ObjectMap>> triplets,
                     Collector<Tuple4<Long, Long, Long, Double>> collector) throws Exception {
    Set<Tuple2<Long, Long>> tripletIds = Sets.newHashSet();

    Tuple2<Long, Double> clusterRefineId = getLowestVertexIdAndSimilarity(triplets, tripletIds, column);
    if (clusterRefineId == null) {
//    LOG.info("Exclude all case" + tripletIds.toString());
      for (Tuple2<Long, Long> tripletId : tripletIds) {
        LOG.info("CollectExcludeTuplesGroupReduceFunction refineId null: " + tripletId.toString());
        collector.collect(new Tuple4<>(tripletId.f0, tripletId.f1, Long.MIN_VALUE, 0D));
      }
    } else {
//    LOG.info("Exclude none + enrich" + tripletIds.toString());
      for (Tuple2<Long, Long> tripletId : tripletIds) {
        LOG.info("CollectExcludeTuplesGroupReduceFunction: refineId not null" + tripletId.toString() + " refine: "
        + clusterRefineId.toString());

        collector.collect(new Tuple4<>(tripletId.f0, tripletId.f1, clusterRefineId.f0, clusterRefineId.f1));
      }
    }
  }

  /**
   * quick'n'dirty fix haha later todo
   */
  private Tuple2<Long, Double> getLowestVertexIdAndSimilarity(Iterable<Triplet<Long, ObjectMap, ObjectMap>> triplets,
                                                              Set<Tuple2<Long, Long>> tripletIds, int column) {
    Set<String> ontologies = Sets.newHashSet();
    Tuple2<Long, Double> minimumIdAndSim = null;

    for (Triplet<Long, ObjectMap, ObjectMap> triplet : triplets) {
      Long srcId = triplet.getSrcVertex().getId();
      Long trgId = triplet.getTrgVertex().getId();
      tripletIds.add(new Tuple2<>(srcId, trgId));
      Long tmpId = srcId < trgId ? srcId : trgId;
      Double similarity = triplet.getEdge().getValue().getSimilarity();

      if (minimumIdAndSim == null) {
        minimumIdAndSim = new Tuple2<>(tmpId, similarity);
      }
      if (minimumIdAndSim.f0 > tmpId) {
        minimumIdAndSim.f0 = tmpId;
      }
      if (minimumIdAndSim.f1 < similarity) {
        minimumIdAndSim.f1 = similarity;
      }

      Set<String> tripletOnts;
      if (column == 1) {
        tripletOnts = triplet.getTrgVertex().getValue().getOntologiesList();
      } else {
        tripletOnts = triplet.getSrcVertex().getValue().getOntologiesList();
      }

      for (String ont : tripletOnts) {
        if (!ontologies.add(ont)) {
//        LOG.info("problem duplicate ontology triplet: " + triplet);
          minimumIdAndSim = null;
          break;
        }
      }
    }
    return minimumIdAndSim;
  }
}
