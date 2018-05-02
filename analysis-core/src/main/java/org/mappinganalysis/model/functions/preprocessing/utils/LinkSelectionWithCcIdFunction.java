package org.mappinganalysis.model.functions.preprocessing.utils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Add links to result set, check if datasource is already contained in current cluster.
 * If contained, remove link.
 *
 * We also detect and handle indirect 1:n like a -> b -> c -> a
 *
 * TODO To reduce complexity to ccid groups, ccid is still used.
 * todo Test version with sort partition.
 */
public class LinkSelectionWithCcIdFunction
    implements GroupReduceFunction<EdgeSourceSimTuple, Tuple2<Long, Long>> {
  private static final Logger LOG = Logger.getLogger(LinkSelectionWithCcIdFunction.class);
  private HashMap<String, Integer> sourcesMap;

  public LinkSelectionWithCcIdFunction(List<String> sources) {
    this.sourcesMap = AbstractionUtils.getSourcesMap(sources);
  }

  @Override
  public void reduce(Iterable<EdgeSourceSimTuple> values,
                     Collector<Tuple2<Long, Long>> out) throws Exception {

    HashMap<Integer, HashSet<HashSet<Long>>> sourcesContainedEntitiesMap = Maps.newHashMap();
    HashMap<Long, Integer> entitySourceMap = Maps.newHashMap();

    for (EdgeSourceSimTuple edge : values) {
      // get accumulated (and updated) src/trg dataset values
      int srcDataSetInt;
      if (entitySourceMap.containsKey(edge.getSrcId())) {
        srcDataSetInt = entitySourceMap.get(edge.getSrcId());
      } else {
        srcDataSetInt = sourcesMap.get(edge.getSrcDataSource());
        entitySourceMap.put(edge.getSrcId(), srcDataSetInt);
      }

      int trgDataSetInt;
      if (entitySourceMap.containsKey(edge.getTrgId())) {
        trgDataSetInt = entitySourceMap.get(edge.getTrgId());
      } else {
        trgDataSetInt = sourcesMap.get(edge.getTrgDataSource());
        entitySourceMap.put(edge.getTrgId(), trgDataSetInt);
      }

      // get set of evolving clusters within cc for the src/trg dataset combination
      HashSet<HashSet<Long>> srcEntities = sourcesContainedEntitiesMap
          .getOrDefault(srcDataSetInt, Sets.newHashSet());
      HashSet<HashSet<Long>> trgEntities = sourcesContainedEntitiesMap
          .getOrDefault(trgDataSetInt, Sets.newHashSet());

      // get actual src/trg dataset combination for this edge
      HashSet<Long> srcSet = null;
      HashSet<Long> trgSet = null;
      if (srcEntities.isEmpty()) {
        srcSet = Sets.newHashSet(edge.getSrcId());
      } else {
        for (HashSet<Long> srcEntity : srcEntities) {
          if (srcEntity.contains(edge.getSrcId())) {
            srcSet = srcEntity;
          }
        }
        if (srcSet == null) {
          srcSet = Sets.newHashSet(edge.getSrcId());
        }
      }

      if (trgEntities.isEmpty()) {
        trgSet = Sets.newHashSet(edge.getTrgId());
      } else {
        for (HashSet<Long> trgEntity : trgEntities) {
          if (trgEntity.contains(edge.getTrgId())) {
            trgSet = trgEntity;
          }
        }
        if (trgSet == null) {
          trgSet = Sets.newHashSet(edge.getTrgId());
        }
      }

      if (AbstractionUtils.hasOverlap(srcDataSetInt, trgDataSetInt)) {
        // no merge
        if (srcDataSetInt == trgDataSetInt) {
          srcEntities.add(srcSet);
          srcEntities.add(trgSet);
        } else {
          srcEntities.add(srcSet);
          trgEntities.add(trgSet);
          sourcesContainedEntitiesMap.put(trgDataSetInt, trgEntities);
        }
        sourcesContainedEntitiesMap.put(srcDataSetInt, srcEntities);
//        LOG.info("edge not created: " + edge.toString());
      } else {
        int mergedDataSources = srcDataSetInt + trgDataSetInt;
        // update sources contained entity set

        // remove old ones and update
        srcEntities.remove(srcSet); // Optional?
        trgEntities.remove(trgSet);

        sourcesContainedEntitiesMap.put(srcDataSetInt, srcEntities);
        sourcesContainedEntitiesMap.put(trgDataSetInt, trgEntities);

        // merge and add to sourcesContainedEntities
        HashSet<Long> mergedEntities = Utils.merge(srcSet, trgSet);

        for (Long mergedEntity : mergedEntities) {
//          LOG.info("update entitySourceEntry: " + mergedEntity + " sources: " + mergedDataSources);
          entitySourceMap.put(mergedEntity, mergedDataSources);
        }

        HashSet<HashSet<Long>> allSetsThisDataSources
            = sourcesContainedEntitiesMap.getOrDefault(mergedDataSources, Sets.newHashSet());
        allSetsThisDataSources.add(mergedEntities);
        sourcesContainedEntitiesMap.put(mergedDataSources, allSetsThisDataSources);

        out.collect(new Tuple2<>(edge.getSrcId(), edge.getTrgId()));
      }
    }
  }
}
