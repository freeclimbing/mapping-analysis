package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

public class MergeMapFunction
    implements MapFunction<MergeTriplet, MergeTuple> {
  private static final Logger LOG = Logger.getLogger(MergeMapFunction.class);
  MergeTuple reuseTuple;

  public MergeMapFunction() {
    reuseTuple = new MergeTuple();
  }

  @Override
  public MergeTuple map(MergeTriplet triplet) throws Exception {
    // prepare
    MergeTuple priority = triplet.getSrcTuple();
    MergeTuple minor = triplet.getTrgTuple();

    Set<Long> trgElements = minor.getClusteredElements();
    Set<Long> srcElements = priority.getClusteredElements();
    if (srcElements.size() < trgElements.size()) {
      MergeTuple tmp = minor;
      minor = priority;
      priority = tmp;
    }

    // set tuple properties
    reuseTuple.setId(priority.getId() > minor.getId() ? priority.getId() : minor.getId());
    // is there a case where minor label should be taken?
    reuseTuple.setLabel(priority.getLabel());

    // geo coordinates
    MergeTuple geoTuple = Utils.isOnlyOneValidGeoObject(priority, minor);
    if (geoTuple != null) {
      reuseTuple.setGeoProperties(geoTuple);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.GN_NS)) {
      reuseTuple.setGeoProperties(priority);
    } else if (AbstractionUtils.containsSrc(minor.getIntSources(), Constants.GN_NS)) {
      reuseTuple.setGeoProperties(minor);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.DBP_NS)) {
      reuseTuple.setGeoProperties(priority);
    } else if (AbstractionUtils.containsSrc(priority.getIntSources(), Constants.DBP_NS)) {
      reuseTuple.setGeoProperties(minor);
    }

    srcElements.addAll(trgElements);
    reuseTuple.addClusteredElements(srcElements);
    reuseTuple.setIntSources(AbstractionUtils.mergeIntValues(
        priority.getIntSources(),
        minor.getIntSources()));
    reuseTuple.setIntTypes(AbstractionUtils.mergeIntValues(
        priority.getIntTypes(),
        minor.getIntTypes()));
    reuseTuple.setBlockingLabel(priority.getBlockingLabel());

    LOG.info("############new cluster: " + reuseTuple.toString());
    return reuseTuple;
  }
}
