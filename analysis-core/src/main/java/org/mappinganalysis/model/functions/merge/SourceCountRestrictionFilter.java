package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;

/**
 * Check cluster for contained element count, if lower than max source count, return true.
 */
class SourceCountRestrictionFilter implements FilterFunction<MergeTuple> {
  private int sourcesCount;

  public SourceCountRestrictionFilter(int sourcesCount) {
    this.sourcesCount = sourcesCount;
  }

  /**
   * Check cluster for contained element count and restrict if >= sourcesCount.
   * @param tuple MergeTuple to check
   * @return if contained element count lower than max source count, return true.
   * @throws Exception
   */
  @Override
  public boolean filter(MergeTuple tuple) throws Exception {
    return AbstractionUtils.getSourceCount(tuple.getIntSources()) < sourcesCount;
  }
}