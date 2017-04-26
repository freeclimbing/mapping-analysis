package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple6;
import org.mappinganalysis.io.impl.DataDomain;

/**
 * srcId, trgId, srcTuple, trgTuple, sim, blocking label
 */
public class MergeTriplet<T>
    extends Tuple6<Long, Long, T, T, Double, String> {
  public MergeTriplet() {
  }

  public MergeTriplet(T srcTuple, T trgTuple, Double similarity) {
    this.f2 = srcTuple;
    this.f3 = trgTuple;
    this.f4 = similarity;
  }

  public void setIdAndTuples(T leftIn, T rightIn, DataDomain domain) {
    if (domain == DataDomain.GEOGRAPHY) {
      MergeGeoTuple left = (MergeGeoTuple) leftIn;
      MergeGeoTuple right = (MergeGeoTuple) rightIn;

      if (left.getId() < right.getId()) {
        setSrcId(left.getId());
        setSrcTuple((T) left);
        setTrgId(right.getId());
        setTrgTuple((T) right);
      } else {
        setTrgId(left.getId());
        setTrgTuple((T) left);
        setSrcId(right.getId());
        setSrcTuple((T) right);
      }
    } else {
      MergeMusicTuple left = (MergeMusicTuple) leftIn;
      MergeMusicTuple right = (MergeMusicTuple) rightIn;

      if (left.getId() < right.getId()) {
        setSrcId(left.getId());
        setSrcTuple((T) left);
        setTrgId(right.getId());
        setTrgTuple((T) right);
      } else {
        setTrgId(left.getId());
        setTrgTuple((T) left);
        setSrcId(right.getId());
        setSrcTuple((T) right);
      }
    }
}

  public Long getSrcId() {
    return f0;
  }

  public void setSrcId(Long id) {
    f0 = id;
  }

  public Long getTrgId() {
    return f1;
  }

  public void setTrgId(Long id) {
    f1 = id;
  }

  public T getSrcTuple() {
    return f2;
  }

  public void setSrcTuple(T src) {
    f2 = src;
  }

  public T getTrgTuple() {
    return f3;
  }

  public void setTrgTuple(T trg) {
    f3 = trg;
  }

  public Double getSimilarity() {
    return f4;
  }

  public void setSimilarity(Double similarity) {
    f4 = similarity;
  }

  public String getBlockingLabel() {
    return f5;
  }

  public void setBlockingLabel(String label) {
    f5 = label;
  }
}
