package org.mappinganalysis.model;

import org.junit.Test;
import org.apache.log4j.Logger;

import static org.junit.Assert.*;

public class MergeTripletTest {
  private static final Logger LOG = Logger.getLogger(MergeTripletTest.class);


  @Test
  public void testGetLatitude() throws Exception {
    MergeTriplet triplet = new MergeTriplet();

    if (triplet.getLatitude() != null) {
      LOG.info(triplet.getLatitude());
    } else {
      LOG.info("working");
    }

  }

  @Test
  public void testSetLatitude() throws Exception {

  }
}