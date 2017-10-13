package org.mappinganalysis.util;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class HungarianAlgorithmTest {

  /**
   * Careful: Negate similarities to simulate "weights".
   */
  @Test
  public void hungarianExecuteTest() throws Exception {
    double[][] matrix = new double[4][4];
    matrix[0][0] = 0.1;
    matrix[0][1] = 0.3;
    matrix[0][2] = 0.2;
    matrix[0][3] = 0.4;

    matrix[1][0] = 0.3;
    matrix[1][1] = 0.4;
    matrix[1][2] = 0.1;
    matrix[1][3] = 0.2;

    matrix[2][0] = 0.4;
    matrix[2][1] = 0.2;
    matrix[2][2] = 0.3;
    matrix[2][3] = 0.1;

    matrix[3][0] = 0.3;
    matrix[3][1] = 0.2;
    matrix[3][2] = 0.1;
    matrix[3][3] = 0.4;

    /*
      result reports the combination of resulting elements
      left source (result[x]) and right source (resulting value)
     */
    HungarianAlgorithm algorithm = new HungarianAlgorithm(matrix);

    int[] execute = algorithm.execute();

    assertTrue(execute[0] == 0);
    assertTrue(execute[1] == 2);
    assertTrue(execute[2] == 3);
    assertTrue(execute[3] == 1);
  }

  @Test
  public void hungarianSmallExecuteTest() throws Exception {
    double[][] matrix = new double[1][1];
    matrix[0][0] = 0.1;

    HungarianAlgorithm algorithm = new HungarianAlgorithm(matrix);
    int[] execute = algorithm.execute();

    assertTrue(execute[0] == 0);
  }
}