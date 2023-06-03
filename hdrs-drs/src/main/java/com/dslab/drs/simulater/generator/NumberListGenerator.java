package com.dslab.drs.simulater.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
/**
 *
 * @author kh87313
 */
public final class NumberListGenerator extends Generator<List> {

  private int _lb, _ub, _interval;
  private final Random rand;
  private List<Integer> lastVal;
  private final UniformIntegerGenerator numberCountGernerator;
  private final UniformIntegerGenerator numberNumberGernerator;
  
  public NumberListGenerator(int lowerBound, int upperBound) {
    setNewBound(lowerBound, upperBound);
    numberCountGernerator = new UniformIntegerGenerator(lowerBound, upperBound);
    numberNumberGernerator = new UniformIntegerGenerator(1, upperBound);
    rand = new Random();
  }

  public void setNewBound(int lowerBound, int upperBound) {
    _lb = lowerBound;
    _ub = upperBound;
    _interval = _ub - _lb + 1;
  }

  @Override
  public List<Integer> nextValue() {
    int selcted[] = new int[_ub + 1];

    List<Integer> filesList = new ArrayList<>();
    int filesCount = numberCountGernerator.nextValue();
    for (int i = 0; i < filesCount; i++) {
      int fileNumber = numberNumberGernerator.nextValue();
      while (true) {
        if (selcted[fileNumber] == 1) {
          fileNumber++;
          if (fileNumber > _ub) {
            fileNumber = 1;
          }
        } else {
          selcted[fileNumber] = 1;
          filesList.add(fileNumber);
          break;
        }
      }
    }

    setLastValue(filesList);
    return filesList;
  }

  @Override
  public List<Integer> lastValue() {
    return lastVal;
  }

  private void setLastValue(List<Integer> last) {
    lastVal = last;
  }
}
