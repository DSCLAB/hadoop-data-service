package com.dslab.drs.yarn.application.containermanager;

import com.dslab.drs.utils.DrsConfiguration;
import com.dslab.drs.utils.ReflectionUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class ResizeAlgo {

  private static final Map<Class<? extends ResizeAlgo>, ResizeAlgo> instances
          = new ConcurrentHashMap<>();

  private static final Log LOG = LogFactory.getLog(ResizeAlgo.class);
  private static final ContainerManagerImpl CMG = ContainerManagerImpl.getInstance();
  final Map<Integer, ResizeElement> elements = new ConcurrentHashMap<>();
  final Map<Integer, Long> totalRate = new ConcurrentHashMap<>();
  final Map<Integer, Long> result = new ConcurrentHashMap<>();//size-num
  final Map<Integer, Long> resetResult = new ConcurrentHashMap<>();//size-num
  final Map<Integer, Long> execute = new ConcurrentHashMap<>();//size-num
  private static Long LONGEST_INTERVAL_RUNTIME = 0L;
  private static double OBJ_FUNCTION = 0.0;

  int killC = 0;
  int reqC = 0;

  private final LogIntegrations logIntegration = new LogIntegrations();
  private final List<Integer> sizeList = new ArrayList<>();

  public static final ResizeAlgo DEFAULT_ALGO = getInstance(DefaultResizeAlgo.class);

  public static ResizeAlgo getInstance(Class<? extends ResizeAlgo> clazz) {
    ResizeAlgo algo = ReflectionUtils.newInstance(clazz);
    ResizeAlgo algoRet = instances.putIfAbsent(clazz, algo);
    if (algoRet != null) {
      return algoRet;
    }
    return algo;
  }

  public void run() {
    LOG.debug("Evaluate benefit.");
    logIntegration.reset();
    logIntegration.init();
    CMG.getContainerIntervals().stream()
            .forEach(e -> logIntegration.appendIntervalSize(e.getKey()));
    logIntegration.appendNowContainerNum(CMG.getIntervalsContainerNumLog());
    final String intervalSizeLog = logIntegration.getIntervalSizeLog();
    LOG.info("now container number: " + intervalSizeLog + " = " + logIntegration.getNowContainerNumLog());
    resetAll();
    killC = 0;
    reqC = 0;
    //do algo to get result  
    CMG.getContainerIntervals().stream()
            .forEach((entry) -> elements.put(entry.getKey(), new ResizeElement()
                    .setFileNum(entry.getValue().getFilesSize())
                    .setTaskTime(entry.getValue().getAVGTime())
                    .setTotalFileNum(entry.getValue().getFilesSize() + entry.getValue().getRunningFilesSize())));

    long shortTaskTimeTmp = 0L;
    for (Map.Entry<Integer, ResizeElement> e : elements.entrySet()) {
      sizeList.add(e.getKey());
      logIntegration.appendTaskAVGtime(e.getValue().getTaskTime());
      logIntegration.appendFileCount(e.getValue().getTotalFileNum());
      if (e.getValue().getTaskTime() > 0) {
        if (shortTaskTimeTmp == 0L) {
          shortTaskTimeTmp = e.getValue().getTaskTime();
        }
//        } else if (e.getValue().getTaskTime() < shortTaskTimeTmp) {
//          shortTaskTimeTmp = e.getValue().getTaskTime();
//        }
      }
    }
    LOG.info("file count: " + intervalSizeLog + " = " + logIntegration.getFileCount());
    LOG.info("task average runtime: " + intervalSizeLog + " = " + logIntegration.getTaskAVGtime());

    if (shortTaskTimeTmp == 0) {
      LOG.info("no interval has task time. Wait!");
      OBJ_FUNCTION = -1;
      return;
    }

    //prevent 0 avg time
    for (Map.Entry<Integer, ResizeElement> e : elements.entrySet()) {
      if (e.getValue().getTaskTime() == 0 && e.getValue().getTotalFileNum() > 0) {
        long properT = findProperTaskTime(e.getKey());
        LOG.debug("interval_" + e.getKey() + " .proper task time =" + properT);
        elements.put(e.getKey(), e.getValue().setTaskTime(properT));
      }
    }
    //get rate
    elements.entrySet().stream()
            .forEach(e -> totalRate.put(e.getKey(), e.getValue().getTotalFileNum() * e.getValue().getTaskTime() * e.getKey()));
    long sum = totalRate.entrySet().stream().mapToLong(e -> e.getValue()).sum();
    if (sum <= 0) {
      return;
    }
    List<Integer> decreaseList = new ArrayList<>();
    long decrease = 0;
//    LOG.debug("initial request total memory = " + sum);
    for (Map.Entry<Integer, Long> e : totalRate.entrySet()) {
      if ((e.getValue() * CMG.getTotalMem()) / (sum * e.getKey()) == 0) {
        decrease += e.getKey();
        decreaseList.add(e.getKey());
      } else {
        result.put(e.getKey(), (e.getValue() * CMG.getTotalMem()) / (sum * e.getKey()));
      }
    }
    //reverse
    Map<Integer, Long> m = reverseMap(totalRate, "MAX");
    if (decrease > 0) {
//      LOG.debug("Because container's result in some interval is 0.XXX, Re-evaluate!");
      for (Map.Entry<Integer, Long> e : m.entrySet()) {
        if (belongDecrease(decreaseList, e.getKey())) {
          resetResult.put(e.getKey(), 1L);
        } else if ((CMG.getTotalMem() - decrease - (e.getKey() * result.get(e.getKey()))) > 0) {
          resetResult.put(e.getKey(), result.get(e.getKey()));
          decrease += (e.getKey() * result.get(e.getKey()));
//          resetResult.put(e.getKey(), (e.getValue() * (CMG.getTotalMem() - decrease)) / (sum * e.getKey()));
        } else {
          resetResult.put(e.getKey(), (CMG.getTotalMem() - decrease) / e.getKey());
        }
      }
      Map<Integer, Long> resultRev = reverseMap(resetResult, "MIN");
      for (Map.Entry<Integer, Long> e : resultRev.entrySet()) {
        logIntegration.appendGoalContainerNum(e.getValue());
        execute.put(e.getKey(), e.getValue() - CMG.getContainerInterval(e.getKey()).getContainerNum());
      }
//      resultRev.entrySet().stream().forEach(e -> logIntegration.appendGoalContainerNum(e.getValue()));
//      resultRev.entrySet().stream().forEach(e -> execute.put(e.getKey(),
//              e.getValue() - CMG.getContainerInterval(e.getKey()).getContainerNum()));
    } else {
      result.entrySet().stream().forEach(e -> logIntegration.appendGoalContainerNum(e.getValue()));
      result.entrySet().stream().forEach(e -> execute.put(e.getKey(),
              e.getValue() - CMG.getContainerInterval(e.getKey()).getContainerNum()));
    }
    LOG.info("GOAL container: " + intervalSizeLog + " = " + logIntegration.getGoalContainerNumLog());
//    totalRate.entrySet().stream()
//            .forEach(e -> result.put(e.getKey(), (e.getValue() * CMG.getTotalMem()) / (sum * e.getKey())));

    for (Map.Entry<Integer, Long> e : execute.entrySet()) {
      if (e.getValue() > 0) {
        logIntegration.appendRequestContainerNum(e.getValue());
        logIntegration.appendKillContainerNum(0);
        reqC += e.getValue();
      } else if (e.getValue() < 0) {
        logIntegration.appendKillContainerNum(Math.abs(e.getValue()));
        logIntegration.appendRequestContainerNum(0);
        killC += e.getValue();
      } else {
        logIntegration.appendKillContainerNum(0);
        logIntegration.appendRequestContainerNum(0);
      }
    }
    LOG.info("request container: " + intervalSizeLog + " = " + logIntegration.getRequestContainerNumLog());
    LOG.info("kill container: " + intervalSizeLog + " = " + logIntegration.getKillContainerNumLog());

    //Longest_Intervals_time
    long tmp = -99;
    for (Map.Entry<Integer, ContainerInterval> e : CMG.getContainerIntervals()) {
      if (e.getValue().getLongestRunTime(shortTaskTimeTmp) == Integer.MAX_VALUE) {
        tmp = Integer.MAX_VALUE;
        break;
      }
      if (e.getValue().getLongestRunTime(shortTaskTimeTmp) > tmp) {
        tmp = e.getValue().getLongestRunTime(shortTaskTimeTmp);
      }
    }
    if (tmp == Integer.MAX_VALUE) {
      LOG.debug("do resize right away!");
    } else {
      LOG.debug("longest interval runtime = " + tmp);
    }
    LONGEST_INTERVAL_RUNTIME = tmp;
    if (tmp == -1 || tmp == Integer.MAX_VALUE) {
      OBJ_FUNCTION = 1;
      return;
    }

    //predict result time
    long predictTmp = 0L;
    if (decrease > 0) {
      for (Map.Entry<Integer, ContainerInterval> e : CMG.getContainerIntervals()) {
        if (e.getValue().getPredictRunTime(shortTaskTimeTmp, resetResult.get(e.getKey())) > predictTmp) {
          predictTmp = e.getValue().getPredictRunTime(shortTaskTimeTmp, resetResult.get(e.getKey()));
        }
      }
    } else {
      for (Map.Entry<Integer, ContainerInterval> e : CMG.getContainerIntervals()) {
        if (e.getValue().getPredictRunTime(shortTaskTimeTmp, result.get(e.getKey())) > predictTmp) {
          predictTmp = e.getValue().getPredictRunTime(shortTaskTimeTmp, result.get(e.getKey()));
        }
      }
    }
    LOG.debug("predict longest interval runtime = " + predictTmp);
    int isReq = 0;
    if (reqC > 0) {
      isReq = 1;
    }
    long COST = CMG.getRequestTime() * isReq + CMG.getReleaseTime();
    LOG.debug("request cost = " + COST);
    OBJ_FUNCTION = (double) (LONGEST_INTERVAL_RUNTIME - (COST + predictTmp)) / (double) LONGEST_INTERVAL_RUNTIME;
    LOG.info("Object function value = " + OBJ_FUNCTION);
  }

  private boolean belongDecrease(List<Integer> list, int size) {
    for (Integer i : list) {
      if (size == i) {
        return true;
      }
    }
    return false;
  }

  public long getLongestRuntime() {
    return LONGEST_INTERVAL_RUNTIME;
  }

  public boolean isExecute() {
    if (reqC == 0 && killC == 0) {
      return false;
    }
    double percent = CMG.getConf().getDouble(
            DrsConfiguration.DRS_CONTAINERMANAGER_OBJFUNCTION_VALUE,
            DrsConfiguration.DRS_CONTAINERMANAGER_OBJFUNCTION_VALUE_DEFAULT);
    LOG.debug("Object function you set = " + percent);
    return OBJ_FUNCTION > percent;
  }

  abstract void execute();

  private void resetAll() {
    execute.clear();
  }

  private static Map<Integer, Long> reverseMap(Map<Integer, Long> map, String policy) {
    List<Integer> list = new ArrayList<>();
    Map<Integer, Long> ret = new TreeMap<>();
    if ("MAX".equals(policy)) {
      ret = new TreeMap<>(new MapKeyComparatorMax());
    } else if ("MIN".equals(policy)) {
      ret = new TreeMap<>(new MapKeyComparatorMin());
    }
    for (Integer l : map.keySet()) {
      list.add(l);
    }
    Collections.reverse(list);
    for (int i = 0; i < list.size(); i++) {
      System.out.println(list.get(i));
      ret.put(list.get(i), map.get(list.get(i)));
    }
    return ret;
  }

  private static class MapKeyComparatorMax implements Comparator<Integer> {

    @Override
    public int compare(Integer int1, Integer int2) {
      if (int1 > int2) {
        return -1;
      } else if (int1 < int2) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  private static class MapKeyComparatorMin implements Comparator<Integer> {

    @Override
    public int compare(Integer int1, Integer int2) {
      if (int1 > int2) {
        return 1;
      } else if (int1 < int2) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  private long findProperTaskTime(int size) {
    int index = sizeList.indexOf(size);
    int max = sizeList.size() - 1;
    int mm = 0;
    long taskT = 0L;
    long next = 0L;
    long last = 0L;
    while (taskT == 0L) {
      if (index + mm <= max) {
        next = CMG.getContainerInterval(sizeList.get(index + mm)).getAVGTime();
      }
      if (index - mm >= 0) {
        last = CMG.getContainerInterval(sizeList.get(index - mm)).getAVGTime();
      }

      if (next != 0 && last != 0) {
        taskT = (next + last) / 2;
      } else if (next == 0 && last != 0) {
        taskT = last;
      } else if (next != 0 && last == 0) {
        taskT = next;
      }
      mm++;
    }
    return taskT;
  }

}
