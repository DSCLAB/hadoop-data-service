package com.dslab.drs.simulater;

import java.util.Scanner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

/**
 *
 * @author Weli,kh87313
 */
public class Main {

  private static final Log LOG = LogFactory.getLog(Main.class);

  public static void main(final String[] args) throws Exception {
//org.apache.log4j.BasicConfigurator.configure();
//LOG.isDebugEnabled();

    String currentDir = System.getProperty("user.dir");
    System.out.println();
    if (args.length != 2) {
      System.out.println("command need two args: 1)xml , 2)workload");
      return;
    }
    SimulatorConfManager confManager = new SimulatorConfManager(args[0], args[1]);
    Logger.getLogger("com.dslab.drs").setLevel(confManager.getLogLever());

    LOG.debug("debug Main");
    LOG.info("info Main");
    LOG.error("error Main");
    SimulatorThreadManager threadManager = new SimulatorThreadManagerImpl();
    confManager.checkConf();

// 確認R檔案 csv檔
//    DrsConfiguration simulatorConf = confManager.getSimulaterConf();
//    Workload simulatorWorkload = confManager.getSimulaterWorkload();
//
//    //印出有設定的參數
//    simulatorWorkload.printWorkload();
    SimulatorCommandManager commandManager = new SimulatorCommandManagerImpl(confManager, threadManager);

    //upload yarn
    Scanner scanner = new Scanner(System.in);

    while (true) {
      System.out.println("What do you want to do? Please choose one: 1.createFiles 2.start 3.stop 4.status 5.addOne  0.quit");
      String yourChoose = scanner.next();
      if ("quit".equals(yourChoose) || "0".equals(yourChoose)) {
        commandManager.stop();
        threadManager.closeAll();
        return;
      } else if ("createFiles".equals(yourChoose) || "1".equals(yourChoose)) {
        commandManager.createFiles();
      } else if ("start".equals(yourChoose) || "2".equals(yourChoose)) {
        commandManager.start();
      } else if ("stop".equals(yourChoose) || "3".equals(yourChoose)) {
        commandManager.stop();
      } else if ("status".equals(yourChoose) || "4".equals(yourChoose)) {
        commandManager.printStatus();
      } else if ("addOne".equals(yourChoose) || "5".equals(yourChoose)) {
        commandManager.addOne();
      }
    }


    //關閉所有Thread
  }
}
