package com.dslab.drs.monitor.tool;

import java.util.Scanner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

/**
 *
 * @author kh87313
 */
public class MonitorTool {

  private static final Log LOG = LogFactory.getLog(MonitorTool.class);

  public static void main(final String[] args) {
    try {
      String currentDir = System.getProperty("user.dir");
      System.out.println();
      if (args.length != 1) {
        System.out.println("command need one args: 1)confFile");
        return;
      }
      MonitorConfManager monitorConfManager = new MonitorConfManager(args[0]);
      Logger.getLogger("com.dslab.drs").setLevel(monitorConfManager.getLogLever());

      MonitorCommandManager commandManager = new MonitorCommandManagerImpl(monitorConfManager);

      Scanner scanner = new Scanner(System.in);

      while (true) {
        System.out.println("==================");
        System.out.println("What do you want to do? Please choose one: 1.print running jonbs 2.print job status  0.quit");
        String yourChoose = scanner.next();
        System.out.println("==================");
        if ("quit".equals(yourChoose) || "0".equals(yourChoose)) {
          commandManager.quit();
          return;
        } else if ("1".equals(yourChoose)) {
          commandManager.printRunningJobs();
        } else if ("2".equals(yourChoose)) {
          System.out.println("Input job application ID...");
          String jobApplicationId = scanner.next();
          System.out.println("------------------");
          commandManager.printJobStatus(jobApplicationId);
        }

      }
      
      
    } catch (Exception ex) {
      ex.printStackTrace();
      System.out.println("fail:"+ex.getMessage());
    }
  }

}
