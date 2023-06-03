
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Weli
 */
public class StringTest {

public static void main(String[] args) throws IOException {
        String a = "{\"task\":{\"id\":\"1484990710152%umc-01%641\",\"redirectfrom\""
                + ":\"\",\"servername\":\"umc-01\",\"clientname\":\"umc-01\",\"from\""
                + ":\"file:///tmp/smb/test\",\"to\":\"hds:///TEST/t87\",\"state\":\"SUCCEED\""
                + ",\"progress\":1.0,\"startTime\":\"2017-01-21T17:25:10.152\",\"elapsed\":33,\""
                + "expectedsize\":6,\"transferredsize\":6}}";
        String b = "{\"dataInfo\":[{\"uri\":\"file:///tmp/test/Benchmark_LOG.csv\",\"location\":\"file\",\"name\":\"Benchmark_LOG.csv\",\"size\":0,\"ts\":\"2017-01-23T14:54:59.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test1\",\"location\":\"file\",\"name\":\"Test1\",\"size\":1048576,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test10\",\"location\":\"file\",\"name\":\"Test10\",\"size\":1048576,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test2\",\"location\":\"file\",\"name\":\"Test2\",\"size\":5242880,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test3\",\"location\":\"file\",\"name\":\"Test3\",\"size\":4194304,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test4\",\"location\":\"file\",\"name\":\"Test4\",\"size\":3145728,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test5\",\"location\":\"file\",\"name\":\"Test5\",\"size\":1048576,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test6\",\"location\":\"file\",\"name\":\"Test6\",\"size\":1048576,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test7\",\"location\":\"file\",\"name\":\"Test7\",\"size\":5242880,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test8\",\"location\":\"file\",\"name\":\"Test8\",\"size\":4194304,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]},{\"uri\":\"file:///tmp/test/Test9\",\"location\":\"file\",\"name\":\"Test9\",\"size\":9437184,\"ts\":\"2017-01-23T14:54:58.000\",\"type\":\"file\",\"dataowner\":[{\"host\":\"umc-01\",\"ratio\":1.0}]}]}";
        System.out.println(a.indexOf("\"id\":"));
        parseResponse(b);
        String c = "http://umc-01:8000/dataservice/v1/access?from=local%3A%2F%2F%2Ftmp%2FTest10&to=file%3A%2F%2F%2Ftmp%2Ftest%2F";
        String temp = c.substring(c.indexOf("local%3A%2F%2F") + 14, c.indexOf("&"));
        String t=temp.replaceAll("%2F","/");
        System.out.println(t);
//        writeCsv(resParse(a));
//        writeCsv(resParse(b));
//        a = a.replaceAll("\"", "");
//        a = a.replaceAll("}", "");
//        a.trim();
//        String[] aa = a.split(",");
//        String from = aa[4].substring(5);
//        String to = aa[5].substring(3);
//        String state = aa[6].substring(6);
//        String startTime = aa[8].substring(10);
//        String elapsed = aa[9].substring(8);
//        String size = aa[10].substring(0);
//        String transferredsize = aa[11].substring(16);
//        System.out.println(a);
//        System.out.println(from);
//        System.out.println(to);
//        System.out.println(state);
//        System.out.println(startTime);
//        System.out.println(elapsed);
////        System.out.println(size);
//        System.out.println(transferredsize);

    }
  
  public static String[] resParse(String res) {
        res.replaceAll("\"", "");
        res = res.replaceAll("}", "");
        String[] ret = new String[4];
        if (res.contains("SUCCEED")) {
            String[] results = res.split(",");
            String from = results[4].substring(5);
            String to = results[5].substring(3);
            String elapsed = results[9].substring(8);
            String transferredsize = results[11].substring(16);
            ret[0] = '"' + "from" + '"' + "," + from;
            ret[1] = '"' + "to" + '"' + "," + to;
            ret[2] = '"' + "elapsed" + '"' + "," + '"' + elapsed + '"';
            ret[3] = '"' + "transferredsize" + '"' + "," + '"' + transferredsize + '"';
            for (int i = 0; i < 4; i++) {
                ret[i] = ret[i].replaceAll("\":", "");
            }
        }
        return ret;
    }

  public static void writeCsv(String[] results) throws FileNotFoundException, IOException {

    try (FileOutputStream fos = new FileOutputStream("F:\\Benchmark_LOG.csv");
            OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
            BufferedWriter bw = new BufferedWriter(osw)) {
      for (String result : results) {
        bw.write(result);
        bw.flush();
        bw.newLine();
      }
    }
  }

  public static void parseResponse(String res) {
    while (res.contains("\"uri\":")) {
      int begin = res.indexOf("\"uri\":");
      int end = res.indexOf("\"location\":");
      String filePath = res.substring(begin + 6, end);
      String temp = res.substring(end + 10);
      res = temp;
      String file1 = filePath.replaceAll("\"", "");
      String file2 = file1.replaceAll(",", "");
      System.out.println(file2);
    }
  }
}
