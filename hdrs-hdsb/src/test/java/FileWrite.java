
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.math3.distribution.ZipfDistribution;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Weli
 */
public class FileWrite {

  public static void main(String[] args) throws IOException {
    for (int i = 1; i < 100001; i++) {
      ZipfDistribution z = new ZipfDistribution(10000, 4);
      int size = z.sample() + 10;
      String ID = "F:\\localFile\\Test" + i;
      try (FileOutputStream fos = new FileOutputStream(ID)) {
        int byteSize = size * 1024;
        byte[] buffer = new byte[byteSize];
        fos.write(buffer);
        fos.close();
      } catch (IOException e) {
        System.out.println(e);
      }
    }
  }

}
