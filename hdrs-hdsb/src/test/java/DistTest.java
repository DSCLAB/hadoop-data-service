
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Weli
 */
public class DistTest {

  public static void main(String[] args) throws InterruptedException {
//        NormalDistribution normal = new NormalDistribution(50, 10);
//        double n = normal.sample();
//        System.out.println(n);
//        UniformIntegerDistribution uni = new UniformIntegerDistribution(1, 100);
//        int u = uni.sample();
//        System.out.println(u);
//      ZipfDistribution z = new ZipfDistribution(10, 5);
//      System.out.println(z.sample());
    long s = System.currentTimeMillis();
    Object o = new Object();
    synchronized (o) {
      o.wait(10);
    }
    System.out.println("elapse = " + (System.currentTimeMillis() - s));

    for (int i = 0; i < 100; i++) {
      ZipfDistribution z = new ZipfDistribution(100, 2);
//      System.out.println(z.sample());
    }
  }
}
