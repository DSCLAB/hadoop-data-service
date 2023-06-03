
import java.io.File;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Weli
 */
public class FileListTest {

    public static void main(String[] args) {
        File f = new File("F:\\");
        String filePaht = f.getAbsolutePath();

        for (int i = 0; i < f.listFiles().length; i++) {
            File ff = new File(filePaht + f.list()[i]);
            if (!ff.isDirectory()) {
                System.out.println(f.list()[i]);
            }
        }
    }

}
