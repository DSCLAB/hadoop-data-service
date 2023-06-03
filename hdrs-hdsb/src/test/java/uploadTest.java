
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Weli
 */
public class uploadTest {

    public static void main(String[] args) throws IOException {
        try {
            String uri = "http://192.168.103.29:8000/dataservice/v1/access?from=" + encode("local://F:\\") + "&to=" + encode("hds:///TEST/weli");
            URL url = new URL(uri);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setChunkedStreamingMode(1024 * 1024);
            conn.setUseCaches(false);
            conn.setRequestProperty("User-Agent", "CodeJava Agent");
            conn.setRequestProperty("Content-Type", "application/octet-stream;");
            conn.setRequestProperty("Connection", "Keep-Alive");
            conn.setRequestProperty("Content-length", String.valueOf(new File("F:\\123.property").length()));
            conn.connect();

            try (FileInputStream fin = new FileInputStream("F:\\123.property"); OutputStream stream = conn.getOutputStream()) {
                byte[] buffer = new byte[10240];
                int idx = 0;
                while ((idx = fin.read(buffer)) != -1) {
                    stream.write(buffer, 0, idx);
                    stream.flush();
                }
            }

            int responseCode = conn.getResponseCode();
            System.out.println("response Code :" + responseCode);

            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            //print result
            System.out.println(response.toString());

        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static String encode(String url) {
        try {
            return URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }
}
