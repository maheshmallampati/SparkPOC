import java.io.*;
import java.net.*;
import java.util.*;

public class CurlCommand {
	
	public static void main(String[] args) throws Exception {
		ProcessBuilder pb = new ProcessBuilder(
                "curl",
                "-XPOST",
                "curl https://login.xyz.com/v1/oauth/token -H \"Accept: application/json\" --data 'client_id=<clientId>' --data 'client_secret=<clientSecret>' --data 'redirect_uri=localhost' --data 'code=<code>' ");
		try {
pb.redirectErrorStream(true);
Process p = pb.start();

InputStream is = p.getInputStream();

InputStream errorStream = p.getErrorStream();
BufferedInputStream bis = new BufferedInputStream(is);
BufferedReader br = new BufferedReader(new InputStreamReader(errorStream));
String strLine = "";
while ((strLine = br.readLine()) != null) {
    System.out.println(strLine);
}
br.close();
}
catch (Exception e) {
e.printStackTrace();
}
}
}
