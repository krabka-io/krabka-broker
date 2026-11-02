import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Loopback fault fixture: hold the first actual DLQ Produce response after backend append. */
public class WireResponseGate {
  record Request(short api, short version, String client) {}
  static final AtomicBoolean held=new AtomicBoolean();
  static byte[] frame(DataInputStream in) throws IOException {
    int size=in.readInt();if(size<4||size>100*1024*1024)throw new IOException("invalid frame "+size);
    byte[] data=in.readNBytes(size);if(data.length!=size)throw new EOFException();return data;
  }
  static synchronized void send(DataOutputStream out, byte[] data) throws IOException {
    out.writeInt(data.length);out.write(data);out.flush();
  }
  static void bridge(Socket client,int backendPort,Path gate) {
    try(Socket backend=new Socket(InetAddress.getLoopbackAddress(),backendPort)) {
      var requests=new ConcurrentHashMap<Integer,Request>();
      var upstream=Thread.startVirtualThread(()->{
        try {
          var in=new DataInputStream(client.getInputStream());var out=new DataOutputStream(backend.getOutputStream());
          while(true) {
            byte[] data=frame(in);var buf=ByteBuffer.wrap(data);
            short api=buf.getShort(),version=buf.getShort();int correlation=buf.getInt();short len=buf.getShort();
            byte[] id=new byte[Math.max(0,len)];buf.get(id);
            requests.put(correlation,new Request(api,version,new String(id,StandardCharsets.UTF_8)));
            send(out,data);
          }
        } catch(Exception ignored) {} finally {try{client.close();backend.close();}catch(IOException ignored){}}
      });
      try {
        var in=new DataInputStream(backend.getInputStream());var out=new DataOutputStream(client.getOutputStream());
        while(true) {
          byte[] data=frame(in);int correlation=ByteBuffer.wrap(data).getInt();Request request=requests.remove(correlation);
          if(request!=null && request.api()==0 && request.client().toLowerCase(Locale.ROOT).contains("dlq") && held.compareAndSet(false,true)) {
            String digest=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
            String event=String.format("client=%s api=%d version=%d correlation=%d response_bytes=%d response_sha256=%s held_ms=%d%n",request.client(),request.api(),request.version(),correlation,data.length,digest,System.currentTimeMillis());
            Files.writeString(gate.resolve("held.txt"),event);System.out.print(event);System.out.flush();
            while(!client.isClosed() && !Files.exists(gate.resolve("release")))Thread.sleep(10);
          }
          if(client.isClosed())break;
          send(out,data);
        }
      } catch(Exception ignored) {} finally {try{client.close();backend.close();}catch(IOException ignored){}upstream.interrupt();}
    } catch(IOException ex) {try{client.close();}catch(IOException ignored){}}
  }
  public static void main(String[] args) throws Exception {
    int listen=Integer.parseInt(args[0]),backend=Integer.parseInt(args[1]);Path gate=Path.of(args[2]);Files.createDirectories(gate);
    try(ServerSocket server=new ServerSocket()) {
      server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(),listen));
      System.out.printf("READY listen=%d backend=%d%n",listen,backend);System.out.flush();
      while(true) {Socket client=server.accept();Thread.startVirtualThread(()->bridge(client,backend,gate));}
    }
  }
}
