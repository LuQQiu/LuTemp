package alluxio;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ThreadLocalRandom;

public class RandomReadTest {
  public static void test(String localPath, String fusePath, long fileSize, long bufferSize, int loop) throws IOException {
    if (bufferSize > Integer.MAX_VALUE) {
      throw new IOException("Cannot handle buffer size bigger than Integer.MAX_VALUE");
    }
    byte[] buffer = new byte[(int) bufferSize];
    ThreadLocalRandom localRandom = ThreadLocalRandom.current();
    try (RandomAccessFile localInStream = new RandomAccessFile(localPath, "r");
        RandomAccessFile fuseInStream = new RandomAccessFile(fusePath, "r")) {
      for (int i = 0; i < loop; i++) {
        long offset = localRandom.nextLong(fileSize);
        fuseInStream.seek(offset);
        int bytesRead = fuseInStream.read(buffer);
        if (bytesRead == -1) {
          throw new IOException(String.format("Read to the end with offset %s and file size %s", offset, fileSize));
        }
        ReadMain.validateDataCorrectness(localInStream, buffer, offset, bytesRead);
      }
    }
  }
}
