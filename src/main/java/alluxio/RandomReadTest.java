package alluxio;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomReadTest {
  public static void testSingleBuffer(String localPath, String fusePath, long fileSize, long bufferSize, int iteration) throws IOException {
    if (bufferSize > Integer.MAX_VALUE) {
      throw new IOException("Cannot handle buffer size bigger than Integer.MAX_VALUE");
    }
    byte[] buffer = new byte[(int) bufferSize];
    ThreadLocalRandom localRandom = ThreadLocalRandom.current();
    try (RandomAccessFile localInStream = new RandomAccessFile(localPath, "r");
        RandomAccessFile fuseInStream = new RandomAccessFile(fusePath, "r")) {
      for (int i = 0; i < iteration; i++) {
        randomRead(localInStream, fuseInStream, buffer, localRandom, fileSize);
      }
    }
    System.out.printf("Finished test of file size %s%n", fileSize);
  }

  public static void testAllBuffer(String localPath, String fusePath, long fileSize, long[] bufferSizes, int iteration, long endTime) throws IOException {
    List<byte[]> buffers = new ArrayList<>();
    for (long bufferSize : bufferSizes) {
      if (bufferSize > Integer.MAX_VALUE) {
        throw new IOException("Cannot handle buffer size bigger than Integer.MAX_VALUE");
      }
      buffers.add(new byte[(int) bufferSize]);
    }
    ThreadLocalRandom localRandom = ThreadLocalRandom.current();
    try (RandomAccessFile localInStream = new RandomAccessFile(localPath, "r");
         RandomAccessFile fuseInStream = new RandomAccessFile(fusePath, "r")) {
      boolean firstRound = true;
      while (firstRound || System.currentTimeMillis() < endTime) {
        for (int i = 0; i < iteration; i++) {
          for (byte[] buffer : buffers) {
            randomRead(localInStream, fuseInStream, buffer, localRandom, fileSize);
          }
          System.out.printf("Finished iteration %s of file size %s%n", iteration, fileSize);
        }
        firstRound = false;
      }
    }
  }
  
  private static void randomRead(RandomAccessFile localInStream, RandomAccessFile fuseInStream, byte[] buffer, ThreadLocalRandom random, long fileSize) throws IOException {
    long offset = random.nextLong(fileSize);
    fuseInStream.seek(offset);
    int bytesRead = fuseInStream.read(buffer);
    if (bytesRead == -1) {
      throw new IOException(String.format("Read to the end with offset %s and file size %s", offset, fileSize));
    }
    ReadMain.validateDataCorrectness(localInStream, buffer, offset, bytesRead);
  }
}
