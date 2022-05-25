package alluxio;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class ReadMain {
  public static final long SEED = 2312;
  public static final long KB = 1024;
  public static final long MB = 1024 * KB;
  public static final long GB = 1024 * MB;
  public static final int BUFFER_LEN = 1024;
  public static final Random RANDOM = new Random();

  public static final long[] FILE_SIZES = {100 * KB, MB, 63 * MB, 65 * MB, GB, 10 * GB};
  public static final long[] READ_BUFFER_SIZES = {128, 1000, 1001, MB, 1025, 4 * KB, 32 * KB, 128 * KB, MB, 4 * MB};
  public static final int LOOP_TIMES = 5;

  public static void main(String[] args) throws IOException {
    String localFolder = "/Users/alluxio/downloads/tempFuseTest";
    String fuseFolder = "/Users/alluxio/alluxioFolder/alluxio/standaloneFuse";
    runSingleThreadFullTest(localFolder, fuseFolder, true);
    System.out.println("Test passed");
  }
  
  public static void runSingleThreadFullTest(String localFolder, String fuseFolder, boolean random) throws IOException {
    for (long fileSize : FILE_SIZES) {
      String fileName = getRandomFileName("FuseTest", SEED);
      Path localPath = Paths.get(localFolder, fileName);
      Path fusePath = Paths.get(fuseFolder, fileName);
      createFileWithLen(localPath.toString(), fileSize, SEED);
      Files.copy(localPath, fusePath);
      try {
        for (long bufferSize : READ_BUFFER_SIZES) {
          if (bufferSize > fileSize) {
            break;
          }
          if (random) {
            RandomReadTest.test(localPath.toString(), fusePath.toString(), fileSize, bufferSize, LOOP_TIMES);
          } else {
            SequentialReadTest.test(localPath.toString(), fusePath.toString(), fileSize, bufferSize, LOOP_TIMES);
          }
        }
      } finally {
        Files.delete(localPath);
        Files.delete(fusePath); 
      }
    }
  }

  public static void validateDataCorrectness(RandomAccessFile localInStream, byte[] data, long offset, int bytesRead) throws IOException {
    byte[] localBuffer = new byte[bytesRead];
    localInStream.seek(offset);
    int localOffset = 0;
    while (localOffset < bytesRead) {
      int localBytesRead = localInStream.read(localBuffer, localOffset, bytesRead - localOffset);
      if (localBytesRead == -1) {
        throw new IOException(String.format("Cannot read %s bytes of offset %s from local file", bytesRead, offset));
      }
      localOffset += localBytesRead;
    }
    for (int i = 0; i < bytesRead; i++) {
      if (localBuffer[i] != data[i]) {
        throw new IOException("Data read from fuse is different from data read from local");
      }
    }
  }

  public static void createFileWithLen(String filePath, long len, long seed) throws IOException {
    RANDOM.setSeed(seed);
    FileOutputStream stream = new FileOutputStream(filePath);
    int bufferLen = (int) Math.min(BUFFER_LEN, len);
    byte[] buffer = new byte[bufferLen];
    while (len > 0) {
      RANDOM.nextBytes(buffer);
      int sizeToWrite = (int) Math.min(bufferLen, len);
      stream.write(buffer, 0, sizeToWrite);
      len -= sizeToWrite;
    }
  }

  public static String getRandomFileName(String prefix, long seed) throws IOException {
    RANDOM.setSeed(seed);
    return prefix + RANDOM.nextLong() + System.currentTimeMillis();
  }
}
