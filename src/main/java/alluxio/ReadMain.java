package alluxio;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;

public final class ReadMain {
  public static final long KB = 1024;
  public static final long MB = 1024 * KB;
  public static final long GB = 1024 * MB;
  public static final int BUFFER_LEN = 1024;
  public static final Random RANDOM = new Random();

  // public static final long[] FILE_SIZES = {100 * KB, MB, 1034, 63 * MB, 65 * MB, GB, 10 * GB};
  public static final long[] FILE_SIZES = {63 * MB, 65 * MB, GB, 10 * GB};
  public static final long[] READ_BUFFER_SIZES = {4 * MB, MB, 128 * KB, 32 * KB, 4 * KB, 1025, MB, 1001, 1000, 128};

  private static final CommandLineParser PARSER = new DefaultParser();
  private static final String LOCAL_FOLDER_OPTION_NAME = "l";
  private static final String FUSE_FOLDER_OPTION_NAME = "f";
  private static final String THREAD_NUMBER_OPTION_NAME = "t";
  private static final String ITERATION_OPTION_NAME = "i";
  private static final String DURATION_OPTION_NAME = "d";
  private static final String RANDOM_READ_OPTIONS_NAME = "r";
  private static final String SAME_DATASET_OPTION_NAME = "s";
  private static final String FILE_SIZE_OPTION_NAME = "fs";
  private static final String HELP_OPTION_NAME = "h";
  
  private static final Option LOCAL_FOLDER_OPTION = Option.builder(LOCAL_FOLDER_OPTION_NAME)
      .hasArg()
      .required(true)
      .longOpt("local-folder")
      .desc("Local folder to write source file to and validate data correctness with.")
      .build();
  private static final Option FUSE_FOLDER_OPTION
      = Option.builder(FUSE_FOLDER_OPTION_NAME)
      .hasArg()
      .required(true)
      .longOpt("fuse-folder")
      .desc("The Fuse folder to validate read correctness with")
      .build();
  private static final Option THREAD_NUMBER_OPTION = Option.builder(THREAD_NUMBER_OPTION_NAME)
      .required(true)
      .hasArgs()
      .longOpt("thread-number")
      .desc("Number of threads to execute the read testing")
      .build();
  private static final Option ITERATION_OPTION = Option.builder(ITERATION_OPTION_NAME)
      .required(true)
      .hasArgs()
      .longOpt("iteration")
      .desc("Number of iterations to execute the read testing")
      .build();
  private static final Option DURATION_OPTION = Option.builder(DURATION_OPTION_NAME)
      .required(false)
      .hasArgs()
      .longOpt("duration")
      .desc("Duration in minutes, the longer one of iteration or duration wins")
      .build();
  private static final Option RANDOM_READ_OPTION = Option.builder(RANDOM_READ_OPTIONS_NAME)
      .required(false)
      .longOpt("random-read")
      .desc("true to execute random read, false to execute sequential read testing")
      .build();
  private static final Option SAME_DATASET_OPTION = Option.builder(SAME_DATASET_OPTION_NAME)
      .required(false)
      .longOpt("same-dataset")
      .desc("true to read the same dataset, false to create a unique file for each thread")
      .build();
  private static final Option FILE_SIZE_OPTION = Option.builder(FILE_SIZE_OPTION_NAME)
      .required(false)
      .hasArgs()
      .longOpt("file_size")
      .desc("If provided, all threads read the same file with the given file size")
      .build();
  private static final Option HELP_OPTION = Option.builder(HELP_OPTION_NAME)
      .required(false)
      .desc("Print this help message")
      .build();
  
  private static final Options OPTIONS = new Options()
      .addOption(LOCAL_FOLDER_OPTION)
      .addOption(FUSE_FOLDER_OPTION)
      .addOption(THREAD_NUMBER_OPTION)
      .addOption(ITERATION_OPTION)
      .addOption(DURATION_OPTION)
      .addOption(RANDOM_READ_OPTION)
      .addOption(SAME_DATASET_OPTION)
      .addOption(FILE_SIZE_OPTION)
      .addOption(HELP_OPTION);

  // problem read big file with smaller buffer size is way too slow
  public static void main(String[] args) throws Exception {
    CommandLine cli = PARSER.parse(OPTIONS, args);
    String localFolder = cli.getOptionValue(LOCAL_FOLDER_OPTION_NAME);
    String fuseFolder = cli.getOptionValue(FUSE_FOLDER_OPTION_NAME);
    int threadNum = Integer.parseInt(cli.getOptionValue(THREAD_NUMBER_OPTION_NAME));
    int iteration = Integer.parseInt(cli.getOptionValue(ITERATION_OPTION_NAME));
    int duration = Integer.parseInt(cli.getOptionValue(DURATION_OPTION_NAME));
    boolean random = cli.hasOption(RANDOM_READ_OPTIONS_NAME) && Boolean.parseBoolean(cli.getOptionValue(RANDOM_READ_OPTIONS_NAME));
    boolean sameDataset = cli.hasOption(SAME_DATASET_OPTION_NAME) && Boolean.parseBoolean(cli.getOptionValue(SAME_DATASET_OPTION_NAME));
    int fileSize = cli.hasOption(FILE_SIZE_OPTION_NAME) ? Integer.parseInt(cli.getOptionValue(FILE_SIZE_OPTION_NAME)) : 0;

    if (threadNum == 1) {
      // runSingleThreadFullTest(localFolder, fuseFolder, random, iteration, duration);
      runFullTest(localFolder, fuseFolder);
      return;
    }
    if (fileSize > 0) {
      runMultiThreadSameFileTest(localFolder, fuseFolder, fileSize, random, threadNum, iteration, duration);
      return;
    }
    if (sameDataset) {
      runMultiThreadSameFileSetTest(localFolder, fuseFolder, random, threadNum, iteration, duration);
    } else {
      runMultiThreadIsolatedFileTest(localFolder, fuseFolder, random, threadNum, iteration, duration);
    }
    
    System.out.println("Test finished");
    System.exit(0);
  }

  public static void runFullTest(String localFolder, String fuseFolder) throws IOException, InterruptedException {
    List<Thread> threads = new ArrayList<>();
    for (long fileSize : FILE_SIZES) {
      for (long bufferSize : READ_BUFFER_SIZES) {
        Thread thread = new Thread(() -> {
          try {
            Path[] paths = prepareDataset(localFolder, fuseFolder, fileSize, "FuseTest");
            Path localPath = paths[0];
            Path fusePath = paths[1];
            try {
              SequentialReadTest.testSingleBuffer(localPath.toString(), fusePath.toString(), fileSize, bufferSize, 1);
            } finally {
              Files.delete(localPath);
              Files.delete(fusePath);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
        threads.add(thread);
      }
    }
    Collections.shuffle(threads);
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
  }
  
  public static void runSingleThreadFullTest(String localFolder, String fuseFolder, boolean random, int iteration, int duration) throws IOException {
    for (long fileSize : FILE_SIZES) {
      Path[] paths = prepareDataset(localFolder, fuseFolder, fileSize, "FuseTest");
      Path localPath = paths[0];
      Path fusePath = paths[1];
      try {
        for (long bufferSize : READ_BUFFER_SIZES) {
          if (bufferSize > fileSize) {
            break;
          }
          if (random) {
            RandomReadTest.testSingleBuffer(localPath.toString(), fusePath.toString(), fileSize, bufferSize, iteration);
          } else {
            SequentialReadTest.testSingleBuffer(localPath.toString(), fusePath.toString(), fileSize, bufferSize, iteration);
          }
        }
      } finally {
        Files.delete(localPath);
        Files.delete(fusePath); 
      }
    }
  }

  public static void runMultiThreadSameFileTest(String localFolder, String fuseFolder, long fileSize, boolean random,  int threadNum, int iteration, int duration) throws IOException, InterruptedException {
    Path[] paths = prepareDataset(localFolder, fuseFolder, fileSize, "FuseTest");
    Path localPath = paths[0];
    Path fusePath = paths[1];
    try {
      long endTime = System.currentTimeMillis() + (long) duration * 60 * 1000;
      final CyclicBarrier barrier = new CyclicBarrier(threadNum);
      List<Thread> threads = new ArrayList<>(threadNum);
      for (int i = 0; i < threadNum; i++) {
        Thread t = new Thread(() -> {
          try {
            barrier.await();
            if (random) {
              RandomReadTest.testAllBuffer(localPath.toString(), fusePath.toString(), fileSize, READ_BUFFER_SIZES, iteration, endTime);
            } else {
              SequentialReadTest.testAllBuffer(localPath.toString(), fusePath.toString(), fileSize, READ_BUFFER_SIZES, iteration, endTime);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
        threads.add(t);
      }
      Collections.shuffle(threads);
      for (Thread t : threads) {
        t.start();
      }
      for (Thread t : threads) {
        t.join();
      }
    } finally {
      Files.delete(localPath);
      Files.delete(fusePath); 
    }
  }
  
  public static void runMultiThreadSameFileSetTest(String localFolder, String fuseFolder, boolean random,  int threadNum, int iteration, int duration) throws IOException, InterruptedException {
    int fileNumber = FILE_SIZES.length;
    Path[] localPaths = new Path[fileNumber];
    Path[] fusePaths = new Path[fileNumber];
    for (int i = 0; i < fileNumber; i++) {
      Path[] dataset = prepareDataset(localFolder, fuseFolder, FILE_SIZES[i], "FuseTest" + i);
      localPaths[i] = dataset[0];
      fusePaths[i] = dataset[1];
    }
    long endTime = System.currentTimeMillis() + (long) duration * 60 * 1000;
    final CyclicBarrier barrier = new CyclicBarrier(threadNum);
    List<Thread> threads = new ArrayList<>(threadNum);
    for (int i = 0; i < threadNum; i++) {
      final int thread_id = i;
      Thread t = new Thread(() -> {
        try {
          barrier.await();
          int index = thread_id % fileNumber;
          Path localPath = localPaths[index];
          Path fusePath = fusePaths[index];
          if (random) {
            RandomReadTest.testAllBuffer(localPath.toString(), fusePath.toString(), FILE_SIZES[index], READ_BUFFER_SIZES, iteration, endTime);
          } else {
            SequentialReadTest.testAllBuffer(localPath.toString(), fusePath.toString(), FILE_SIZES[index], READ_BUFFER_SIZES, iteration, endTime);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      threads.add(t);
    }
    Collections.shuffle(threads);
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    for (int i = 0; i < fileNumber; i++) {
      Files.delete(localPaths[i]);
      Files.delete(fusePaths[i]);
    }
  }

  /**
   * Each thread create a single file, each iteration execute the read for all buffer sizes.
   * 
   * @param localFolder
   * @param fuseFolder
   * @param random
   * @param threadNum
   * @throws Exception
   */
  public static void runMultiThreadIsolatedFileTest(String localFolder, String fuseFolder, boolean random,  int threadNum, int iteration, int duration)
      throws Exception {
    final CyclicBarrier barrier = new CyclicBarrier(threadNum);
    List<Thread> threads = new ArrayList<>(threadNum);
    long endTime = System.currentTimeMillis() + (long) duration * 60 * 1000;
    for (int i = 0; i < threadNum; i++) {
      final int thread_id = i;
      Thread t = new Thread(() -> {
        try {
          barrier.await();
          long fileSize = FILE_SIZES[thread_id % FILE_SIZES.length];
          Path[] paths = prepareDataset(localFolder, fuseFolder, fileSize, "FuseTests" + thread_id);
          Path localPath = paths[0];
          Path fusePath = paths[1];
          try {
            if (random) {
              RandomReadTest.testAllBuffer(localPath.toString(), fusePath.toString(), fileSize, READ_BUFFER_SIZES, iteration, endTime);
            } else {
              SequentialReadTest.testAllBuffer(localPath.toString(), fusePath.toString(), fileSize, READ_BUFFER_SIZES, iteration, endTime);
            }
          } finally {
            Files.delete(localPath);
            Files.delete(fusePath);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      threads.add(t);
    }
    Collections.shuffle(threads);
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
  }
  
  private static Path[] prepareDataset(String localFolder, String fuseFolder, long fileSize, String namePrefix) throws IOException {
    String fileName = getRandomFileName("FuseTest");
    Path localPath = Paths.get(localFolder, fileName);
    Path fusePath = Paths.get(fuseFolder, fileName);
    createFileWithLen(localPath.toString(), fileSize);
    Files.copy(localPath, fusePath);
    return new Path[]{localPath, fusePath};
  }


  public static void validateDataCorrectness(RandomAccessFile localInStream, byte[] data, long offset, int length) throws IOException {
    byte[] localBuffer = new byte[length];
    localInStream.seek(offset);
    int localOffset = 0;
    while (localOffset < length) {
      int localBytesRead = localInStream.read(localBuffer, localOffset, length - localOffset);
      if (localBytesRead == -1) {
        throw new IOException(String.format("Cannot read %s bytes of offset %s from local file", length, offset));
      }
      localOffset += localBytesRead;
    }
    for (int i = 0; i < length; i++) {
      if (localBuffer[i] != data[i]) {
        throw new IOException("Data read from fuse is different from data read from local");
      }
    }
  }

  public static void createFileWithLen(String filePath, long len) throws IOException {
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

  public static String getRandomFileName(String prefix) {
    return prefix + RANDOM.nextLong();
  }
}
