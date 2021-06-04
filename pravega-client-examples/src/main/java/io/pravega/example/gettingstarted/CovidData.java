package io.pravega.example.gettingstarted;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;

import java.io.*;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class CovidData{

    private static final String PRAVEGA_SCOPE = "coviddata";
    private static final String PRAVEGA_STREAM = "summarydetails";
    private static final String READER_GROUP_NAME = "myReaderGroup4";
    private static final URI PRAVEGA_ENDPOINT = URI.create("lrmk147.lss.emc.com:9090");
    //private static final String DATASET_PATH = "/home/qian/Desktop/workspace/covidData/summary_details/2020_01_22_00_Summary_Details.csv";
    private static final String DATASET_PATH = "/home/intern/data/COVID19_Tweets_Dataset/Summary_Details/2020_01/2020_01_22_01_Summary_Details.csv";

    public static void main(String[] args) throws IOException {
        ingestData();
    }
    /**
     * ingestData example: i) create a Scope and Stream, ii) crete an {@link EventStreamWriter} and a process that writes
     * {@link TweetData} events with data from a real file, and iii) create a {@link ReaderGroup}, an
     * {@link EventStreamReader} and a process that reads the {@link TweetData} events.
     */

    private static void ingestData() {

        @Cleanup("shutdown")
        ExecutorService executorService = new ScheduledThreadPoolExecutor(4);

        // Create the Scope and Stream against Pravega.
        @Cleanup
        StreamManager streamManager = StreamManager.create(PRAVEGA_ENDPOINT);
        streamManager.createScope(PRAVEGA_SCOPE);
        streamManager.createStream(PRAVEGA_SCOPE, PRAVEGA_STREAM, StreamConfiguration.builder().build());

        // Create the factory to instantiate writers and readers.
        ClientConfig clientConfig =  ClientConfig.builder().controllerURI(PRAVEGA_ENDPOINT).build();
        @Cleanup
        EventStreamClientFactory factory = EventStreamClientFactory.withScope(PRAVEGA_SCOPE, clientConfig);

        // Create a writer.
        @Cleanup
        EventStreamWriter<TweetData> writer = factory.createEventWriter(PRAVEGA_STREAM,
                new JavaSerializer<>(), EventWriterConfig.builder().build());

        // Write some data and wait for the response.
        CompletableFuture<Void> writerTask = CompletableFuture.runAsync(() -> {
            try (BufferedReader fileReader = new BufferedReader(new FileReader(DATASET_PATH))) {
                boolean finished = false;
                while (!finished) {
                    String line = fileReader.readLine();
                    // Finished with the file, so quit the loop.
                    if (line == null) {
                        finished = true;
                        continue;
                    }
                    try {
                        TweetData tweetData = TweetData.parseTweetData(line);
                        writer.writeEvent(tweetData).join();
                        System.out.println("Written to Pravega: " + tweetData);
                        Thread.sleep(1);
                    } catch (Exception ex) {
                        System.err.println("Problem parsing line: " + line);
                        ex.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }, executorService);

        // Then, create a reader group and a reader to read the data from Pravega.
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(PRAVEGA_SCOPE, clientConfig);
        readerGroupManager.createReaderGroup(READER_GROUP_NAME,
                ReaderGroupConfig.builder().stream(Stream.of(PRAVEGA_SCOPE, PRAVEGA_STREAM)).build());

        @Cleanup
        EventStreamReader<TweetData> reader = factory.createReader("reader1", READER_GROUP_NAME,
                new JavaSerializer<>(), ReaderConfig.builder().build());

        // Read the data back.
        // Write some data and wait for the response.
        CompletableFuture<Void> readerTask = CompletableFuture.runAsync(() -> {
            EventRead<TweetData> tweetData;
            do {
                tweetData = reader.readNextEvent(5000);
                if (!tweetData.isCheckpoint()) {
                    System.out.println("Read from Pravega: " + tweetData.getEvent());
                }
            } while (tweetData.isCheckpoint() || tweetData.getEvent() != null);
        }, executorService);

        // Wait for the tasks to complete.
        CompletableFuture.allOf(writerTask, readerTask).join();
    }

    /**
     * One of the datasets contain the following fields for every Tweet:
     * Tweet_ID,Language,Geolocation_coordinate,RT,Likes,Retweets,Country,Date Created
     */
    @Data
    @AllArgsConstructor
    private static class TweetData implements Serializable {

        private static SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZ yyyy");
        private long tweetId;
        private String language;
        private boolean geolocation;
        private boolean rt;
        private long likes;
        private long retweets;
        private String country;
        private Date creationTime;

        public static TweetData parseTweetData(String line) throws ParseException {
            String[] fields = line.trim().split(",");
            return new TweetData(Long.parseLong(fields[0]), fields[1], Boolean.parseBoolean(fields[2]),
                    Boolean.parseBoolean(fields[3]), Long.parseLong(fields[4]), Long.parseLong(fields[5]),
                    fields[6], formatter.parse(fields[7]));
        }
    }
}

