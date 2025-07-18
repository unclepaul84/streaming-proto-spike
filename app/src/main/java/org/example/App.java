/*
 * This source file was generated by the Gradle 'init' task
 */

package org.example;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.DynamicMessage;
import proto.*;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.archivers.tar.*;
import org.apache.commons.compress.compressors.gzip.*;

import java.io.*;
import java.nio.file.*;

public class App {

    public static void main(String[] args) throws Exception {

        TestStreamedProtoWithIndex();
        //TestStreamedProtoTarGz();
       // TestStreamedProto();

    }

    

    public static void TestStreamedProtoTarGz() throws Exception 
    {
        String baseFolder = "tar/";

        String write_File = baseFolder + "data.binpb";
        
        String index_File = baseFolder + "name.index";
      
        java.io.File indexFile = new java.io.File(index_File);
        
        OnDiskBPlusTree index = new OnDiskBPlusTree(index_File);
        
        if (indexFile.exists()) {
            if (!indexFile.delete()) {
                System.err.println("Failed to delete existing index file: " + index_File);
            }
        }

       

        PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader header = PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader
                .newBuilder()
                .setSource("Java App")
                .build();

        var writer = new PricesStreamableFileWriter(
                write_File, header, (offset, payload) -> {

                    try {
                        String name = payload.getPrice().getName();
                        if (name != null && !name.isEmpty()) {
                            index.insert(name.getBytes("UTF-8"), longToBytesBigE(offset));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });

        List<Double> prices = new ArrayList<Double>();
        for (int i = 0; i < 10000; i++) {
            prices.add(100.0 + i);
        }

        long totalBytesWritten = 0;
        var startTime = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {

            var priceEntity = PriceEntityOuterClass.PriceEntity.newBuilder()
                    .addAllPrices(prices)
                    .setCurrency("USD")
                    .setName("AAPL" + i)
                    .build();

            PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload payload = PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload
                    .newBuilder()
                    .setPrice(priceEntity)
                    .build();
            var bytesWritten = writer.Write(payload);

            if (i % 10000 == 0) {

                long elapsedTime = System.currentTimeMillis() - startTime;
                double averageBytesPerSecond = (double) totalBytesWritten / (elapsedTime /
                        1000.0) / (1024.0 * 1024.0);
                System.out.println("Average MB written per second: " + String.format("%.1f",
                        averageBytesPerSecond));

            }

            if (i % 100000 == 0) {
                System.out.println("Written " + i + " records");
            }

            totalBytesWritten += bytesWritten;
        }

        writer.close();
      
        createTarGz(Paths.get(baseFolder), Paths.get("prices.binpb.tar.gz"));
   
    }

    public static void TestStreamedProtoWithIndex() throws Exception {

        String baseFolder = "";

        String write_File = baseFolder + "price_entities_java_indexed.binpb";
        String read_File = baseFolder + "price_entities_java_indexed.binpb";
        String index_File = baseFolder +"name.index";
        int num_items = 100000;

        java.io.File indexFile = new java.io.File(index_File);
        OnDiskBPlusTree index = new OnDiskBPlusTree(index_File);
          if (indexFile.exists()) {
            if (!indexFile.delete()) {
                System.err.println("Failed to delete existing index file: " + index_File);
            }
        }

       

        PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader header = PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader
                .newBuilder()
                .setSource("Java App")
                .build();

        var writer = new PricesStreamableFileWriter(
                write_File, header, (offset, payload) -> {

                    try {
                        String name = payload.getPrice().getName();
                        if (name != null && !name.isEmpty()) {
                            index.insert(name.getBytes("UTF-8"), longToBytesBigE(offset));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });

        List<Double> prices = new ArrayList<Double>();
        for (int i = 0; i < 10000; i++) {
            prices.add(100.0 + i);
        }

        long totalBytesWritten = 0;
        var startTime = System.currentTimeMillis();

        for (int i = 0; i < num_items; i++) {

            var priceEntity = PriceEntityOuterClass.PriceEntity.newBuilder()
                    .addAllPrices(prices)
                    .setCurrency("USD")
                    .setName("AAPL" + i)
                    .build();

            PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload payload = PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload
                    .newBuilder()
                    .setPrice(priceEntity)
                    .build();
            var bytesWritten = writer.Write(payload);

            if (i % 10000 == 0) {

                long elapsedTime = System.currentTimeMillis() - startTime;
                double averageBytesPerSecond = (double) totalBytesWritten / (elapsedTime /
                        1000.0) / (1024.0 * 1024.0);
                System.out.println("Average MB written per second: " + String.format("%.1f",
                        averageBytesPerSecond));

            }

            if (i % 100000 == 0) {
                System.out.println("Written " + i + " records");
            }

            totalBytesWritten += bytesWritten;
        }

        writer.close();
   

        StreamableProtoFileParser<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> parser = null;
        try {
            parser = new StreamableProtoFileParser<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload>(
                    read_File, t -> {
                        try {
                            return PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader
                                    .parseFrom(t);
                        } catch (InvalidProtocolBufferException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }

                    }, h -> {
                        try {
                            return PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload
                                    .parseFrom(h);
                        } catch (InvalidProtocolBufferException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });
            var accessor = parser.GetPayloadRandomAccesor();

            PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader parsedHeader = accessor
                    .GetHeader();
                    long timerStart = System.currentTimeMillis();

                    for (int i = 0; i < num_items; i++) {
                        String name = "AAPL" + i;
                        index.search(name.getBytes()).forEach(value -> {
                            try {
                                long offset = bytesToLongBigE(value);
                                PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload payload = accessor
                                        .GetPayloadAtOffset(offset);
                               if(!name.equals(payload.getPrice().getName()))
                                {
                                    throw new RuntimeException("Name mismatch: " + name + " != " + payload.getPrice().getName());
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });

                    }

              
                    System.out.println("Elapsed time for indexed search: " + (System.currentTimeMillis() - timerStart) + " ms");


                    var   enumerator = parser.GetPayloadEnumerator();
                    timerStart = System.currentTimeMillis();
                    while(enumerator.GetNextPayload() != null) {
                        // Just iterate through the file to ensure it works
                    }
                    System.out.println("Elapsed time for full scan: " + (System.currentTimeMillis() - timerStart) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void TestStreamedProto() throws Exception {

        String write_File = "price_entities_java.binpb";
        String read_File = "price_entities_java.binpb";

        PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader header = PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader
                .newBuilder()
                .setSource("Java App")
                .build();

        var writer = new PricesStreamableFileWriter(
                write_File, header);

        List<Double> prices = new ArrayList<Double>();
        for (int i = 0; i < 10000; i++) {
            prices.add(100.0 + i);
        }

        long totalBytesWritten = 0;
        var startTime = System.currentTimeMillis();

        for (int i = 0; i < 30000; i++) {

            var priceEntity = PriceEntityOuterClass.PriceEntity.newBuilder()
                    .addAllPrices(prices)
                    .setCurrency("USD")
                    .build();

            PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload payload = PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload
                    .newBuilder()
                    .setPrice(priceEntity)
                    .build();
            var bytesWritten = writer.Write(payload);

            if (i % 10000 == 0) {

                long elapsedTime = System.currentTimeMillis() - startTime;
                double averageBytesPerSecond = (double) totalBytesWritten / (elapsedTime /
                        1000.0) / (1024.0 * 1024.0);
                System.out.println("Average MB written per second: " + String.format("%.1f",
                        averageBytesPerSecond));

            }

            if (i % 100000 == 0) {
                System.out.println("Written " + i + " records");
            }

            totalBytesWritten += bytesWritten;
        }

        writer.close();

        // Convert priceEntity to JSON using Google Protobuf library
        // String json = JsonFormat.printer().print(priceEntities.get(0));

        int recordCount = 0;

        // Read price entities from the file
        StreamableProtoFileParser<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> parser = null;
        try {
            parser = new StreamableProtoFileParser<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload>(
                    read_File, t -> {
                        try {
                            return PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader
                                    .parseFrom(t);
                        } catch (InvalidProtocolBufferException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }

                    }, h -> {
                        try {
                            return PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload
                                    .parseFrom(h);
                        } catch (InvalidProtocolBufferException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });
            var enumerator = parser.GetPayloadEnumerator();
            PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader parsedHeader = enumerator
                    .GetHeader();
            System.out.println(JsonFormat.printer().print(parsedHeader));
            while (true) {
                PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload payload = enumerator
                        .GetNextPayload();

                recordCount++;

                if (recordCount % 100000 == 0) {
                    System.out.println("Read " + recordCount + " records");
                }

                if (payload == null)
                    break;

                // System.out.println(JsonFormat.printer().print(payload));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Big Endian (network byte order) */

    public static byte[] longToBytesBigE(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }

    /* Big Endian (network byte order) */
    public static long bytesToLongBigE(byte[] bytes) {
        if (bytes == null || bytes.length != 8) {
            throw new IllegalArgumentException("Byte array must be exactly 8 bytes long");
        }
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[i] & 0xFF);
        }
        return value;
    }

    public static void createTarGz(Path sourceDir, Path tarGzPath) throws IOException {
        try (
            OutputStream fos = Files.newOutputStream(tarGzPath);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(bos);
            TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)
        ) {
            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            Files.walk(sourceDir).forEach(path -> {
                if (Files.isDirectory(path)) return;

                Path relativePath = sourceDir.relativize(path);
                TarArchiveEntry entry = new TarArchiveEntry(path.toFile(), relativePath.toString());

                try (InputStream is = Files.newInputStream(path)) {
                    taos.putArchiveEntry(entry);
                    is.transferTo(taos);
                    taos.closeArchiveEntry();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            taos.finish();
        }
    }
}
