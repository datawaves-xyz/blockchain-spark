package io.iftech.sparkudf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class TestUtils {

    public static String getStringFromFile(String filename) {
        InputStreamReader reader = new InputStreamReader(
            TestUtils.class.getResourceAsStream(filename),
            StandardCharsets.UTF_8
        );

        return new BufferedReader(reader).lines()
            .collect(Collectors.joining(System.lineSeparator()));
    }

    public static List<Object> convertSeqToList(Seq<Object> seq) {
        return JavaConverters.seqAsJavaList(seq);
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
