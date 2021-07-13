package Backup;

import kafka.server.KafkaConfig;

public class Config {
    public static String ACCESS_KEY = "";

    public static String SECRET_KEY = "";

    public static String BUCKET = "";

    public static String REGION = "";

    public static int MAX_INDEX_SIZE = 1024;

    public static int INDEX_INTERVAL_BYTES = 4096;

    public static Long ROLL_JITTER_MS = 0L;

    public Config(KafkaConfig config){
        MAX_INDEX_SIZE = config.logIndexSizeMaxBytes();
        INDEX_INTERVAL_BYTES = config.logIndexIntervalBytes();
        ROLL_JITTER_MS = config.logRollTimeJitterMillis();
    }
}
