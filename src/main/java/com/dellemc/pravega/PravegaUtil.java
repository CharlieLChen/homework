package com.dellemc.pravega;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;

import java.net.URI;

public class PravegaUtil {
    final public static String SCOPE = "dell";
    public static URI CONTROLLER;
    public static ClientConfig clientConfig = ClientConfig.builder().controllerURI(PravegaUtil.CONTROLLER).build();
    public static StreamManager streamManager;
    public static String BASE_PATH = "C:\\Users\\chenc49\\OneDrive - Dell Technologies\\Desktop\\chat";

    static {
        try {
            CONTROLLER = new URI("tcp://127.0.0.1:9090");
            streamManager = StreamManager.create(CONTROLLER);
        } catch (Exception e) {
            System.out.println("Initialize the app failed due to " + e);
            System.exit(1);
        }
    }

    public static void createScope() {
        streamManager.createScope(SCOPE);
    }

    public static void createStream(String stream) {
        streamManager.createStream(SCOPE, stream, StreamConfiguration.builder().build());
    }

    public static boolean streamCreated(String stream) {
        return streamManager.checkStreamExists(SCOPE, stream);
    }

    public static void close() {
        streamManager.close();
    }

}
