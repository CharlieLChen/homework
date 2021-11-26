package com.dellemc.pravega.chat;

import com.dellemc.pravega.PravegaUtil;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class Chat {

    private String self;
    private String peer;
    private WriteMessageTask writeChannel;
    private ReadMessageTask readChannel;

    public Chat(String self, String peer) {
        this.self = self;
        this.peer = peer;
        PravegaUtil.createScope();
    }

    public void writeMessage() {
        writeChannel = new WriteMessageTask(self, this);
        new Thread(writeChannel).start();
    }

    public void readMessage() {
        readChannel = new ReadMessageTask(self, peer);
        new Thread(readChannel).start();
    }

    public void startChat() {
        System.out.println();
        System.out.println("You: " + self);
        System.out.println("Peer: " + peer);
        System.out.println("=======================================================================");
        writeMessage();
        readMessage();
    }

    public void stop() {
        writeChannel.stop();
        readChannel.stop();
        PravegaUtil.close();
    }
}


class WriteMessageTask implements Runnable {
    private String stream;
    private Chat chat;
    private AtomicBoolean stop = new AtomicBoolean(false);

    WriteMessageTask(String stream, Chat chat) {
        this.stream = stream;
        this.chat = chat;
    }

    @Override
    public void run() {
        PravegaUtil.createStream(stream);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(PravegaUtil.SCOPE, PravegaUtil.clientConfig);
        EventStreamWriter<Message> eventWriter = clientFactory.createEventWriter(stream, new JavaSerializer<Message>(), EventWriterConfig.builder().build());
        while (!stop.get()) {
            try {
                System.out.printf("You(%s): ", stream);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
                String s = bufferedReader.readLine().trim();
                // ignore the empty input
                if (s.length() == 0) {
                    continue;
                }
                // wrap the input
                Message message = handleInput(s);
                eventWriter.writeEvent(message);
                if (message.getMessageType().equals(Message.UPLOAD_MSG)) {
                    System.out.println("upload file " + message.getFileName() + " successfully");
                }

                // received the stop signal, call chat.stop() to terminate current chat
                if (s.equals("bye")) {
                    break;
                }
            } catch (Exception e) {
                System.out.println("WriteMessageTask occurred an error " + e);
                break;
            }
        }

        chat.stop();
        clientFactory.close();
        eventWriter.close();

    }

    public void stop() {
        stop.set(true);
    }

    public Message handleInput(String msg) throws Exception {
        if (msg.startsWith(Message.UPLOAD_MSG)) {
            String[] split = msg.split("@");
            String filePath = split[1];

            Path path = Paths.get(filePath);
            Path fileName = path.getFileName();
            byte[] bytes = Files.readAllBytes(path);
            return new Message(fileName.toString(), bytes);
        }

        return new Message(msg);
    }
}

class ReadMessageTask implements Runnable {

    private String peer;
    private String self;
    private String readerGroup;
    private String readerId;

    private AtomicBoolean stop = new AtomicBoolean(false);

    ReadMessageTask(String self, String peer) {
        this.peer = peer;
        this.self = self;
        this.readerGroup = "readergroup" + peer;
        this.readerId = "readerid" + peer;
    }

    @Override
    public void run() {
        //wait for peer stream creating
        while (!PravegaUtil.streamCreated(peer)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("waiting stream created been interrupted");
                return;
            }
        }

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(PravegaUtil.SCOPE, PravegaUtil.CONTROLLER);
        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(PravegaUtil.SCOPE + "/" + peer).build());
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(PravegaUtil.SCOPE, PravegaUtil.clientConfig);
        EventStreamReader<Message> reader = clientFactory.createReader(readerId, readerGroup, new JavaSerializer<Message>(), ReaderConfig.builder().build());
        try {
            while (!stop.get()) {
                Message event = reader.readNextEvent(1000).getEvent();
                if (event != null) {
                    System.out.println();
                    handleEvent(event);
                    System.out.print(String.format("You(%s): ", self));
                }
            }
        } catch (Exception exception) {
            System.out.println("WriteMessageTask occurred an error " + exception);
        }

        // stop signal received or exception happened, need to close the resources.
        reader.close();
        readerGroupManager.deleteReaderGroup(readerGroup);
        readerGroupManager.close();
        System.out.println("End talk");
    }

    public void stop() {
        stop.set(true);
    }

    private void handleEvent(Message event) throws Exception {
        if (event.getMessageType().equals(Message.NORMAL_MSG)) {
            System.out.println(peer + ": " + event.getMsg());
            return;
        }

        String fileDir = PravegaUtil.BASE_PATH + "\\" + self;

        File dir = new File(fileDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        String fileName = fileDir + "\\" + event.getFileName();
        Path path = Paths.get(fileName);
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        Files.write(path, event.getContent());
        System.out.println("Received file " + event.getFileName() + " successfully");
        System.out.println();
    }
}


class Message implements Serializable {
    public static String NORMAL_MSG = "normal";
    public static String UPLOAD_MSG = "upload";

    private String messageType;
    private String msg;
    private String fileName;
    private byte[] content;

    Message(String msg) {
        this.msg = msg;
        messageType = NORMAL_MSG;
    }

    Message(String fileName, byte[] content) {
        this.messageType = UPLOAD_MSG;
        this.content = content;
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getMsg() {
        return msg;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }
}