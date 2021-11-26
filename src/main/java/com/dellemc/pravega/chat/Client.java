package com.dellemc.pravega.chat;

public class Client {
    //upload@C:\Users\chenc49\OneDrive - Dell Technologies\Desktop\param\zk.txt

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            System.out.println("self and peer is required");
            System.exit(1);
        }
        String selfName = args[0].trim();
        String peerName = args[1].trim();
        Chat chat = new Chat(selfName, peerName);
        chat.startChat();
    }
}


