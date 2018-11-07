package mserver;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

public class server {
    final static int NUM_SERVERS = 5;
    static boolean recovering = false;

    public static void main(String[] args) {
        if (args.length != 1 + 2) { //only argument should be ports of server
            System.err.println("Usage: java mserver/server <clients' port number> <MServer host> <MServer port>");
            System.exit(1);
        }

        Scanner scan = new Scanner(System.in);
        String inputLine;
        ArrayList<metadata> filesHere = new ArrayList<>(); //ArrayList for holding all metadata this server holds

        serverClientsThread servClient = new serverClientsThread(Integer.parseInt(args[0]));
        servClient.start();

        //GENERATE STARTING METADATA?

        //CONNECT TO MSERVER
        try {
            String currentDirectory =  "directory" + File.separator + InetAddress.getLocalHost().getHostAddress() + File.separator;

            File dir = new File("directory" + File.separator);

            if(dir.mkdir()) {  //returns true if directory is created
                System.out.println("Directory created");
        }

            serverMserverThread servMserv = new serverMserverThread(args[1], Integer.parseInt(args[2]), filesHere);  //pass metadata arraylist pointer
            servMserv.start();

            //wait for messages from user
            while ((inputLine = scan.nextLine()) != null) {
                if (inputLine.toUpperCase().equals("STOP")) {
                    System.out.println("Stopping Heartbeats.");
                    //Stop heartbeat messages
                    servMserv.setHeartbeating(false);
                    recovering = true;

                } else if (inputLine.toUpperCase().equals("START")) {
                    System.out.println("Restarting Heartbeats.");
                    //Start heartbeat messages
                    servMserv.setHeartbeating(true);

                    for (metadata file: filesHere) { //empty all files stored here.
                        for (String name: file.chunkNames) {
                            FileWriter fileWriter = new FileWriter(currentDirectory + name + ".txt");
                            //open writer to file chunk
                            fileWriter.write("");
                            fileWriter.close();
                        }
                    }

                } else {
                    System.out.println("Commands are: STOP or START");
                }
            }

        } catch (Exception e) {
            System.err.println("Server-to-MServer error.");
            e.printStackTrace();
        }

    }

    /*
    public static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }
    */
}



