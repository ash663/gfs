package mserver;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;

public class serverMserverThread extends Thread {
    private Socket socket;
    private boolean heartBeating = true;
    String serverAddress;

    ArrayList<metadata> filesHere; //ArrayList for holding all metadata this server holds
    ObjectOutputStream oos;

    public serverMserverThread(String host, int port, ArrayList<metadata> filesHere) {
        try {
            this.socket = new Socket(host, port);
            this.filesHere = filesHere;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setHeartbeating(boolean value) {
        heartBeating = value;
    }


    public void run() {
        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            Object inputObj;
            String chunkname = "initialized";
            int readOffset;
            long skipped;
            String inputline;

            String currentDirectory = "directory" + File.separator + socket.getLocalAddress() + File.separator;

            //START HEARTBEAT MESSAGES
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new sendHeartbeat(), 0, 5000); //sends a heartbeat msgs every 5 seconds (5000 ms)

            //LISTEN FOR MSERVER COMMANDS
            while ((inputObj = ois.readObject()) != null) {
                if (inputObj instanceof metadata) {         //"CREATE FILE"
                    System.out.println("Received create chunk.");

                    Iterator<String> iter = ((metadata) inputObj).chunkNames.iterator();
                    while (iter.hasNext()) { //Assumes file to create is last in list.
                        // (server just created metadata for this new file, so should be only one chunk in list)
                        chunkname = iter.next();
                    }

                    File file = new File(currentDirectory + chunkname + ".txt");
                    if (file.createNewFile()) {
                        System.out.println("File \"" + chunkname + ".txt\" has been created successfully.");
                    } else {
                        System.err.println("File create error (already exists).");
                    }
                    filesHere.add((metadata) inputObj);

                } else if (inputObj instanceof String) {
                    if (((String) inputObj).equals("CHECK")) {      //"CHECK CHUNK SIZE"
                        System.out.println("Received chunk size request ");
                        chunkname = (String) ois.readObject();  //read chunk's name
                        System.out.println("on " + chunkname + ".txt");

                        File file = new File(currentDirectory + chunkname + ".txt");

                        System.out.println(file.length());
                        oos.writeObject(file.length());
                    } else if (((String) inputObj).equals("PAD")) { //"PAD CHUNK WITH NULL"
                        System.out.print("Received pad request ");
                        chunkname = (String) ois.readObject();  //read chunk's name
                        System.out.println("on " + chunkname + ".txt");

                        File file = new File(currentDirectory + chunkname + ".txt");
                        FileWriter fileWriter = new FileWriter(file, true);

                        //appends null to end of file
                        fileWriter.append(null);
                        fileWriter.flush();
                        fileWriter.close();
                    } else if (((String) inputObj).toUpperCase().equals("READ")) {
                        System.out.println("MServer read starting");

                        serverAddress = (String) ois.readObject();
                        chunkname = (String) ois.readObject();

                        FileInputStream fstream = new FileInputStream(currentDirectory + chunkname + ".txt");
                        InputStreamReader istream = new InputStreamReader(fstream);
                        BufferedReader breader = new BufferedReader(istream);

                        //RESPOND TO CLIENT. Loop sending every char read from the file
                        while ((inputline = breader.readLine()) != null) {
                            oos.writeObject("RECOVERYWRITE`" + serverAddress + "`" + chunkname + "`" + inputline);
                        }

                        fstream.close(); //close filestream

                        System.out.println("MServer read complete");

                    } else if (((String) inputObj).toUpperCase().equals("APPEND") || ((String) inputObj).toUpperCase().equals("WRITE")) {
                        System.out.println("MServer write starting");
                        chunkname = (String) ois.readObject();

                        File file = new File(currentDirectory + chunkname + ".txt");
                        FileWriter fileWriter = new FileWriter(file, true); //overwrite from beginning of file
                        //open writer to file chunk

                        fileWriter.write((String) ois.readObject() + "\n"); //append the string to the end of the chunk

                        fileWriter.close(); //close fileWriter
                        System.out.println("MServer write complete");
                    }

                } else {
                    System.err.println("Server received unknown msg from Mserver."); //Unrecognized message
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    class sendHeartbeat extends TimerTask {

        public void run() {
            //System.out.println("SendHeartbeat executing");
            try {
                if (heartBeating) {
                    //SEND HEARTBEAT MESSAGE
                    if (filesHere.size() == 0 || filesHere.isEmpty()) { //even if we have no metadata, keep telling Mserver we're still up
                        //System.out.println("Sending Heartbeat msg - KEEPALIVE");
                        oos.writeObject("KEEPALIVE");
                        oos.flush();
                    } else {
                        for (int i = 0; i < filesHere.size(); i++) {
                            //System.out.println("Sending Heartbeat msg - metadata");
                            oos.writeObject(filesHere.get(i));
                            oos.flush();
                        }
                    }
                } else {
                    System.err.println("Not sending heartbeat.");
                }

            } catch (Exception e) {
                System.err.println("Error writing heartbeat message to Mserver.");
                e.printStackTrace();
            }
        }
    }
}

