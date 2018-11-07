package mserver;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class serverClientThread extends Thread {
    private Socket socket;

    public serverClientThread(Socket socket) {
        this.socket = socket;
    }

    public void run() {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String inputLine, chunkname;
            int readByte;
            int readOffset;
            long skipped;
            String currentDirectory;

            File dir = new File("directory" + File.separator + socket.getLocalAddress());
            if(dir.mkdir()) {  //returns true if directory is created
                System.out.println("Directory created");
            }

            currentDirectory =  "directory" + File.separator + socket.getLocalAddress() + File.separator;

            while ((inputLine = input.readLine()) != null) { //wait for messages from client

                //CHECK CLIENT REQUESTS
                if (inputLine.toUpperCase().equals("READ")) {
                    chunkname = input.readLine();
                    FileInputStream fstream = new FileInputStream(currentDirectory + chunkname + ".txt");
                    InputStreamReader istream = new InputStreamReader(fstream);
                    BufferedReader breader = new BufferedReader(istream);

                    readOffset = Integer.parseInt(input.readLine());
                    skipped = breader.skip(readOffset);

                    if(readOffset != skipped) {
                        System.err.println("Skipped " + skipped + ", not " + readOffset + " number of bytes.");
                    }

                    if( (inputLine = breader.readLine()) == null || inputLine.isEmpty() ) {
                        out.println("No read. End of file found.");
                    } else {
                        out.println(inputLine);
                        while ((inputLine = breader.readLine()) != null) {
                            out.println(inputLine);
                        }
                    }

                    //RESPOND TO CLIENT. Loop sending every line from the file

                    out.println(""); //send null string for client to break loop

                    fstream.close(); //close filestream

                } else if (inputLine.toUpperCase().equals("APPEND") || inputLine.toUpperCase().equals("WRITE") ) {
                    if(server.recovering) {
                        out.println("ABORT");
                    } else {
                        out.println("AGREED"); //for 2-phase protocol
                        chunkname = input.readLine();

                        FileWriter fileWriter = new FileWriter(currentDirectory + chunkname + ".txt", true);
                        //open writer to file chunk

                        fileWriter.write(input.readLine() + System.lineSeparator()); //append the string to the end of the chunk

                        out.println("DONE"); //RESPOND TO CLIENT.

                        fileWriter.close(); //close fileWriter
                    }
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

