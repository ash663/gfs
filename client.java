package mserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class client {
    final static int NUM_CLIENTS = 2;

    public static void main(String[] args) {
        if (args.length != (2 + (server.NUM_SERVERS * 2))) {
            System.err.println("Usage: java mserver/client <MServerhost> <MServerport> <server1host> <server1port> ... <serverNport>");
            System.err.println("Should be " + server.NUM_SERVERS + " number of servers");
            System.exit(1);
        }

        String inputLine, filename, readOffset;
        int writeOffset;
        byte[] b;

        String[] curServHost = new String[3]; //"initialized"; curServHost2 = "initialized", curServHost3 = "initialized";
        String servChunk = "initialized";
        String servOffset = "initialized";
        int curServPort = -1;
        int[] curServNum = new int[3];
        boolean[] foundServ = new boolean[3];
        boolean error = false;
        Scanner scan = new Scanner(System.in);
        String[] serverHosts = new String[server.NUM_SERVERS];
        int[] serverPorts = new int[server.NUM_SERVERS];
        Socket[] serverSockets = new Socket[server.NUM_SERVERS];
        BufferedReader[] servInputs = new BufferedReader[server.NUM_SERVERS]; //for reading input from servers
        PrintWriter[] servOutputs = new PrintWriter[server.NUM_SERVERS]; //for writing output to servers
        String appendingData;
        boolean[] downServers = new boolean[3];
        String commitResponse;
        boolean decision = true;
        for (int i3 = 0; i3 < 3; i3++) {
            curServNum[i3] = -1;
            foundServ[i3] = false;
            downServers[i3] = false;
        }


        //CONNECT TO MSERVER
        try {
            Socket MservSock = new Socket(args[0], Integer.parseInt(args[1]));
            BufferedReader MservInput = new BufferedReader(new InputStreamReader(MservSock.getInputStream()));
            PrintWriter MservOutput = new PrintWriter(MservSock.getOutputStream(), true);

            //CONNECT TO SERVERS
            int i2 = 0;
            for (int i = 2; i < 2 + (server.NUM_SERVERS * 2); i = i + 2) {
                serverHosts[i2] = args[i];
                serverPorts[i2] = Integer.parseInt(args[i + 1]);
                i2++;
            }

            for (int i = 0; i < server.NUM_SERVERS; i++) {
                serverSockets[i] = new Socket(serverHosts[i], serverPorts[i]);
                servInputs[i] = new BufferedReader(new InputStreamReader(serverSockets[i].getInputStream()));
                servOutputs[i] = new PrintWriter(serverSockets[i].getOutputStream(), true);
            }

            System.out.println("Waiting for client requests:");

            //LOOP READING SYS INPUT, SENDING REQUESTS TO MSERVER, FORWARDING RESPONSE TO SERVER, THEN FORWARDING RESPONSE TO CLIENT
            while ((inputLine = scan.nextLine()) != null) {
                if (inputLine.toUpperCase().equals("CREATE")) {
                    System.out.println("Enter filename: ");
                    filename = scan.nextLine();

                    //SEND REQUESTS TO MSERVER
                    MservOutput.println("CREATE");
                    MservOutput.println(filename);

                    System.out.println("Sent to Mserver");

                    //GET MSERVER RESPONSE
                    for (int i = 0; i < 3; i++) {
                        System.out.println(MservInput.readLine());  //RESPOND TO USER
                    }

                } else if (inputLine.toUpperCase().equals("READ")) {
                    System.out.println("Enter filename: ");
                    filename = scan.nextLine();
                    System.out.println("Enter offset: ");
                    readOffset = scan.nextLine();

                    //SEND REQUESTS TO MSERVER
                    MservOutput.println("READ");
                    MservOutput.println(filename);
                    MservOutput.println(readOffset);

                    System.out.println("Sent to Mserver");

                    //GET MSERVER RESPONSE
                    while ((inputLine = MservInput.readLine()) != null) {
                        System.out.println(inputLine);
                        if (inputLine.toUpperCase().equals("BREAK")) {
                            break;
                        } else if (inputLine.toUpperCase().equals("SERVER")) {
                            curServHost[0] = MservInput.readLine();    //reader consumes newline char
                            System.out.println(curServHost[0]);
                            //curServPort = Integer.parseInt(MservInput.readLine());
                        } else if (inputLine.toUpperCase().equals("CHUNKNAME")) {
                            servChunk = MservInput.readLine();      //reader consumes newline char
                            System.out.println(servChunk);
                        } else if (inputLine.toUpperCase().equals("OFFSET")) {
                            servOffset = MservInput.readLine();     //reader consumes newline char
                            System.out.println(servOffset);
                        } else if (inputLine.toUpperCase().equals("DOWN")) {
                            System.err.println("Servers are down");
                            error = true;
                            break;
                        } else {
                            System.err.println("Unrecognized READ response.");
                            error = true;
                            break;
                        }
                    }

                    if (error) {
                        System.err.println("Error");
                        error = false; //reset
                    } else {
                        System.out.println("Checking Mserver response.");
                        System.out.println(curServHost[0]);

                        //CONTACT SERVER
                        for (int i = 0; i < server.NUM_SERVERS; i++) {

                            if (serverSockets[i].getInetAddress().getHostAddress().equals(curServHost[0])) {
                                curServNum[0] = i;
                                foundServ[0] = true;
                            }
                        }

                        if (foundServ[0]) {
                            System.out.println("Performing READ at " + curServHost[0] + " on file " + servChunk + " at offset " + servOffset);

                            servOutputs[curServNum[0]].println("READ");
                            servOutputs[curServNum[0]].println(servChunk);
                            servOutputs[curServNum[0]].println(servOffset);

                            //GET SERVER RESPONSE
                            while (!(inputLine = servInputs[curServNum[0]].readLine()).isEmpty()) {
                                    System.out.println(inputLine); //RESPOND TO USER
                            }

                            foundServ[0] = false; //reset server search variable
                        } else {
                            System.err.println("Received unknown server's info from Mserver");
                        }
                    }


                } else if (inputLine.toUpperCase().equals("APPEND") || inputLine.toUpperCase().equals("WRITE")) {
                    System.out.println("Enter filename: ");
                    filename = scan.nextLine();     //scanner consumes newline char

                    System.out.println("Enter string to append (with size < 2048): ");
                    inputLine = scan.nextLine();    //scanner consumes newline char
                    b = inputLine.getBytes(StandardCharsets.UTF_8);
                    writeOffset = b.length;

                    while (writeOffset > 2048) { //check string doesn't exceed 2048
                        System.err.println("Max amount of data that can be appended at a time is 2048 bytes");
                        System.out.println("Enter string to append: ");
                        inputLine = scan.nextLine(); //scanner consumes newline char
                        b = inputLine.getBytes();
                        writeOffset = b.length;
                    }

                    //SEND REQUESTS TO MSERVER
                    MservOutput.println("WRITE");
                    MservOutput.println(filename);
                    MservOutput.println(writeOffset);

                    System.out.println("Sent to Mserver");

                    //READ 3 Servers info
                    curServHost[0] = MservInput.readLine();
                    if (curServHost[0].equals("DOWN")) {
                        downServers[0] = true;
                    } else if (curServHost[0].equals("ERROR")) {
                        error = true;
                    } else {
                        servChunk = MservInput.readLine();
                    }

                    if (error) {
                        error = false; //reset
                    } else {
                        curServHost[1] = MservInput.readLine();
                        if (curServHost[1].equals("DOWN")) {
                            downServers[1] = true;
                        } else {
                            servChunk = MservInput.readLine();
                        }

                        curServHost[2] = MservInput.readLine();
                        if (curServHost[2].equals("DOWN")) {
                            downServers[2] = true;
                        } else {
                            servChunk = MservInput.readLine();
                        }

                        System.out.println(servChunk);

                        for (int i3 = 0; i3 < 3; i3++) {
                            //CONTACT SERVER
                            for (int i = 0; i < server.NUM_SERVERS; i++) {
                                if (serverSockets[i].getInetAddress().getHostAddress().equals(curServHost[i3])) {
                                    curServNum[i3] = i;
                                    foundServ[i3] = true;
                                }
                            }
                            System.out.println(foundServ[i3]);
                            System.out.println(downServers[i3]);
                        }

                        //GET SERVER RESPONSE
                        //start 2P
                        for (int i3 = 0; i3 < 3; i3++) {
                            if (foundServ[i3] && !downServers[i3]) {
                                servOutputs[curServNum[i3]].println("WRITE");
                            }
                        }
                        //check for cohort responses
                        for (int i3 = 0; i3 < 3; i3++) {
                            if (foundServ[i3] && !downServers[i3]) {
                                commitResponse = servInputs[curServNum[i3]].readLine();
                                if (commitResponse.equals("ABORT")) {
                                    decision = false;
                                }
                                System.out.println(commitResponse);  //Should be "AGREED"
                            }
                        }

                        if (decision) {
                            //tell server where to write
                            for (int i3 = 0; i3 < 3; i3++) {
                                if (foundServ[i3] && !downServers[i3]) {
                                    servOutputs[curServNum[i3]].println(servChunk);
                                }
                            }

                            appendingData = new String(b, StandardCharsets.UTF_8); //convert byte[] back into string and send it to server

                            //send the data
                            for (int i3 = 0; i3 < 3; i3++) {
                                if (foundServ[i3] && !downServers[i3]) {
                                    servOutputs[curServNum[i3]].println(appendingData);
                                }
                            }

                            //RESPOND TO USER
                            for (int i3 = 0; i3 < 3; i3++) {
                                if (foundServ[i3] && !downServers[i3]) {
                                    System.out.println(servInputs[curServNum[i3]].readLine());
                                }
                            }
                        }
                        //reset server search variables
                        for (int i3 = 0; i3 < 3; i3++) {
                            foundServ[i3] = false;
                            downServers[i3] = false;
                        }
                        decision = true;
                        MservOutput.println("DONE"); //Tell metaaserver writes are done
                    }
                } else {
                    System.out.println("Input not recognized. (Should be CREATE or READ or APPEND)");
                    System.out.println("Input was " + inputLine);
                }
            }

        } catch (Exception e) {
            System.err.println("Client to MServer connection error.");
            e.printStackTrace();
        }

    }
}
