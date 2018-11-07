package mserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;


public class MClientThread extends Thread {
    private Socket socket;
    ArrayList<metadata> allFiles;


    public MClientThread(Socket socket, ArrayList<metadata> allFiles) {
        this.socket = socket;
        this.allFiles = allFiles;
    }

    public void run() {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String inputLine, filename, offset;
            String chunkname = null, newChunkname;
            int chunkNum, chunksOffset;
            boolean foundMeta = false,  exists = false;
            boolean[] foundServ = new boolean[3];

            long current;
            int file = 0;
            int[] serv = new int[3];
            int[] servNum = new int[3];
            servNum[0] = 0;
            servNum[1] = 0;
            servNum[2] = 0;

            machine[] hostServer = new machine[3];
            hostServer[0] = null;
            hostServer[1] = null;
            hostServer[2] = null;
            MutableLong[] chunksize = new MutableLong[3];
            chunksize[0] = new MutableLong(-1);
            chunksize[1] = new MutableLong(-1);
            chunksize[2] = new MutableLong(-1);

            MutableLong[] updateTime = new MutableLong[3];
            updateTime[0] = new MutableLong(-1);
            updateTime[1] = new MutableLong(-1);
            updateTime[2] = new MutableLong(-1);

            Random rand = new Random(); //for randomization of server to create file
            metadata newmeta = new metadata();
            ArrayList<String> chunklist;
            ArrayList<machine> chunkServers;
            ArrayList<MutableLong> lastUpdate;
            ArrayList<MutableLong> chunkLength;
            String localAddress = socket.getLocalAddress().getHostAddress();
            int localPort = socket.getPort();
            Iterator<String> nameIter;
            Iterator<MutableLong> lengthIter;
            Iterator<machine> iter;

            System.out.println("Waiting on client connection.");
            while ((inputLine = input.readLine()) != null) { //RESPOND TO CLIENT REQUESTS
                if (inputLine.toUpperCase().equals("CREATE")) {
                    filename = input.readLine();

                    for(int i = 0; i < allFiles.size(); i++) {
                            if(allFiles.get(i).filename.equals(filename)) {
                                exists = true;
                            }
                    }
                    if(exists) {
                        System.out.println(filename + " already exists.");
                        out.println(filename + " already exists.");
                        exists = false;
                    } else {
                        System.out.println("Creating file " + filename);

                        //RANDOMLY DECIDE SERVER 3 servers
                        serv[0] = (rand.nextInt() & Integer.MAX_VALUE) % server.NUM_SERVERS; //pick random server
                        while (MServer.serverDown[serv[0]]) {           //if server is down, need to pick another server
                            serv[0] = (rand.nextInt() & Integer.MAX_VALUE) % server.NUM_SERVERS;
                        }
                        serv[1] = (serv[0]+1) % server.NUM_SERVERS;
                        serv[2] = (serv[1]+1) % server.NUM_SERVERS;

                        //System.out.println("chunk on servers " + MServerServersThread.MserverThreads[serv].getAddress());

                        //DECIDE CHUNKNAME
                        newChunkname = localAddress + String.valueOf(localPort) + String.valueOf(file);
                        //local address is for ease of checking. localPort+file for unique name
                        file++;


                        //CREATE METADATA
                        chunklist = new ArrayList<>();
                        chunklist.add(newChunkname);
                        chunklist.add(newChunkname);
                        chunklist.add(newChunkname);

                        chunkServers = new ArrayList<>();
                        for(int i =0; i<3; i++) {
                            chunkServers.add(new machine(MServerServersThread.MserverThreads[serv[i]].getAddress(), MServerServersThread.MserverThreads[serv[i]].getPort()));
                        }
                        lastUpdate = new ArrayList<>();
                        MutableLong creation = new MutableLong(System.currentTimeMillis());
                        lastUpdate.add(creation);
                        lastUpdate.add(creation);
                        lastUpdate.add(creation);

                        chunkLength = new ArrayList<>();
                        chunkLength.add(new MutableLong(0)); //new chunk has no data, so length is 0
                        chunkLength.add(new MutableLong(0));
                        chunkLength.add(new MutableLong(0));

                        newmeta = new metadata(filename, chunklist, chunkServers, lastUpdate, chunkLength);
                        allFiles.add(newmeta);

                        //TELL SERVERS
                        for(int i =0; i<3; i++) {
                            out.println(MServerServersThread.MserverThreads[serv[i]].createChunk(newmeta)); //RESPOND TO CLIENT                        }
                        }
                    }
                } else if (inputLine.toUpperCase().equals("READ")) {
                    filename = input.readLine();
                    offset = input.readLine();

                    System.out.println("READ on " + filename + " at " + offset );

                    chunkNum = Integer.parseInt(offset) / 8192;
                    chunksOffset = Integer.parseInt(offset) % 8192;

                    System.out.println("Equivalent to chunknum " + chunkNum);

                    //DETERMINE APPROPRIATE CHUNK
                    for (int i = 0; i < allFiles.size(); i++) {
                        newmeta = allFiles.get(i);
                        if (newmeta.filename.equals(filename)) {
                            foundMeta = true;
                            break;
                        }
                    }

                    if (foundMeta) {


                        //find server number to check if server is down
                        for(int i2 = 0; i2 < 3; i2++) {
                            hostServer[i2] = newmeta.chunkServers.get(chunkNum*3 + (i2));
                            for (int i = 0; i < MServerServersThread.MserverThreads.length; i++) {
                                if (MServerServersThread.MserverThreads[i].getAddress().equals(hostServer[i2].address)) {
                                    servNum[i2] = i;
                                    foundServ[i2] = true;
                                }
                            }
                        }

                        if( (MServer.serverDown[servNum[0]] || !foundServ[0]) &&  (MServer.serverDown[servNum[1]] || !foundServ[1])
                        && (MServer.serverDown[servNum[2]] || !foundServ[2]) ) {
                            System.err.println("All chunk's servers are down.");
                            out.println("DOWN");
                        } else {
                            for (int i2 = 0; i2 < 3; i2++) {
                                if (!MServer.serverDown[servNum[i2]] && foundServ[i2]) {
                                    System.out.println("Equivalent to chunk " + newmeta.chunkNames.get(chunkNum * 3 + i2) + " on " + hostServer[i2].address + " at offset " + chunksOffset);

                                    if (!MServer.serverDown[servNum[i2]]) {
                                        //SEND INFO TO CLIENT
                                        out.println("SERVER");
                                        //send host and port
                                        out.println(hostServer[i2].address); //concatenate newline char so client can use readline
                                        //out.println(String.valueOf(hostServer.port));

                                        out.println("CHUNKNAME");
                                        //send chunk's name
                                        out.println(newmeta.chunkNames.get(chunkNum * 3 + i2));

                                        out.println("OFFSET");
                                        //send offset in chunk
                                        out.println(chunksOffset);

                                        out.println("BREAK");
                                        foundMeta = false;
                                        foundServ[i2] = false;
                                        break;
                                    }
                                } else { //only possible if we have metadata of a server that doesn't match our server connections, which shouldn't happen
                                    out.println("ERROR");
                                    System.err.println("Requested metadata's server not found.");
                                }
                            }
                        }
                    } else {
                        out.println("ERROR");
                        System.err.println("Requested file's metadata not found.");
                    }


                } else if (inputLine.toUpperCase().equals("WRITE") || inputLine.toUpperCase().equals("APPEND")) {
                    filename = input.readLine();
                    offset = input.readLine();  //Must be < 2048, checked at client side

                    if (!(Integer.parseInt(offset) < 2048)) {
                        System.err.println("Write offset greater than 2048 bytes.");
                    } else {
                        //DETERMINE APPROPRIATE CHUNK
                        for (int i = 0; i < allFiles.size(); i++) {
                            newmeta = allFiles.get(i);
                            if (newmeta.filename.equals(filename)) {
                                foundMeta = true;
                                break;
                            }
                        }

                        if (foundMeta) {
                            //Get Chunk's Servers' machines
                            iter = newmeta.chunkServers.iterator();
                            while (iter.hasNext()) {
                                hostServer[0] = hostServer[1];
                                hostServer[1] = hostServer[2];
                                hostServer[2] = iter.next();
                            }

                            for(int i2 = 0; i2 < 3; i2++) {
                                //find server's number
                                for (int i = 0; i < MServerServersThread.MserverThreads.length; i++) {
                                    if (MServerServersThread.MserverThreads[i].getAddress().equals(hostServer[i2].address)) {
                                        servNum[i2] = i;
                                        foundServ[i2] = true;
                                    }
                                }
                            }

                            //for updating time
                            lengthIter = newmeta.lastUpdate.iterator();
                            while (lengthIter.hasNext()) {
                                updateTime[0] = updateTime[1];
                                updateTime[1] = updateTime[2];
                                updateTime[2] = lengthIter.next();
                            }

                            //most up to date chunk is the longest, will be placed in chunksize[0]
                            lengthIter = newmeta.chunkLength.iterator();
                            while (lengthIter.hasNext()) {
                                chunksize[0] = chunksize[1];
                                chunksize[1] = chunksize[2];
                                chunksize[2] = lengthIter.next();
                            }
                            if(chunksize[0].getLong() < chunksize[1].getLong()) {
                                chunksize[0] = chunksize[1];
                            }
                            if(chunksize[0].getLong() < chunksize[2].getLong()) {
                                chunksize[0] = chunksize[2];
                            }

                            //get the name of the chunk (same in repetitions of 3)
                            nameIter = newmeta.chunkNames.iterator();
                            while (nameIter.hasNext()) {
                                chunkname = nameIter.next();
                            }

                            if (chunksize[0].getLong() != -1) { //chunksize is -1 if we didn't find the most up to date chunk
                                if (((long) 8192 - chunksize[0].getLong()) < Long.valueOf(offset)) { //If too large, need new chunk
                                    //RANDOMLY DECIDE SERVERS
                                    serv[0] = (rand.nextInt() & Integer.MAX_VALUE) % server.NUM_SERVERS; //pick random server
                                    while (MServer.serverDown[serv[0]]) {           //if server is down, need to pick another server
                                        serv[0] = (rand.nextInt() & Integer.MAX_VALUE) % server.NUM_SERVERS;
                                    }
                                    serv[1] = (serv[0]+1) % server.NUM_SERVERS;
                                    serv[2] = (serv[1]+1) % server.NUM_SERVERS;

                                    for(int i = 0; i < 3; i++) {
                                        MServerServersThread.MserverThreads[servNum[i]].padWithNull(chunkname); //PAD CHUNKS WITH NULL CHARACTER
                                    }
                                    //DECIDE NEW CHUNKNAME
                                    chunkname = localAddress + String.valueOf(localPort) + String.valueOf(file); //REPLACE OLD CHUNK NAME WITH NEW
                                    file++;
                                }
                            }

                            current = System.currentTimeMillis(); //get current time (for all chunk replicas)

                            for(int i2 = 0; i2 < 3; i2++) {
                                if (foundServ[i2]) {
                                        //If the current size of last chunk of the file is S such that 8192−S < appended data size, then the
                                        //rest of that chunk is padded with a null character, and a new chunk is created for the append operation
                                        if (chunksize[0].getLong() != -1) {
                                            if (((long) 8192 - chunksize[0].getLong()) < Long.valueOf(offset)) { //If too large, need new chunk
                                                //ADD TO METADATA
                                                newmeta.chunkNames.add(chunkname);
                                                hostServer[i2] = new machine(MServerServersThread.MserverThreads[serv[i2]].getAddress(),
                                                        MServerServersThread.MserverThreads[serv[i2]].getPort());           //REPLACE OLD CHUNK INFO WITH NEW CHUNK
                                                newmeta.chunkServers.add(hostServer[i2]);
                                                newmeta.lastUpdate.add(new MutableLong(current));
                                                newmeta.chunkLength.add(new MutableLong(Long.valueOf(offset)));     //new chunk is gonna have data written to it

                                                //TELL SERVER TO CREATE CHUNK
                                                MServerServersThread.MserverThreads[serv[i2]].createChunk(newmeta);
                                            } else {
                                                chunksize[i2].setLong(chunksize[i2].getLong() + Long.valueOf(offset));    //update length of chunk client is writing to
                                                updateTime[i2].setLong(current); //Update time
                                            }

                                            if (!MServer.serverDown[servNum[i2]]) {
                                                //SEND INFO TO CLIENT
                                                out.println(hostServer[i2].address);
                                                System.out.println(hostServer[i2].address);
                                                out.println(chunkname);
                                                System.out.println(chunkname);
                                            }  else {
                                                out.println("DOWN");
                                                System.err.println("Requested file's server is down.");
                                            }
                                            //reset booleans for next request
                                            foundMeta = false;
                                            foundServ[i2] = false;

                                        } else {
                                            out.println("ERROR");
                                            System.err.println("Error checking chunk size");
                                        }
                                } else { //only possible if we have metadata of a server that doesn't match our server connections, which shouldn't happen
                                    out.println("ERROR");
                                    System.err.println("Requested file not found.");
                                }
                            }

                            System.out.println(input.readLine()); //wait for client confirmation of write finishing

                        } else {
                            out.println("ERROR");
                            System.err.println("Requested file not found.");
                        }
                    }
                }
            }

            System.out.println("Client connection finished.");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

