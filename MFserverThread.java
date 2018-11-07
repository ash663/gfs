package mserver;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;

public class MFserverThread extends Thread {
    private Socket socket = null;
    private boolean isDown = false;
    public final Object statusLock = new Object();
    public final Object missesLock = new Object();
    ArrayList<metadata> allFiles;
    String servAddr;
            //locAddr;
    int servPort;
    ObjectOutputStream out;
    ObjectInputStream ois;
    long downTime;
    long upTime;

    public MFserverThread(Socket socket, ArrayList<metadata> allFiles) {
        this.socket = socket;
        this.servAddr = socket.getInetAddress().getHostAddress();
        //this.locAddr = socket.getLocalAddress().getHostAddress();
        this.servPort = socket.getPort();
        this.allFiles = allFiles;
    }

    public boolean getStatus() {
        synchronized (statusLock) {
            return isDown;
        }
    }

    public String getAddress() {
        return this.servAddr;
    }

    public int getPort() {
        return this.servPort;
    }

    public void run() {
        while (socket == null) {
        }
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());
            //BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            Object input;
            boolean found = false;
            metadata metaIn, currentMeta;
            MutableLong lastBeat = new MutableLong(System.currentTimeMillis());
            MutableInt misses = new MutableInt();
            String[] tokens;
            int meta = 0;
            int index;
            Iterator<MutableLong> lengthIter;
            MutableLong[] chunksize = new MutableLong[3];
            String recoveryAddress;

            //START CHECKING FOR HEARTBEATS
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new checkHeartbeat(lastBeat, misses), 0, 5000); //creates a check every 5 seconds (5000 ms)

            while ((input = ois.readObject()) != null) {
                long current = System.currentTimeMillis();
                lastBeat.setLong(current); //set time of last heartbeat
                synchronized (missesLock) {
                    misses.setMisses(0); //reset number of missed messages
                }
                synchronized (statusLock) {
                    if (isDown) {
                        System.out.println("Server recovery starting.");
                        isDown = false; //reset server to active
                        upTime = current;


                        MutableLong metaTime;
                        for (metadata currentMeta2 : allFiles) {
                            meta = 0;

                            //get chunksize of all replicas of last chunk
                            lengthIter = currentMeta2.chunkLength.iterator();
                            while (lengthIter.hasNext()) {
                                chunksize[0] = chunksize[1];
                                chunksize[1] = chunksize[2];
                                chunksize[2] = lengthIter.next();
                            }

                            //get longest chunksize (most up to date)
                            if (chunksize[0].getLong() >= chunksize[1].getLong() && chunksize[0].getLong() >= chunksize[2].getLong()) {
                                index = 0;
                            } else if (chunksize[1].getLong() >= chunksize[0].getLong() && chunksize[1].getLong() >= chunksize[2].getLong()) {
                                index = 1;
                            } else {
                                index = 2;
                            }

                            recoveryAddress = currentMeta2.chunkServers.get(currentMeta2.chunkServers.size() - 1 - (2 - index)).address;

                            for (int i = 0; i < currentMeta2.lastUpdate.size(); i = i + 3) {
                                metaTime = currentMeta2.lastUpdate.get(i);

                                if (metaTime.getLong() > downTime) {

                                    for (int i2 = 0; i2<MServerServersThread.MserverThreads.length; i2++) {
                                        if (MServerServersThread.MserverThreads[i2].servAddr.equals(recoveryAddress) ) {
                                            MServerServersThread.MserverThreads[i2].read(currentMeta2.chunkNames.get(i), servAddr);
                                            break;
                                        }
                                    }

                                }
                            }
                        }
                    }
                }

                if (input instanceof metadata) {
                    metaIn = (metadata) input;
                    if (allFiles.contains(metaIn)) {
                        //if we're not updating time on receiving heartbeat, don't do anything
                        /*
                        currentMeta = allFiles.get(allFiles.indexOf(metaIn));
                        for(int i = 0; i < currentMeta.chunkServers.size(); i++){
                            //if(currentMeta.chunkServers.get(i).address.equals(servAddr) && currentMeta.chunkServers.get(i).port == servPort) {//server should only be sending metadata on its files, so should always be true
                                currentMeta.lastUpdate.get(i).setLong(System.currentTimeMillis());  //update last known time of chunk-server mapping, for this server
                            //}else{
                            //    System.err.println("Mserver received metadata from a server not about file at that server");
                            //}
                        }*/
                    } else { //if not in metadata not in our list because new file //or different times
                        for (int i = 0; i < allFiles.size(); i++) {
                            currentMeta = allFiles.get(i);
                            if (currentMeta.filename.equals(metaIn.filename)) {
                                //if we aren't updating time, we know most recent time. If they're behind, they'll update later.
                                /*
                                if(!currentMeta.lastUpdate.equals(metaIn.lastUpdate)) { //if times not equal either we need to update our time, or they haven't yet
                                    for(int i2 = 0; i2 < currentMeta.chunkServers.size(); i2++){
                                        if(currentMeta.chunkServers.get(i2).address.equals(servAddr) && currentMeta.chunkServers.get(i2).port == servPort) {//server should only be sending metadata on its files, so should always be true
                                            if(currentMeta.lastUpdate.get(i2).getLong() < metaIn.lastUpdate.get(i2).getLong()) { //if we need to update time, which should never happen
                                                currentMeta.lastUpdate.get(i2).setLong(System.currentTimeMillis());  //update last known time of chunk-server mapping, for this server
                                            }else if(currentMeta.lastUpdate.get(i2).getLong() > metaIn.lastUpdate.get(i2).getLong()) {
                                                //if they need to update time, they haven't gotten the message we sent them yet. don't need to do anything, they'll update eventually
                                            }else {
                                                //if they are equal, don't need to do anything
                                            }
                                        }else{
                                            System.err.println("Mserver received metadata from a server not about file at that server");
                                        }
                                    }
                                }else {
                                */ //Mserver decides on new chunkcreation/server placement, so will have most uptodate chunkNames/Servers. They'll update eventually
                                // }

                                found = true;
                            }
                        }
                        if (!found) {//if there's no entry for that file, add it
                            allFiles.add(metaIn); //should only be true at the start of program, when creating files
                        } else {
                            found = false; //otherwise, reset checking variable
                        }
                    }
                } else if (input instanceof String) {
                    tokens = ((String) input).split("`");

                    if (tokens[0].equals("RECOVERYWRITE")) {
                        System.out.println(tokens[0]);
                        for (int i = 0; i < MServerServersThread.MserverThreads.length; i++) {
                            if (MServerServersThread.MserverThreads[i].getAddress().equals(tokens[1])) {
                                MServerServersThread.MserverThreads[i].write(tokens[2], tokens[3]);
                            }
                        }
                    } else if (((String) input).toUpperCase().equals("KEEPALIVE")) {
                        //System.out.println("KEEPALIVE received");
                    } else {
                        System.err.println("Mserver received non-metadata from server.");
                    }
                } else {
                    System.err.println("Mserver received non-metadata from server.");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    class checkHeartbeat extends TimerTask { //check if heartbeat msgs have been missed. 3 implies server has gone down.
        MutableLong lastBeat;
        MutableInt misses;

        checkHeartbeat(MutableLong lastBeat, MutableInt misses) {
            this.lastBeat = lastBeat;
            this.misses = misses;
        }

        public void run() {
            long current = System.currentTimeMillis();
            if (current >= lastBeat.getLong() + (5000)) { //if at least 5 seconds (5000 millisecs) have passed since last heartbeat
                synchronized (missesLock) {
                    misses.setMisses(misses.getMisses() + 1); //increment the number of missed heartbeats
                    if (misses.getMisses() == 3) { //check if it has missed 3 or more heartbeats
                        synchronized (statusLock) {
                            isDown = true;  //if so, say server is down
                            downTime = current;
                            System.out.println("Server is down.");
                        }
                    }
                }
            }
        }
    }

    //FUNCTION CALLED BY MSERVER TO TELL SERVER TO CREATE A FILE CHUNK
    public String createChunk(metadata meta) {
        try {
            System.out.println("Sending chunk create to " + this.servAddr);
            out.writeObject(meta);
            out.flush();
            return "DONE";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "FAILED";
    }


    public void padWithNull(String chunkname) {
        try {
            System.out.println("Sending pad chunk " + chunkname + " to " + this.servAddr);
            out.writeObject("PAD");
            out.flush();
            out.writeObject(chunkname);
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void read(String chunkname, String serverAddress) {
        try {
            out.writeObject("READ");
            out.flush();

            out.writeObject(serverAddress);
            out.flush();

            out.writeObject(chunkname); //send chunkname
            out.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void write(String chunkname, String data) {
        try {
            out.writeObject("WRITE");
            out.flush();

            out.writeObject(chunkname);
            out.flush();

            out.writeObject(data);
            out.flush();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
