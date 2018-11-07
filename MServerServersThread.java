package mserver;
import java.net.ServerSocket;
import java.util.ArrayList;

public class MServerServersThread extends Thread {
    int numServConnections =0;
    int port;
    static MFserverThread[] MserverThreads = new MFserverThread[server.NUM_SERVERS];
    ArrayList<metadata> allFiles;

    MServerServersThread(int port, ArrayList<metadata> allFiles) {
        this.port = port;
        this.allFiles = allFiles;
    }

    public void run() {

        System.out.println("Waiting for server connections on " + this.port);

        try {
            ServerSocket serverSocket = new ServerSocket(port); //open a server socket
            while (numServConnections < server.NUM_SERVERS) {
                MserverThreads[numServConnections] = new MFserverThread(serverSocket.accept(), allFiles);
                MserverThreads[numServConnections].start();
                numServConnections++;
                System.out.println("ServConnection number " + numServConnections + " established");
            }

            System.out.println("Max ServConnections established");
            serverSocket.close();

            while(true) {
                for (int i = 0; i < server.NUM_SERVERS; i++) {
                    MServer.serverDown[i] = MserverThreads[i].getStatus();
                }
            }

        } catch (Exception e) {
            System.err.println("Server-to-MServer socket establishment error");
            e.printStackTrace();
        }
    }

}
