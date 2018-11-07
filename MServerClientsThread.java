package mserver;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Timer;

public class MServerClientsThread extends Thread {
    int numClientConnections = 0;
    int port;
    ArrayList<metadata> allFiles;

    MServerClientsThread(int port, ArrayList<metadata> allFiles) {
        this.port = port;
        this.allFiles = allFiles;
    }

    public void run() {

        System.out.println("Waiting for client connections on " + this.port);

        try {
            ServerSocket serverSocket = new ServerSocket(port); //open a server socket
            while (numClientConnections < client.NUM_CLIENTS) {
                new MClientThread(serverSocket.accept(), allFiles).start();
                numClientConnections++;
                System.out.println("ClientConnection number " + numClientConnections + " established");
            }

            System.out.println("Max ClientConnections established");
            serverSocket.close();

        } catch (Exception e) {
            System.err.println("Client-to-MServer socket establishment error");
            e.printStackTrace();
        }
    }
}