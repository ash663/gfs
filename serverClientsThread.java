package mserver;
import java.net.ServerSocket;

public class serverClientsThread extends Thread {
    int numClientConnections;
    int port;

    public serverClientsThread(int port) {
        this.port = port;
    }

    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);//open a server socket
            while (numClientConnections < client.NUM_CLIENTS) {
                new serverClientThread(serverSocket.accept()).start();
                numClientConnections++;
                System.out.println("ClientConnection number " + numClientConnections + " established");
            }

            System.out.println("Max ClientConnections established");
            serverSocket.close();

        } catch (Exception e) {
            System.err.println("Client-to-server socket establishment error");
            e.printStackTrace();
        }
    }

}

