package mserver;
import java.util.ArrayList;

public class MServer {
    static boolean[] serverDown = new boolean[server.NUM_SERVERS]; //keeps track of servers who've missed heartbeat msgs

    public static void main(String[] args) {
        if (args.length != 2) { //only argument should be listening ports of the MServer
            System.err.println("Usage: java mserver/MServer <servers' port number> <clients' port number> ");
            System.exit(1);
        }

        ArrayList<metadata> allFiles = new ArrayList<>(); //ArrayList for holding all metadata Mserver knows of

        if(Integer.parseInt(args[0]) == Integer.parseInt(args[1])){
            System.err.println("Server and Client Ports cannot be the same.");
            System.exit(2);
        }

        new MServerServersThread(Integer.parseInt(args[0]), allFiles).start();
        new MServerClientsThread(Integer.parseInt(args[1]), allFiles).start();


    }


}
