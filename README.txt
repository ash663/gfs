(commands from outside the directory)
Compile with: javac mserver/MServer.java
Dependencies will compile the other files.

Run Mserver, servers and clients on separate machines, in that order.

Assumed 5 servers and 2 clients. To change these parameters change the appropriate variables
(server.NUM_SERVERS, client.NUM_CLIENTS), and recompile completely.

Run Mserver: java mserver/MServer <servers' port number> <clients' port number>
Run server: java mserver/server <clients' port number> <MServer host> <MServer port>
Run client: java mserver/client <MServerhost> <MServerport> <server1host> <server1port> ... <serverNport>
