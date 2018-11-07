package mserver;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class metadata implements Serializable{
    String filename;
    ArrayList<String> chunkNames = new ArrayList<>(); //Name of Linux files that correspond to the chunks of the file
    ArrayList<machine> chunkServers = new ArrayList<>(); //which server hosts which chunk,
    ArrayList<MutableLong> lastUpdate = new ArrayList<>(); //time when a chunk to server mapping was last updated.
    ArrayList<MutableLong> chunkLength;
    //long[] chunkLength;

    metadata(String filename, ArrayList<String> chunkNames, ArrayList<machine> chunkServers, ArrayList<MutableLong> lastUpdate, ArrayList<MutableLong> chunkLength){
        this.filename = filename;
        this.chunkNames = chunkNames;
        this.chunkServers = chunkServers;
        this.lastUpdate = lastUpdate;
        this.chunkLength = chunkLength;
    }

    metadata() {;}

    @Override
    public boolean equals(Object o) {
        if (o instanceof metadata) {
            //o = (metadata) o;
            if (((metadata) o).filename.equals(this.filename) && ((metadata) o).chunkNames.equals(this.chunkNames) &&
                    ((metadata) o).chunkServers.equals(this.chunkServers) && ((metadata) o).lastUpdate.equals(this.lastUpdate)
                    &&((metadata) o).chunkLength.equals(this.chunkLength)) {
                    //Arrays.equals(((metadata) o).chunkLength,this.chunkLength
                return true;
            }
            else {
                return false;
            }
        }else {
            return false;
        }
    }

}

