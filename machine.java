package mserver;

import java.io.Serializable;

public class machine implements Serializable {
    String address;
    int port;

    machine(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof machine) {
            //o = (machine) o;
            if (((machine) o).address.equals(this.address) && ((machine) o).port == this.port) {
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
