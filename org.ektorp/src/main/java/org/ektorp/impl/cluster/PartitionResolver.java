package org.ektorp.impl.cluster;

import java.util.zip.CRC32;

public class PartitionResolver {

    private final int partitionCount;

    public PartitionResolver(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public int partitionIdFor(String id){
        CRC32 crc32 = new CRC32();
        crc32.update(id.getBytes());
        int crc32value = (int)crc32.getValue();
        int modulo = (crc32value >> 16) % partitionCount;
        if (modulo < 0){
            modulo += partitionCount;
        }
        return modulo;
    }

}
