package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;

import java.io.FileOutputStream;

/**
 * Created by 1403035 on 2014/7/15.
 */
public class BinaryMessageDecoder extends MessageDecoder<byte[], byte[]> {


    @Override
    public CamusWrapper<byte[]> decode(byte[] message) {

        return new CamusWrapper<byte[]>(message);
    }

}
