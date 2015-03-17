package org.mayo.camel.processor;

import org.apache.camel.util.IOHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public final class HBaseHelper {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseHelper.class);
    private static final Map<String, byte[]> NAMES = new HashMap<String, byte[]>();

    private HBaseHelper() {
        //Utility Class
    }

    public static byte[] getHBaseFieldAsBytes(String n) {
        byte[] name = null;
        name = NAMES.get(n);
        if (name == null) {
            name = n.getBytes();
            NAMES.put(n, name);
        }
        return name;
    }

    public static byte[] toBytes(Object obj) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        } else if (obj instanceof Byte) {
            return Bytes.toBytes((Byte) obj);
        } else if (obj instanceof Short) {
            return Bytes.toBytes((Short) obj);
        } else if (obj instanceof Integer) {
            return Bytes.toBytes((Integer) obj);
        } else if (obj instanceof Long) {
            return Bytes.toBytes((Long) obj);
        } else if (obj instanceof Double) {
            return Bytes.toBytes((Double) obj);
        } else if (obj instanceof String) {
            return Bytes.toBytes((String) obj);
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = null;
            try {
                oos = new ObjectOutputStream(baos);
                oos.writeObject(obj);
                return baos.toByteArray();
            } catch (IOException e) {
                LOG.warn("Error while serializing object. Null will be used.", e);
                return null;
            } finally {
                IOHelper.close(oos);
                IOHelper.close(baos);
            }
        }
    }

    public Object fromBytes(byte[] binary) {
        Object result = null;
        ObjectInputStream ois = null;

        if (binary == null) {
            return null;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(binary);
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            ois = new ObjectInputStream(bais) {
                @Override
                public Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                    try {
                        return classLoader.loadClass(desc.getName());
                    } catch (Exception e) {
                    }
                    return super.resolveClass(desc);
                }
            };
            result = ois.readObject();
        } catch (IOException e) {
            LOG.warn("Error while deserializing object. Null will be used.", e);
        } catch (ClassNotFoundException e) {
            LOG.warn("Could not find class while deserializing object. Null will be used.", e);
        } finally {
            IOHelper.close(ois);
            IOHelper.close(bais);
        }
        return result;
    }

}
