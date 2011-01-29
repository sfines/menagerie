/*
 * Copyright 2010 Scott Fines
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.menagerie.collections;

import org.menagerie.Serializer;

import java.io.*;
import java.util.AbstractMap;
import java.util.Map;

/**
 * Convenience implementation of a Java-serialization-based Map.Entry Serializer.
 * <p>
 * This is equivalent to a JavaSerializer with a specific implementation of the Map.Entry interface, but this allows
 * the correct generic usage in Map implementations without requiring the addition of an extra method/interface just to
 * allow maps to serialize entries transparently.
 * <p>
 * This class is stateless, and therefore thread-safe.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 08-Jan-2011
 *          Time: 10:58:20
 */
public final class JavaEntrySerializer<K extends Serializable, V extends Serializable> implements Serializer<Map.Entry<K,V>> {

    @Override
    @SuppressWarnings({"unchecked"})
    public Map.Entry<K, V> deserialize(byte[] data) {
        try {
            ObjectInputStream inputStream  = new ObjectInputStream(new ByteArrayInputStream(data));
            return (AbstractMap.SimpleEntry<K,V>)inputStream.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            //should never happen, since AbstractMap.SimpleEntry is part of the JDK
            throw new RuntimeException(e);
        }
    }


    @Override
    public byte[] serialize(Map.Entry<K, V> instance) {
         try {
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            ObjectOutputStream arrayOutput = new ObjectOutputStream(byteArrayStream);
            arrayOutput.writeObject(instance);
            arrayOutput.flush();

            byte[] bytes = byteArrayStream.toByteArray();
            arrayOutput.close();
            return bytes;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
