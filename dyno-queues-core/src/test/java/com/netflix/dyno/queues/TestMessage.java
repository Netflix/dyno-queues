/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.dyno.queues;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author Viren
 *
 */
public class TestMessage {

    @Test
    public void test() {
        Message msg = new Message();
        msg.setPayload("payload");
        msg.setTimeout(10, TimeUnit.SECONDS);
        assertEquals(msg.toString(), 10 * 1000, msg.getTimeout());
        msg.setTimeout(10);
        assertEquals(msg.toString(), 10, msg.getTimeout());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrioirty() {
        Message msg = new Message();
        msg.setPriority(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrioirty2() {
        Message msg = new Message();
        msg.setPriority(100);
    }
}
