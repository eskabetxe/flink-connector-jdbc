/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.datasource.transactions.xa.xid;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static javax.transaction.xa.Xid.MAXBQUALSIZE;
import static javax.transaction.xa.Xid.MAXGTRIDSIZE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link XidImpl}. */
public class XaXidTest {
    public static final XidImpl XID =
            new XidImpl(1, randomBytes(MAXGTRIDSIZE), randomBytes(MAXBQUALSIZE));

    @Test
    void testXidsEqual() {
        XidImpl other =
                new XidImpl(
                        XID.getFormatId(), XID.getGlobalTransactionId(), XID.getBranchQualifier());
        assertThat(other).isEqualTo(XID).hasSameHashCodeAs(XID);
    }

    @Test
    void testXidsNotEqual() {
        assertThat(new XidImpl(0, XID.getGlobalTransactionId(), XID.getBranchQualifier()))
                .isNotEqualTo(XID);
        assertThat(
                        new XidImpl(
                                XID.getFormatId(),
                                randomBytes(MAXGTRIDSIZE),
                                XID.getBranchQualifier()))
                .isNotEqualTo(XID);
        assertThat(
                        new XidImpl(
                                XID.getFormatId(),
                                XID.getGlobalTransactionId(),
                                randomBytes(MAXBQUALSIZE)))
                .isNotEqualTo(XID);
    }

    private static byte[] randomBytes(int size) {
        return fillRandom(new byte[size]);
    }

    private static byte[] fillRandom(byte[] bytes) {
        new Random().nextBytes(bytes);
        return bytes;
    }
}
