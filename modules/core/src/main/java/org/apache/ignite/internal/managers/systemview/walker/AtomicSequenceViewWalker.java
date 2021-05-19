/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.systemview.walker;

import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicSequenceView;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link AtomicSequenceView} attributes walker.
 * 
 * @see AtomicSequenceView
 */
public class AtomicSequenceViewWalker implements SystemViewRowAttributeWalker<AtomicSequenceView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "name", String.class);
        v.accept(1, "value", long.class);
        v.accept(2, "batchSize", long.class);
        v.accept(3, "groupName", String.class);
        v.accept(4, "groupId", int.class);
        v.accept(5, "removed", boolean.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(AtomicSequenceView row, AttributeWithValueVisitor v) {
        v.accept(0, "name", String.class, row.name());
        v.acceptLong(1, "value", row.value());
        v.acceptLong(2, "batchSize", row.batchSize());
        v.accept(3, "groupName", String.class, row.groupName());
        v.acceptInt(4, "groupId", row.groupId());
        v.acceptBoolean(5, "removed", row.removed());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 6;
    }
}
