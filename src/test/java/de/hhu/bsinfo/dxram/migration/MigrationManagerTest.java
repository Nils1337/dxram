/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.migration;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class MigrationManagerTest {

    private static final long CHUNK_START_ID = 0x445800000000BEEFL;

    private static final long CHUNK_END_ID =   0x4458000000CAFFE1L;

    private static final long TOTAL_CHUNKS = CHUNK_END_ID - CHUNK_START_ID + 1;

    private static final int WORKER_COUNT = 16;

    private static final short TARGET = 42;

    private MigrationManager manager = null;

    @Mock
    private DXRAMComponentAccessor componentAccessor;

    @Test
    public void createTasks() {

        manager = new MigrationManager(WORKER_COUNT, componentAccessor);

        MigrationTask[] tasks = manager.createMigrationTasks(TARGET, CHUNK_START_ID, CHUNK_END_ID);

        assertEquals(WORKER_COUNT, tasks.length);

        int totalChunks = Arrays.stream(tasks)
                .map(MigrationTask::getChunkCount)
                .reduce(0, (a, b) -> a + b);

        assertEquals(TOTAL_CHUNKS, totalChunks);
    }

    @Test
    public void partitionChunks() {

        long[] chunkIds = LongStream.rangeClosed(CHUNK_START_ID, CHUNK_END_ID).toArray();

        long[][] partitionedChunks = MigrationManager.partitionChunks(chunkIds, WORKER_COUNT);

        assertEquals(WORKER_COUNT, partitionedChunks.length);

        int totalChunks = Arrays.stream(partitionedChunks)
                .map(a -> a.length)
                .reduce(0, (a, b) -> a + b);

        assertEquals(TOTAL_CHUNKS, totalChunks);
    }
}