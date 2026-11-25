package com.Distributedsystem.DFSS.algorithms;

import com.Distributedsystem.DFSS.entity.FilePart;
import com.Distributedsystem.DFSS.repository.FilePartRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MergeAlgorithmTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void writesChunksInOrderAndFallsBackToReplica() throws Exception {
        FilePartRepository repository = mock(FilePartRepository.class);
        MergeAlgorithm mergeAlgorithm = new MergeAlgorithm(repository);

        Path firstReplica = temporaryDirectory.resolve("first-replica");
        Path secondPrimary = temporaryDirectory.resolve("second-primary");
        Files.writeString(firstReplica, "first-");
        Files.writeString(secondPrimary, "second");

        List<FilePart> metadata = List.of(
                part(1, "Node1", temporaryDirectory.resolve("missing-primary")),
                part(1, "Node2", firstReplica),
                part(2, "Node1", secondPrimary)
        );
        when(repository.findByFileIdOrderByPartNumberAsc("42")).thenReturn(metadata);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        mergeAlgorithm.writeTo(2, 42, output);

        assertArrayEquals("first-second".getBytes(), output.toByteArray());
    }

    @Test
    void failsWhenChunkMetadataIsMissing() {
        FilePartRepository repository = mock(FilePartRepository.class);
        when(repository.findByFileIdOrderByPartNumberAsc("7")).thenReturn(List.of());

        MergeAlgorithm mergeAlgorithm = new MergeAlgorithm(repository);
        assertThrows(IOException.class,
                () -> mergeAlgorithm.writeTo(1, 7, new ByteArrayOutputStream()));
    }

    private FilePart part(int number, String node, Path path) {
        FilePart part = new FilePart();
        part.setFileId("42");
        part.setPartNumber(number);
        part.setNodeId(node);
        part.setPartPath(path.toString());
        return part;
    }
}
