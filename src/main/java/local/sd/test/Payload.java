package local.sd.test;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;

@Slf4j
class Payload {
    String getJson() throws IOException {
        ClassLoader classLoader = this.getClass().getClassLoader();
        File payloadFile = new File(Objects.requireNonNull(classLoader.getResource("payload.json")).getFile());
        log.info("Loaded payload file: {} bytes", payloadFile.length());
        return new String(Files.readAllBytes(payloadFile.toPath()));
    }
}
