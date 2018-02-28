package ru.csc.bdse;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.shaded.com.google.common.util.concurrent.Futures;


import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@RunWith(SpringRunner.class)
@SpringBootTest
@Ignore
public class ApplicationTests {

	@Test
	public void contextLoads() throws InterruptedException {
	}

}
