package ru.csc.bdse.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import ru.csc.bdse.app.pb10.PhoneBookKvNode;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.KeyValueApiHttpClient;

@SpringBootApplication
public class Application {
    public static final String KVNODE_URL = "KVNODE_URL";
    public static final String PHONEBOOK_VERSION = "PHONEBOOK_VERSION";

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    PhoneBookApi phoneBook() {
        String nodeUrl = System.getenv(KVNODE_URL);
        if (nodeUrl == null) {
            throw new IllegalArgumentException("Expected KVNODE_URI envvar");
        }
        KeyValueApi api = new KeyValueApiHttpClient(nodeUrl);

        String phonebookVersion = System.getenv(PHONEBOOK_VERSION);
        if (phonebookVersion == null) phonebookVersion = "1.0";
        if (phonebookVersion.equals("1.0")) {
            return new PhoneBookKvNode(api);
        } else {
            throw new IllegalArgumentException("Expected PHONEBOOK_VERSION envvar to be 1.0");
        }
    }
}
