package ru.csc.bdse.kv;

import org.junit.Ignore;

/**
 * @author semkagtn
 */
@Ignore
public class InMemoryKeyValueApiTest extends AbstractKeyValueApiTest {

    @Override
    protected KeyValueApi newKeyValueApi() {
        return new InMemoryKeyValueApi("node");
    }
}
