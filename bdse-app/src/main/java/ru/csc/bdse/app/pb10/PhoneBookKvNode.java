package ru.csc.bdse.app.pb10;

import ru.csc.bdse.app.PhoneBookKvNodeBase;
import ru.csc.bdse.app.pb10.proto.PhoneBook;
import ru.csc.bdse.kv.KeyValueApi;

public class PhoneBookKvNode extends PhoneBookKvNodeBase {
    PhoneBookKvNode(KeyValueApi keyValueApi) {
        super(keyValueApi, PhoneBook.RecordMessage.parser());
    }
}
