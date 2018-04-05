package ru.csc.bdse.app.pb10;

import ru.csc.bdse.app.pb10.proto.PhoneBook.RecordMessage;

import java.util.Collections;
import java.util.Set;

public class Record implements ru.csc.bdse.app.Record {
    final public RecordMessage data;

    Record(RecordMessage data) {
        this.data = data;
    }

    @Override
    public Long getUid() {
        return data.getUid();
    }

    @Override
    public Set<Character> literals() {
        String lastName = data.getLastName();
        if (lastName == null || lastName.isEmpty()) {
            return Collections.emptySet();
        } else {
            return Collections.singleton(lastName.charAt(0));
        }
    }
}
