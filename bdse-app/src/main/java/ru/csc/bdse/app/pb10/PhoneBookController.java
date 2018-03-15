package ru.csc.bdse.app.pb10;

import com.google.protobuf.ByteString;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.csc.bdse.app.PhoneBookApi;
import ru.csc.bdse.app.pb10.proto.PhoneBook;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

@RestController
public class PhoneBookController {
    private final PhoneBookApi<Record> phoneBookApi;

    public PhoneBookController(PhoneBookApi<Record> phoneBookApi) {
        this.phoneBookApi = phoneBookApi;
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/phone-book/v1.0/{uid}")
    public void put(@PathVariable final long uid,
                    @RequestBody final byte[] firstNameBytes,
                    @RequestBody final byte[] lastNameBytes,
                    @RequestBody final byte[] phoneBytes) {
        phoneBookApi.put(new Record(
                PhoneBook.RecordMessage.newBuilder()
                .setUid(uid)
                .setFirstNameBytes(ByteString.copyFrom(firstNameBytes))
                .setLastNameBytes(ByteString.copyFrom(lastNameBytes))
                .setPhoneBytes(ByteString.copyFrom(phoneBytes))
                .build()));
    }

    @RequestMapping(method = RequestMethod.GET, value = "/phone-book/v1.0/search/{request}")
    public Set<Record> get(@PathVariable final char request) {
        return phoneBookApi.get(request);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/phone-book/v1.0/{uid}")
    public void delete(@PathVariable final long uid,
                    @RequestBody final byte[] firstNameBytes,
                    @RequestBody final byte[] lastNameBytes,
                    @RequestBody final byte[] phoneBytes) {
        phoneBookApi.delete(new Record(
                PhoneBook.RecordMessage.newBuilder()
                        .setUid(uid)
                        .setFirstNameBytes(ByteString.copyFrom(firstNameBytes))
                        .setLastNameBytes(ByteString.copyFrom(lastNameBytes))
                        .setPhoneBytes(ByteString.copyFrom(phoneBytes))
                        .build()));
    }
}
