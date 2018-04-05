package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.kv.KeyValueApi;

@RestController
@RequestMapping("/")
public class ExternalKeyValueApiController extends AbstractKeyValueApiController {
    public ExternalKeyValueApiController(@Qualifier("external") final KeyValueApi keyValueApi) {
        super(keyValueApi);
    }
}
