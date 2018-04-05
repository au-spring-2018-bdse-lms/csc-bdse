package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.kv.KeyValueApi;

@RestController
@RequestMapping("/inner")
public class InnerKeyValueApiController extends AbstractKeyValueApiController {
    public InnerKeyValueApiController(@Qualifier("inner") KeyValueApi keyValueApi) {
        super(keyValueApi);
    }
}
