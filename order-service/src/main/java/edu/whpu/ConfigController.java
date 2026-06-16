package edu.whpu;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConfigController {

    @Value("${username:}")
    private String username;

    @GetMapping("/demo")
    public String getConfig() {
        return username;
    }
}
