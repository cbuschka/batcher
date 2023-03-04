package io.github.cbuschka.batcher;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
class Item {
    private Long id;
    private String name;
}
