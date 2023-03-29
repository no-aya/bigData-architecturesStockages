package ma.enset.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@NoArgsConstructor
@AllArgsConstructor @Getter
public class Employee implements Serializable {
    private Long id;
    private String name;

    private Long age;
    private String dept;
    private double salary;

}
