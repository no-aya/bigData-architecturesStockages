package org.example;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
public class Vente {
    String date;
    String ville;
    String produit;
    float prix;

}
