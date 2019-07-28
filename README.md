# Spark CSV Test
Adresse du fichier de données de test : https://storage.googleapis.com/ebap-data/technical-test/data-engineer/xag.csv

On dispose d'un fichier CSV, selon le modèle suivant: 
input.csv : userId,itemId,rating,timestamp

On souhaite construire 3 CSV de la façon suivante: 
aggratings.csv : userIdAsInteger,itemIdAsInteger,ratingSum 
lookupuser.csv : userId,userIdAsInteger 
lookup_product.csv : itemId,itemIdAsInteger

où: 
userId : identifiant unique d'un utilisateur (String) 
itemId : identifiant unique d'un produit (String) 
rating : score (Float) 
timestamp : timestamp unix, nombre de millisecondes écoulées depuis 1970-01-01 minuit GMT (Long/Int64) 
userIdAsInteger : identifiant unique d'un utilisateur (Int) 
itemIdAsInteger : identifiant unique d'un produit (Int) 
ratingSum : Somme des ratings pour le couple utilisateur/produit (Float)

## Accessing the library

To start the App just run : 
```bash
scala com.github.hosnimed.spark.App `input_file.csv` `output_folder`
```
or
```bash
>sbt run 
```
* input_file.csv : default to `src/main/resources/xag.csv`
* output_folder : default to `src/main/resources`

## Documentation

*A link to the documentation*

## How to contribute

*How others can contribute to the project*