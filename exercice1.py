from pyspark import SparkContext

sc = SparkContext("local", "VentesParVille")

lines = [
    "date ville produit prix",
    "2023-01-05 Casablanca PC 4000",
    "2023-02-12 Rabat Smartphone 2500",
    "2024-03-10 Marrakech Imprimante 1200",
    "2023-05-20 Casablanca Écran 1500",
    "2024-01-01 Rabat Clavier 400"
]

# Sauvegarde dans un fichier
with open("ventes.txt", "w") as f:
    for line in lines:
        f.write(line + "\n")

rdd = sc.textFile("ventes.txt")
header = rdd.first()
data = rdd.filter(lambda line: line != header)

ventes_par_ville = data.map(lambda l: l.split()) \
                       .map(lambda champs: (champs[1], float(champs[3]))) \
                       .reduceByKey(lambda x, y: x + y)

for ville, total in ventes_par_ville.collect():
    print(f"{ville} : {total} MAD")