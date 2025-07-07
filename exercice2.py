from pyspark import SparkContext

sc = SparkContext("local", "VentesParVilleEtAnnee")
rdd = sc.textFile("ventes.txt")
header = rdd.first()
data = rdd.filter(lambda line: line != header)

def extraire_infos(line):
    parts = line.split()
    date = parts[0]
    annee = date.split("-")[0]
    ville = parts[1]
    prix = float(parts[3])
    return ((ville, annee), prix)

ventes_par_ville_annee = data.map(extraire_infos) \
                             .reduceByKey(lambda x, y: x + y)

for (ville, annee), total in ventes_par_ville_annee.collect():
    print(f"{ville} - {annee} : {total} MAD")