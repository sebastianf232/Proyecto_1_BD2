
import datetime

from neo4j import GraphDatabase
import datetime


def menu():
    print("BIENVENIDO AL RECOMENDADOR DE PELICULAS")
    print("Ingresa tus gustos y preferencias a continuación:")


def validar_fecha(fecha_texto):
    try:
        datetime.datetime.strptime(fecha_texto, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def procesar_entrada_lista(entrada, capitalizar=False):
    lista = entrada.split(',')
    if capitalizar:
        # Capitalizar la primera letra de cada palabra en cada elemento de la lista.
        return [item.strip().title() for item in lista]
    else:
        return [item.strip() for item in lista]

def variables_recomendacion():
    genero = procesar_entrada_lista(input("Ingresa los géneros que te gustan, separados por comas: "), True)
    
    while True:
        released = input("Ingresa la fecha de estreno que prefieres (formato YYYY-MM-DD): ").strip()
        if validar_fecha(released):
            break
        print("Formato de fecha incorrecto, por favor intenta de nuevo.")
    
    while True:
        duracion = input("Ingresa la duración mínima que prefieres para la película (en minutos): ").strip()
        if duracion.isdigit():
            duracion = int(duracion)
            break
        print("Por favor ingresa un número válido.")
    
    keywords = procesar_entrada_lista(input("Ingresa palabras clave relacionadas con la película, separadas por comas: "))
    studio = input("Ingresa el estudio de cine que prefieres: ").strip().title()
    director = input("Ingresa el nombre del director que te gustaría ver: ").strip().title()
    actors = procesar_entrada_lista(input("Ingresa los nombres de los actores que te gustan, separados por comas: "), True)

    return genero, released, duracion, keywords, studio, director, actors


#############################################

def neo4j_connect(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

def get_movie_recommendations(driver, genero, released, duracion, keywords, studio, director, actors):
    # Ajustar el rango de fecha
    year = int(released.split("-")[0])
    min_year = year - 5
    max_year = year + 5
    
    # Ajustar el rango de duración
    min_runtime = duracion - 30
    max_runtime = duracion + 30
    
    query = """
    MATCH (m:Movie)<-[:ACTED_IN]-(a:Actor), (m)-[:IS_TYPE]->(g:Genre), (m)<-[:DIRECTED_BY]-(d:Director), (m)<-[:PRODUCED_BY]-(c:Company)
    WHERE 
        g.Name IN $genres AND
        m.Released >= $minYear AND m.Released <= $maxYear AND
        m.runtime >= $minRuntime AND m.runtime <= $maxRuntime AND
        any(keyword IN m.keywords WHERE keyword IN $keywords) AND
        c.Name = $studio AND
        d.name = $director AND
        any(actor IN collect(a.name) WHERE actor IN $actors)
    RETURN m.title AS Title, m.Released AS Released, m.runtime AS Runtime
    LIMIT 10
    """
    
    with driver.session() as session:
        result = session.run(query, {
            'genres': genero,
            'minYear': min_year,
            'maxYear': max_year,
            'minRuntime': min_runtime,
            'maxRuntime': max_runtime,
            'keywords': keywords,
            'studio': studio,
            'director': director,
            'actors': actors
        })
        return list(result)

# Conexión a Neo4j
uri = "neo4j+s://<YOUR-URI>"
user = "<YOUR-USERNAME>"
password = "<YOUR-PASSWORD>"
driver = neo4j_connect(uri, user, password)

################### MAIN

menu()

# Recoger inputs del usuario
inputs = variables_recomendacion()
genero, released, duracion, keywords, studio, director, actors = inputs

# Obtener recomendaciones
recommendations = get_movie_recommendations(driver, *inputs)

#Imprimir recomendaciones
for movie in recommendations:
    print(movie['Title'], movie['Released'], movie['Runtime'])
############################################

