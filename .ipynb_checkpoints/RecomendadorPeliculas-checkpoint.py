import datetime
from neo4j import GraphDatabase

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

def neo4j_connect(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

uri = "neo4j+ssc://4d9cb826.databases.neo4j.io"
user, password = ("neo4j", "_4X58M73ITpFpJVDUxHA2K1Bu92nWAUvuvpz4NzsFLU")
driver = neo4j_connect(uri, user, password)

try:
    driver.verify_connectivity()
    print("Conexión exitosa.")
except Exception as e:
    print(f"Error al conectar: {e}")

def get_movie_recommendations(driver, genero, released, duracion, keywords, studio, director, actors):
    # Convertir la fecha ingresada al año y crear rangos de años
    year = datetime.datetime.strptime(released, "%Y-%m-%d").year
    min_year = f"{year-10}-01-01"
    max_year = f"{year+10}-12-31"
    
    # Ajustar el rango de duración
    min_runtime = duracion - 30
    max_runtime = duracion + 30
    
    # Preparar la consulta Cypher
    query = """
    MATCH (m:Movie)
    OPTIONAL MATCH (m)-[:IS_TYPE]->(g:Genre)
    WITH m, COLLECT(g.Name) AS Genres
    OPTIONAL MATCH (m)<-[:DIRECTED_BY]-(d:Director)
    WITH m, Genres, COLLECT(d.Name) AS Directors
    OPTIONAL MATCH (m)<-[:PRODUCED_BY]-(c:Company)
    WITH m, Genres, Directors, COLLECT(c.Name) AS Companies
    OPTIONAL MATCH (m)<-[:ACTED_IN]-(a:Actor)
    WITH m, Genres, Directors, Companies, COLLECT(a.Name) AS Actors
    WITH m, Genres, Directors, Companies, Actors,
        ((CASE WHEN ANY(genre IN Genres WHERE genre IN $genres) THEN 1 ELSE 0 END) +
        (CASE WHEN $director IS NOT NULL AND ANY(director IN Directors WHERE director = $director) THEN 1 ELSE 0 END) +
        (CASE WHEN $studio IS NOT NULL AND ANY(company IN Companies WHERE company = $studio) THEN 1 ELSE 0 END) +
        (CASE WHEN ANY(actor IN Actors WHERE actor IN $actors) THEN 1 ELSE 0 END) +
        (CASE WHEN ANY(keyword IN split(m.Keyword, ',') WHERE keyword IN $keywords) THEN 1 ELSE 0 END)
        ) AS Score
    RETURN m AS Movie, Score, Genres, Directors, Companies, Actors
    ORDER BY Score DESC
    LIMIT 10

    """
    
    # Ejecutar la consulta
    with driver.session() as session:
        result = session.run(query, {
            'minYear': min_year,
            'maxYear': max_year,
            'minRuntime': min_runtime,
            'maxRuntime': max_runtime,
            'genres': genero,
            'keywords': keywords,
            'studio': studio,
            'director': director,
            'actors': actors
        })
        return list(result)




menu()
inputs = variables_recomendacion()
genero, released, duracion, keywords, studio, director, actors = inputs
recommendations = get_movie_recommendations(driver, *inputs)

for movie in recommendations:
    movie_info = movie['Movie']
    genres = ', '.join(movie['Genres'])  # 'Genres' es una lista de nombres de géneros
    directors = ', '.join(movie['Directors'])  # Similar para 'Directors'
    companies = ', '.join(movie['Companies'])  # Y 'Companies'
    actors = ', '.join(movie['Actors'])  # Y 'Actors'
    score = movie['Score']  # Acceder al puntaje calculado


    print(f"Title: {movie_info['Title']}")
    print(f"Released: {movie_info['Released']}")
    print(f"Runtime: {movie_info['Runtime']}")
    print(f"Genres: {genres}")
    print(f"Directors: {directors}")
    print(f"Companies: {companies}")
    print(f"Actors: {actors}")
    print(f"Score: {score}")  # Imprimir el puntaje de coincidencia
    print("--------------------------------------------------")
