#conexiones a aura
from neo4j import GraphDatabase
uri = "neo4j+ssc://1e48c053.databases.neo4j.io"
auth= ("neo4j", "R_ro7G4XS1ixvmtnxHX5_Kb0pXxZ_Lohg91TPLoaHZQ")

driver = GraphDatabase.driver(uri=uri, auth=auth)

def execute_query(query, parameters={}):
    with driver.session() as session:
        return session.run(query, parameters).data()
    
#un solo label
def crear_nodo(uri, auth, label, propiedades):
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        resultado = session.run(f"CREATE (n:{label} $props) RETURN n", props=propiedades)
        nodo = resultado.single()[0]
    driver.close()
    return nodo

#2 o más labels
def crear_nodo_con_labels(uri, auth, labels, propiedades):
    # Conecta a la base de datos
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        labels_str = ":" + ":".join(labels)
        query = f"CREATE (n{labels_str} $props) RETURN n"
        resultado = session.run(query, props=propiedades)
        nodo = resultado.single()[0]
    driver.close()
    return nodo

def consultar_nodos(uri, auth, label=None, filtros=None, solo_uno=False):
    """
    :param label: Label del nodo (opcional). Si no se especifica, buscará nodos sin filtrar por label.
    :param filtros: Diccionario con propiedades para filtrar. Ej: {"nombre": "Juan", "edad": 30}
    :param solo_uno: Si es True, retorna solo el primer nodo que coincida con los filtros. 
                     Si es False, retorna todos los nodos que coincidan
    """
    
    driver = GraphDatabase.driver(uri, auth=auth)
    
    with driver.session() as session:
        # Construimos la parte inicial del query: MATCH (n:Label)
        query = "MATCH (n"
        if label:
            query += f":{label}"
        query += ")"
        
        # Si se especifican filtros, creamos condiciones en el WHERE
        if filtros:
            condiciones = []
            for key in filtros.keys():
                # Genera algo como "n.nombre = $props.nombre"
                condiciones.append(f"n.{key} = $props.{key}")
            query += " WHERE " + " AND ".join(condiciones)
        
        # Decidimos si queremos un único nodo o todos
        if solo_uno:
            query += " RETURN n LIMIT 1"
        else:
            query += " RETURN n"
        
        # Ejecutamos el query, pasando las propiedades en 'props'
        resultado = session.run(query, props=filtros)
        
        if solo_uno:
            # Retornamos el primer nodo si existe
            registro = resultado.single()
            if registro:
                return registro["n"]
            else:
                return None
        else:
            # Retornamos una lista de todos los nodos encontrados
            nodos = []
            for registro in resultado:
                nodos.append(registro["n"])
            return nodos
    
    driver.close()


def agregar_propiedades_nodo_por_filtro(uri, auth, label, filtros, propiedades):
    """
    Agrega (o sobrescribe) propiedades en un único nodo que cumpla con el filtro.
    Si el filtro coincide con varios, solo se actualiza el primero.
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        # Construir la cláusula WHERE directamente si se proporcionan filtros
        where_clause = ""
        if filtros:
            condiciones = [f"n.{key} = $filtros.{key}" for key in filtros]
            where_clause = "WHERE " + " AND ".join(condiciones)
        
        query = f"""
        MATCH (n:{label})
        {where_clause}
        SET n += $props
        RETURN n
        LIMIT 1
        """
        resultado = session.run(query, filtros=filtros, props=propiedades)
        registro = resultado.single()
        driver.close()
        return registro["n"] if registro else None

def agregar_propiedades_nodos_por_filtro(uri, auth, label, filtros, propiedades):
    """
    Agrega (o sobrescribe) propiedades a todos los nodos que cumplan con el filtro.
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        where_clause = ""
        if filtros:
            condiciones = [f"n.{key} = $filtros.{key}" for key in filtros]
            where_clause = "WHERE " + " AND ".join(condiciones)
        
        query = f"""
        MATCH (n:{label})
        {where_clause}
        SET n += $props
        RETURN n
        """
        resultado = session.run(query, filtros=filtros, props=propiedades)
        nodos = [registro["n"] for registro in resultado]
        driver.close()
        return nodos
    
def actualizar_propiedades_nodo_por_filtro(uri, auth, label, filtros, propiedades):
    """
    Actualiza (o agrega) propiedades en un único nodo que cumpla con el filtro.
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        where_clause = ""
        if filtros:
            condiciones = [f"n.{key} = $filtros.{key}" for key in filtros]
            where_clause = "WHERE " + " AND ".join(condiciones)
        
        query = f"""
        MATCH (n:{label})
        {where_clause}
        SET n += $props
        RETURN n
        LIMIT 1
        """
        resultado = session.run(query, filtros=filtros, props=propiedades)
        registro = resultado.single()
        driver.close()
        return registro["n"] if registro else None


def actualizar_propiedades_nodos_por_filtro(uri, auth, label, filtros, propiedades):
    """
    Actualiza (o agrega) propiedades en todos los nodos que cumplan con el filtro.
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        where_clause = ""
        if filtros:
            condiciones = [f"n.{key} = $filtros.{key}" for key in filtros]
            where_clause = "WHERE " + " AND ".join(condiciones)
        
        query = f"""
        MATCH (n:{label})
        {where_clause}
        SET n += $props
        RETURN n
        """
        resultado = session.run(query, filtros=filtros, props=propiedades)
        nodos = [registro["n"] for registro in resultado]
        driver.close()
        return nodos
    

def eliminar_propiedades_nodo_por_filtro(uri, auth, label, filtros, lista_propiedades):
    """
    Elimina propiedades específicas de un único nodo que cumpla con el filtro.
    Si el filtro coincide con varios, solo se elimina de uno.
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        where_clause = ""
        if filtros:
            condiciones = [f"n.{key} = $filtros.{key}" for key in filtros]
            where_clause = "WHERE " + " AND ".join(condiciones)
        
        query = f"""
        MATCH (n:{label})
        {where_clause}
        FOREACH (key IN $keys | REMOVE n[key])
        RETURN n
        LIMIT 1
        """
        resultado = session.run(query, filtros=filtros, keys=lista_propiedades)
        registro = resultado.single()
        driver.close()
        return registro["n"] if registro else None

def eliminar_propiedades_nodos_por_filtro(uri, auth, label, filtros, lista_propiedades):
    """
    Elimina propiedades específicas de todos los nodos que cumplan con el filtro.
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        where_clause = ""
        if filtros:
            condiciones = [f"n.{key} = $filtros.{key}" for key in filtros]
            where_clause = "WHERE " + " AND ".join(condiciones)
        
        query = f"""
        MATCH (n:{label})
        {where_clause}
        FOREACH (key IN $keys | REMOVE n[key])
        RETURN n
        """
        resultado = session.run(query, filtros=filtros, keys=lista_propiedades)
        nodos = [registro["n"] for registro in resultado]
        driver.close()
        return nodos

def crear_relacion_con_propiedades_por_filtro(
    uri, auth,
    label_a, filtros_a,
    label_b, filtros_b,
    tipo_relacion,
    propiedades_relacion
):
    """
    Crea una relación con un tipo y propiedades entre dos nodos existentes 
    buscados por filtros (en lugar de usar IDs internos).

    :param label_a: Label del primer nodo (p.ej. "Persona")
    :param filtros_a: Diccionario de propiedades para filtrar el primer nodo (p.ej. {"uuid": "abc123"})
    :param label_b: Label del segundo nodo (p.ej. "Persona")
    :param filtros_b: Diccionario de propiedades para filtrar el segundo nodo (p.ej. {"uuid": "xyz789"})
    :param tipo_relacion: Tipo de la relación (p.ej. "AMIGO_DE", "CONOCE_A", etc.)
    :param propiedades_relacion: Diccionario con **al menos 3** propiedades para la relación
    :return: El objeto de relación creado (neo4j.graph.Relationship) o None si no se encontró ningún par de nodos
    """

    # Verificamos que la relación tenga al menos 3 propiedades
    if len(propiedades_relacion) < 3:
        raise ValueError("Se requieren al menos 3 propiedades para la relación.")

    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        # Construimos la cláusula WHERE para cada nodo en función de los filtros
        where_clause_a = ""
        if filtros_a:
            condiciones_a = [f"a.{k} = $filtrosA.{k}" for k in filtros_a]
            where_clause_a = " AND ".join(condiciones_a)
        
        where_clause_b = ""
        if filtros_b:
            condiciones_b = [f"b.{k} = $filtrosB.{k}" for k in filtros_b]
            where_clause_b = " AND ".join(condiciones_b)
        
        # Unimos las condiciones en un WHERE final (si existen)
        if where_clause_a and where_clause_b:
            final_where = f"WHERE {where_clause_a} AND {where_clause_b}"
        elif where_clause_a:
            final_where = f"WHERE {where_clause_a}"
        elif where_clause_b:
            final_where = f"WHERE {where_clause_b}"
        else:
            final_where = ""

        # Creamos el query
        query = f"""
        MATCH (a:{label_a}), (b:{label_b})
        {final_where}
        CREATE (a)-[r:{tipo_relacion} $relProps]->(b)
        RETURN r
        """

        # Ejecutamos el query
        result = session.run(
            query,
            filtrosA=filtros_a,
            filtrosB=filtros_b,
            relProps=propiedades_relacion
        )
        
        # Obtenemos la primera relación creada (o None si no se creó ninguna)
        record = result.single()
        relacion_creada = record["r"] if record else None

    driver.close()
    return relacion_creada

def gestion_Relaciones():
    print("Que opcion desea?")
    print("1) Agregar propiedad a relacion especifica")
    print("2) Agregar propiedad a multiples relaciones")
    print("3) Actualizacion de propiedad de relacion especifica")
    print("4) Actualizacion de multiples propiedades")
    print("5) Eliminacion de una propiedad de relacion especifica")
    print("6) Eliminacion de varios propiedades")
    print("7) Salir")
    respuesta=int(input("INGRESE SU RESPUESTA: "))
    return respuesta


def add_empty_property_to_relationship(driver):
    print("Agregar una nueva propiedad a una relación específica")
    
    # Solicitar y validar la entrada para las etiquetas de los nodos y la relación
    relacion = input("Ingrese el tipo de relación que quiere editar (ej. ACTED_IN): ").strip()
    nodoA = input("Ingrese la etiqueta del nodo de salida (ej. Actor): ").strip()
    nodoB = input("Ingrese la etiqueta del nodo de llegada (ej. Movie): ").strip()
    propiedad = input("Ingrese el nombre de la nueva propiedad (ej. role): ").strip()
    
    # Establecer un valor predeterminado seguro
    default_value = ""  # Asumiendo que quieres establecer la propiedad a una cadena vacía

    query = f"""
    MATCH (a:`{nodoA}`)-[r:`{relacion}`]->(b:`{nodoB}`)
    SET r.`{propiedad}` = "{default_value}"
    RETURN count(r) as updatedCount
    """

    # Ejecutar la consulta
    with driver.session() as session:
        result = session.run(query)
        updated_count = result.single()[0]
        print(f"Propiedades actualizadas con el valor por defecto '{default_value}': {updated_count}")




def remove_property_from_relationshipGENERAL(driver):
    print("Eliminar una propiedad de una relación específica")
    
    # Solicitar y validar la entrada para las etiquetas de los nodos y la relación
    relacion = input("Ingrese el tipo de relación de la que desea eliminar una propiedad: ").strip()
    nodoA = input("Ingrese la etiqueta del nodo de salida (ej. Actor): ").strip()
    nodoB = input("Ingrese la etiqueta del nodo de llegada (ej. Movie): ").strip()
    propiedad = input("Ingrese el nombre de la propiedad que desea eliminar (ej. role): ").strip()

    # Construir y ejecutar la consulta Cypher utilizando interpolación segura para nombres
    query = f"""
    MATCH (a:`{nodoA}`)-[r:`{relacion}`]->(b:`{nodoB}`)
    REMOVE r.`{propiedad}`
    RETURN count(r) as updatedCount
    """

    try:
        with driver.session() as session:
            result = session.run(query)
            updated_count = result.single()[0]
            print(f"Propiedad '{propiedad}' eliminada de {updated_count} relaciones.")
    except Exception as e:
        print(f"Error al intentar eliminar la propiedad: {e}")



def update_property_in_relationship(driver):
    print("Actualizar una propiedad en una relación específica")
    
    # Solicitar los datos necesarios del usuario
    nodoA = input("Ingrese la etiqueta del nodo de salida (ej. Person): ").strip()
    llaveNodoA = input("Ingrese la clave del nodo de salida (ej. id): ").strip()
    valorLlaveNodoA = input("Ingrese el valor de la clave del nodo de salida: ").strip()
    nodoB = input("Ingrese la etiqueta del nodo de llegada (ej. Movie): ").strip()
    llaveNodoB = input("Ingrese la clave del nodo de llegada (ej. id): ").strip()
    valorLlaveNodoB = input("Ingrese el valor de la clave del nodo de llegada: ").strip()
    nombreRelacion = input("Ingrese el tipo de relación (ej. ACTED_IN): ").strip()
    nombrePropiedad = input("Ingrese el nombre de la propiedad a actualizar (ej. role): ").strip()
    valorPropiedad = input("Ingrese el nuevo valor para la propiedad: ").strip()

    # Construir y ejecutar la consulta Cypher utilizando interpolación segura para los nombres y parámetros para los valores
    query = f"""
    MATCH (a:`{nodoA}`)-[r:`{nombreRelacion}`]->(b:`{nodoB}`)
    WHERE a.`{llaveNodoA}` = $valorLlaveNodoA AND b.`{llaveNodoB}` = $valorLlaveNodoB
    SET r.`{nombrePropiedad}` = $valorPropiedad
    RETURN a, r, b
    """

    try:
        with driver.session() as session:
            result = session.run(query, valorLlaveNodoA=valorLlaveNodoA, valorLlaveNodoB=valorLlaveNodoB, valorPropiedad=valorPropiedad)
            for record in result:
                print("Actualización realizada:", record["a"], record["r"], record["b"])
    except Exception as e:
        print(f"Error al intentar actualizar la propiedad: {e}")


def update_property_based_on_filter(driver):
    print("Actualizar una propiedad en una relación basada en un filtro específico")
    
    # Solicitar y validar la entrada para las etiquetas de los nodos y la relación
    nodoA = input("Ingrese la etiqueta del nodo de salida (ej. Person): ").strip()
    nodoB = input("Ingrese la etiqueta del nodo de llegada (ej. Movie): ").strip()
    nombreRelacion = input("Ingrese el tipo de relación (ej. ACTED_IN): ").strip()
    nombrePropiedad = input("Ingrese el nombre de la propiedad a actualizar (ej. role): ").strip()
    valorPropiedad = input("Ingrese el nuevo valor para la propiedad: ").strip()
    
    variableAFiltrar = input("Ingrese la variable por la cual filtrar (ej. age, rating): ").strip()
    operador = input("Ingrese el operador de comparación (ej. =, >, <, >=, <=, <>): ").strip()
    valorAFiltrar = input("Ingrese el valor del filtro para la variable especificada: ").strip()

    # Construir y ejecutar la consulta Cypher utilizando interpolación segura y parámetros
    query = f"""
    MATCH (a:`{nodoA}`)-[r:`{nombreRelacion}`]->(b:`{nodoB}`)
    WHERE a.`{variableAFiltrar}` {operador} $valorAFiltrar
    SET r.`{nombrePropiedad}` = $valorPropiedad
    RETURN a, r, b
    """

    try:
        with driver.session() as session:
            result = session.run(query, {'valorAFiltrar': valorAFiltrar, 'valorPropiedad': valorPropiedad})
            for record in result:
                print(f"Actualización realizada en la propiedad '{nombrePropiedad}' de los nodos relacionados:", record["a"], record["r"], record["b"])
    except Exception as e:
        print(f"Error al intentar actualizar la propiedad basada en el filtro: {e}")


def remove_property_from_relationship(driver):
    print("Eliminar una propiedad específica de una relación")

    # Solicitar los datos necesarios del usuario
    nodoA = input("Ingrese la etiqueta del nodo de salida (ej. Person): ").strip()
    llaveNodoA = input("Ingrese la clave del nodo de salida (ej. id): ").strip()
    valorLlaveNodoA = input("Ingrese el valor de la clave del nodo de salida: ").strip()
    nodoB = input("Ingrese la etiqueta del nodo de llegada (ej. Movie): ").strip()
    llaveNodoB = input("Ingrese la clave del nodo de llegada (ej. id): ").strip()
    valorLlaveNodoB = input("Ingrese el valor de la clave del nodo de llegada: ").strip()
    nombreRelacion = input("Ingrese el tipo de relación (ej. ACTED_IN): ").strip()
    nombrePropiedad = input("Ingrese el nombre de la propiedad que desea eliminar (ej. role): ").strip()

    # Construir y ejecutar la consulta Cypher utilizando interpolación segura para los nombres y parámetros para los valores
    query = f"""
    MATCH (a:`{nodoA}`)-[r:`{nombreRelacion}`]->(b:`{nodoB}`)
    WHERE a.`{llaveNodoA}` = $valorLlaveNodoA AND b.`{llaveNodoB}` = $valorLlaveNodoB
    REMOVE r.`{nombrePropiedad}`
    RETURN a, r, b
    """

    try:
        with driver.session() as session:
            result = session.run(query, valorLlaveNodoA=valorLlaveNodoA, valorLlaveNodoB=valorLlaveNodoB)
            updated_count = 0
            for record in result:
                updated_count += 1
                print("Propiedad eliminada:", record["a"], record["r"], record["b"])
            print(f"Total de relaciones actualizadas: {updated_count}")
    except Exception as e:
        print(f"Error al intentar eliminar la propiedad: {e}")



def add_property_to_relationship(driver):
    print("Agregar una nueva propiedad a una relación específica")

    # Solicitar los datos necesarios del usuario
    nodoA = input("Ingrese la etiqueta del nodo de salida (ej. Person): ").strip()
    llaveNodoA = input("Ingrese la clave del nodo de salida (ej. id): ").strip()
    valorLlaveNodoA = input("Ingrese el valor de la clave del nodo de salida: ").strip()
    nodoB = input("Ingrese la etiqueta del nodo de llegada (ej. Movie): ").strip()
    llaveNodoB = input("Ingrese la clave del nodo de llegada (ej. id): ").strip()
    valorLlaveNodoB = input("Ingrese el valor de la clave del nodo de llegada: ").strip()
    nombreRelacion = input("Ingrese el tipo de relación (ej. ACTED_IN): ").strip()
    nombrePropiedad = input("Ingrese el nombre de la nueva propiedad que desea agregar (ej. role): ").strip()
    valorPropiedad = input("Ingrese el valor de la nueva propiedad: ").strip()

    # Construir y ejecutar la consulta Cypher utilizando interpolación segura para los nombres y parámetros para los valores
    query = f"""
    MATCH (a:`{nodoA}`)-[r:`{nombreRelacion}`]->(b:`{nodoB}`)
    WHERE a.`{llaveNodoA}` = $valorLlaveNodoA AND b.`{llaveNodoB}` = $valorLlaveNodoB
    SET r.`{nombrePropiedad}` = $valorPropiedad
    RETURN a, r, b
    """

    try:
        with driver.session() as session:
            result = session.run(query, {'valorLlaveNodoA': valorLlaveNodoA, 'valorLlaveNodoB': valorLlaveNodoB, 'valorPropiedad': valorPropiedad})
            updated_count = 0
            for record in result:
                updated_count += 1
                print("Nueva propiedad agregada:", record["a"], record["r"], record["b"])
            print(f"Total de relaciones actualizadas: {updated_count}")
    except Exception as e:
        print(f"Error al intentar agregar la nueva propiedad: {e}")


def eliminar_un_nodo_por_filtro(uri, auth, label, filtros):
    """
    Elimina un único nodo que cumpla con el filtro, junto con sus relaciones.
    Retorna True si se eliminó el nodo, False si no se encontró ninguno.
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        # Construcción dinámica del WHERE
        where_clause = ""
        if filtros:
            condiciones = [f"n.{key} = $filtros.{key}" for key in filtros]
            where_clause = "WHERE " + " AND ".join(condiciones)
        
        query = f"""
        MATCH (n:{label})
        {where_clause}
        WITH n LIMIT 1
        DETACH DELETE n
        RETURN count(n) AS nodosEliminados
        """
        resultado = session.run(query, filtros=filtros)
        record = resultado.single()
        nodos_eliminados = record["nodosEliminados"] if record else 0
        
    driver.close()
    return nodos_eliminados > 0


def eliminar_multiples_nodos_por_labels_y_filtros(uri, auth, labels, filtros):
    """
    Elimina todos los nodos que tengan todos los labels en `labels`
    y que cumplan con las propiedades de `filtros`. También elimina 
    sus relaciones (DETACH). Retorna la cantidad de nodos eliminados.
    
    :param labels: Lista de labels (p.ej. ["Persona", "Empleado"]).
                   El nodo debe tener todas esas labels.
    :param filtros: Diccionario de propiedades para filtrar (p.ej. {"edad": 30}).
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        # Construir la parte de los labels, p.ej. :Persona:Empleado
        labels_str = ":" + ":".join(labels)  # ["Persona", "Empleado"] -> ":Persona:Empleado"

        # Construir la cláusula WHERE con las propiedades del diccionario
        where_clause = ""
        if filtros:
            condiciones = [f"n.{key} = $filtros.{key}" for key in filtros]
            where_clause = "WHERE " + " AND ".join(condiciones)
        
        query = f"""
        MATCH (n{labels_str})
        {where_clause}
        DETACH DELETE n
        RETURN count(n) AS nodosEliminados
        """
        
        resultado = session.run(query, filtros=filtros)
        record = resultado.single()
        nodos_eliminados = record["nodosEliminados"] if record else 0

    driver.close()
    return nodos_eliminados


def eliminar_una_relacion_por_filtro(
    uri, auth,
    label_a, filtros_a,
    tipo_relacion,
    label_b, filtros_b
):
    """
    Elimina una sola relación (la primera que coincida) entre dos nodos que cumplan los filtros.
    Retorna True si se eliminó alguna relación, False si no se encontró ninguna.
    
    - label_a, filtros_a: para identificar el nodo de origen
    - tipo_relacion: el tipo de la relación (ej: "AMIGO_DE")
    - label_b, filtros_b: para identificar el nodo de destino
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        where_clause_a = ""
        if filtros_a:
            condiciones_a = [f"a.{k} = $filtrosA.{k}" for k in filtros_a]
            where_clause_a = " AND ".join(condiciones_a)
        
        where_clause_b = ""
        if filtros_b:
            condiciones_b = [f"b.{k} = $filtrosB.{k}" for k in filtros_b]
            where_clause_b = " AND ".join(condiciones_b)

        # Unir condiciones en un único WHERE (si existen)
        if where_clause_a and where_clause_b:
            final_where = f"WHERE {where_clause_a} AND {where_clause_b}"
        elif where_clause_a:
            final_where = f"WHERE {where_clause_a}"
        elif where_clause_b:
            final_where = f"WHERE {where_clause_b}"
        else:
            final_where = ""
        
        query = f"""
        MATCH (a:{label_a})-[r:{tipo_relacion}]->(b:{label_b})
        {final_where}
        WITH r LIMIT 1
        DELETE r
        RETURN count(r) AS relacionesEliminadas
        """
        resultado = session.run(
            query,
            filtrosA=filtros_a,
            filtrosB=filtros_b
        )
        record = resultado.single()
        relaciones_eliminadas = record["relacionesEliminadas"] if record else 0

    driver.close()
    return relaciones_eliminadas > 0

def eliminar_multiples_relaciones_por_filtro(
    uri, auth,
    label_a, filtros_a,
    tipo_relacion,
    label_b, filtros_b
):
    """
    Elimina todas las relaciones que coincidan entre nodos que cumplan los filtros.
    Retorna la cantidad de relaciones eliminadas.
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        where_clause_a = ""
        if filtros_a:
            condiciones_a = [f"a.{k} = $filtrosA.{k}" for k in filtros_a]
            where_clause_a = " AND ".join(condiciones_a)
        
        where_clause_b = ""
        if filtros_b:
            condiciones_b = [f"b.{k} = $filtrosB.{k}" for k in filtros_b]
            where_clause_b = " AND ".join(condiciones_b)

        if where_clause_a and where_clause_b:
            final_where = f"WHERE {where_clause_a} AND {where_clause_b}"
        elif where_clause_a:
            final_where = f"WHERE {where_clause_a}"
        elif where_clause_b:
            final_where = f"WHERE {where_clause_b}"
        else:
            final_where = ""
        
        query = f"""
        MATCH (a:{label_a})-[r:{tipo_relacion}]->(b:{label_b})
        {final_where}
        DELETE r
        RETURN count(r) AS relacionesEliminadas
        """
        resultado = session.run(
            query,
            filtrosA=filtros_a,
            filtrosB=filtros_b
        )
        record = resultado.single()
        relaciones_eliminadas = record["relacionesEliminadas"] if record else 0

    driver.close()
    return relaciones_eliminadas


def primer_menu():
    print("¿Qué desea realizar?")
    print("1. Crear un nodo")
    print("2. Vizualizar nodos")
    print("3. Operaciones con nodos")
    print("4. Crear relaciones")
    print("5. Gestión de relaciones")
    print("6. Eliminar nodos")
    print("7. Eliminar relaciones")
    print("8. Salir")

def menu_nodos():
    print("¿Qué desesa realizar?")
    print("1. Creación de nodos con 1 label")
    print("2. Creación de nodos con 2 o más lables")
    res2 = input("Elija una opción (solo colocar el número de opción): ")

    if res2 == "1":
        print("Escriba el label del nodo que va a ingresar: ")
        label = input("")
        preg1 = input("¿Quiere agreagr propiedades? (Y/N) ")
        if preg1 == "Y":
            n = int(input("¿Cuántas propiedades quiere agregar? "))
            propiedades = {input("Propiedad: "): input("Valor: ") for i in range(n)}

        else: 
            propiedades = {}

        crear_nodo(uri, auth, label, propiedades)

    elif res2 == "2":
        n1 = int(input("¿Cuántos labels quiere ingresar? "))
        print("Escriba el nombre de los labeles del nodo que va a ingresar: ")
        labels = [input("Escriba el label ") for i in range(n1)]
        preg1 = input("¿Quiere agreagr propiedades? (Y/N) ")
        if preg1 == "Y":
            n = int(input("¿Cuántas propiedades quiere agregar? "))
            propiedades = {input("Propiedad: "): input("Valor: ") for i in range(n)}

        else: 
            propiedades = {}

        crear_nodo_con_labels(uri, auth, labels, propiedades)
        
    else:
        print("Escriba una opción válida")


def menu_consulta_nodos():
    res3 = input("¿Desea buscar por label? (Y/N) ").upper()
    res4 = input("¿Desea agregar filtros? (Y/N) ").upper()
    nodo = input("¿Desea consultar un nodo o todos los nodos (uno/varios)? ").lower()
    
    if res3 == "Y" and res4 == "Y" and nodo == "uno":
        label = input("Ingrese el label que quiere buscar: ")
        filtros = {input("Propiedad: "): input("Valor: ")}
        resultado = consultar_nodos(uri, auth, label, filtros, True)
        print("Resultado:", resultado)

    elif res3 == "Y" and res4 == "Y" and nodo == "varios":
        label = input("Ingrese el label que quiere buscar: ")
        filtros = {input("Propiedad: "): input("Valor: ")}
        resultado = consultar_nodos(uri, auth, label, filtros)
        print("Resultados:", resultado)

    elif res3 == "Y" and res4 == "N" and nodo == "uno":
        label = input("Ingrese el label que quiere buscar: ")
        resultado = consultar_nodos(uri, auth, label, filtros=None, solo_uno=True)
        print("Resultado:", resultado)

    elif res3 == "Y" and res4 == "N" and nodo == "varios":
        label = input("Ingrese el label que quiere buscar: ")
        resultado = consultar_nodos(uri, auth, label)
        print("Resultados:", resultado)

    elif res3 == "N" and res4 == "Y" and nodo == "uno":
        filtros = {input("Propiedad: "): input("Valor: ")}
        resultado = consultar_nodos(uri, auth, label=None, filtros=filtros, solo_uno=True)
        print("Resultado:", resultado)

    elif res3 == "N" and res4 == "Y" and nodo == "varios":
        filtros = {input("Propiedad: "): input("Valor: ")}
        resultado = consultar_nodos(uri, auth, label=None, filtros=filtros)
        print("Resultados:", resultado)

    elif res3 == "N" and res4 == "N" and nodo == "uno":
        resultado = consultar_nodos(uri, auth, label=None, filtros=None, solo_uno=True)
        print("Resultado:", resultado)

    elif res3 == "N" and res4 == "N" and nodo == "varios":
        resultado = consultar_nodos(uri, auth)
        print("Resultados:", resultado)
    
    else:
        print("Ingrese una opción válida")
            
def menu_operaciones_nodos():
    print("¿Qué desea realizar? ")
    print("1. Agregar 1 o más propiedades a un nodo")
    print("2. Agregar 1 o más propiedades a múltiples nodos")
    print("3. Actualizar 1 o más propiedades de un nodo")
    print("4. Actualizar 1 o más propiedades de múltiples nodos")
    print("5. Eliminar 1 o más propiedades de un nodo")
    print("6. Eliminar 1 o más propiedades de múltiples nodos")

    res5 = input("Escoge un número: ")
    if res5 == "1":
        nodo = input("Escribe el nombre del nodo al que le quieres agregar propiedades: ")
        preg_res = input("¿Desea buscar por filtro? (Y/N) ")
        if preg_res == "Y":
            print("Agregue las propiedades y el valor para encontrar el nodo: ")
            filtros = {input("Propiedad: "): input("Valor: ")}

        else:
            filtros = {}
            
        n = int(input("¿Cuántas propiedades quiere agregar?"))
        propiedades = {input("Propiedad: "): input("Valor: ") for i in range(n)}
        
        agregar_propiedades_nodo_por_filtro(uri, auth, nodo, filtros, propiedades)

    elif res5 == "2":
        nodos = input("Escribe el nombre del nodo al que le quieres agregar propiedades: ")
        preg_res = input("¿Desea buscar por filtro? (Y/N) ")
        if preg_res == "Y":
            print("Agregue las propiedades y el valor para encontrar el nodo: ")
            filtros = {input("Propiedad: "): input("Valor: ")}
        else:
            filtros = {}
            
        n = int(input("¿Cuántas propiedades quiere agregar?"))
        propiedades = {input("Propiedad: "): input("Valor: ") for i in range(n)}
        
        agregar_propiedades_nodos_por_filtro(uri, auth, nodos, filtros, propiedades)

    elif res5 == "3":
        nodo = input("Escribe el nombre del nodo al que le quieres actualizar propiedades: ")
        preg_res = input("¿Desea buscar por filtro? (Y/N) ")
        if preg_res == "Y":
            print("Agregue las propiedades y el valor para encontrar el nodo: ")
            filtros = {input("Propiedad: "): input("Valor: ")}

        else:
            filtros = {}
            
        n = int(input("¿Cuántas propiedades quiere actualizar?"))
        propiedades = {input("Propiedad: "): input("Valor: ") for i in range(n)}

        actualizar_propiedades_nodo_por_filtro(uri, auth, nodo, filtros, propiedades)

    elif res5 == "4":
        nodo = input("Escribe el nombre del nodo al que le quieres actualizar propiedades: ")
        preg_res = input("¿Desea buscar por filtro? (Y/N) ")
        if preg_res == "Y":
            print("Agregue las propiedades y el valor para encontrar el nodo: ")
            filtros = {input("Propiedad: "): input("Valor: ")}

        else:
            filtros = {}
        
        n = int(input("¿Cuántas propiedades quiere actualizar?"))
        propiedades = {input("Propiedad: "): input("Valor: ") for i in range(n)}

        actualizar_propiedades_nodos_por_filtro(uri, auth, nodo, filtros, propiedades)

    elif res5 == "5":
        nodo = input("Escribe el nombre del nodo al que le quieres eliminar propiedades: ")
        preg_res = input("¿Desea buscar por filtro? (Y/N) ")
        if preg_res == "Y":
            print("Agregue las propiedades y el valor para encontrar el nodo: ")
            filtros = {input("Propiedad: "): input("Valor: ")}

        else:
            filtros = {}
            
        n = int(input("¿Cuántas propiedades quiere eliminar?"))
        propiedades = [input("Propiedad: ") for i in range(n)]

        eliminar_propiedades_nodo_por_filtro(uri, auth, nodo, filtros, propiedades)

    elif res5 == "6":
        nodo = input("Escribe el nombre del nodo al que le quieres eliminar propiedades: ")
        preg_res = input("¿Desea buscar por filtro? (Y/N) ")
        if preg_res == "Y":
            print("Agregue las propiedades y el valor para encontrar el nodo: ")
            filtros = {input("Propiedad: "): input("Valor: ")}

        else:
            filtros = {}
        
        n = int(input("¿Cuántas propiedades quiere eliminar?"))
        propiedades = [input("Propiedad: ") for i in range(n)]
    
        eliminar_propiedades_nodos_por_filtro(uri, auth, nodo, filtros, propiedades)

def menu_crear_relaciones():
    nodo1 = input("Ingrese el nombre del primer nodo: ")
    res6 = input("¿Quiere filtrar este primer nodo? (Y/N) ").upper()
    nodo2 = input("Ingrese el nombre del segundo nodo: ")
    res7 = input("¿Quiere filtrar este segundo nodo? (Y/N) ").upper()
    tipo_relacion = input("Ingrese el tipo de relación que quiere crear (mayúsculas): ").upper()
    print("¿Cuántas propiedades quiere crear? (deben de ser al menos 3) ")
    n = int(input("¿Cuántas propiedades quiere crear?"))
    propiedades = {input("Propiedad: "): input("Valor: ") for i in range(n)}
    
    if res6 == "Y" and res7 == "Y":
        print("Ingrese la propiedad y el valor para filtrar el primer nodo")
        filtro1 = {input("Propiedad: "): input("Valor: ")}
        print("Ingrese la propiedad y el valor para filtrar el segundo nodo")
        filtro2 = {input("Propiedad: "): input("Valor: ")}
        crear_relacion_con_propiedades_por_filtro(uri, auth, nodo1, filtro1, nodo2, filtro2, tipo_relacion, propiedades)

    elif res6 == "Y" and res7 == "N":
        print("Ingrese la propiedad y el valor para filtrar el primer nodo")
        filtro1 = {input("Propiedad: "): input("Valor: ")}
        filtro2 = {}
        crear_relacion_con_propiedades_por_filtro(uri, auth, nodo1, filtro1, nodo2, filtro2, tipo_relacion, propiedades)

    elif res6 == "N" and res7 == "Y":
        filtro1 = {}
        print("Ingrese la propiedad y el valor para filtrar el segundo nodo")
        filtro2 = {input("Propiedad: "): input("Valor: ")}
        crear_relacion_con_propiedades_por_filtro(uri, auth, nodo1, filtro1, nodo2, filtro2, tipo_relacion, propiedades)

    elif res6 == "N" and res7 == "N":
        filtro1 = {}
        filtro2 = {}
        crear_relacion_con_propiedades_por_filtro(uri, auth, nodo1, filtro1, nodo2, filtro2, tipo_relacion, propiedades)
            
    else:
        print("No ingresaste una respuesta válida")

def inputs_relaciones():
    respuesta=gestion_Relaciones()

    if respuesta==1:
        add_property_to_relationship(driver)

    if respuesta==2:
        add_empty_property_to_relationship(driver)

    if respuesta==3:
        update_property_in_relationship(driver)

    if respuesta==4:
        update_property_based_on_filter(driver)

    if respuesta==5:
        remove_property_from_relationship(driver)

    if respuesta==6:
        remove_property_from_relationshipGENERAL(driver)




def menu_eliminacion_nodos():
    print("¿Qué desea realizar?")
    print("1. Eliminar un nodo")
    print("2. Eliminar más de un nodo")
    res8 = input("Ingrese el número de la opción: ")

    if res8 == "1":
        nodo = input("Ingrese el nombre del nodo que quiere eliminar")
        fil_preg = input("¿Quiere encontrar por filtro (Y/N)? ")
        if fil_preg == "Y":
            
            print("Ingrese el filtro para encontrar el nodo a eliminar")
            filtros = {input("Propiedad: "): input("Valor: ")}
    
            eliminar_un_nodo_por_filtro(uri, auth, nodo, filtros)
            
        else:
            filtros = {}
            eliminar_un_nodo_por_filtro(uri, auth, nodo, filtros)

    elif res8 == "2":
        n_nodos = int(input("¿Cuántos nodos desea eliminar? "))
        nodos = [input("Ingrese el nombre del nodo a eliminar: ") for i in range(n_nodos)]
        fil_preg = input("¿Quiere encontrar por filtro (Y/N)? ")
        if fil_preg == "Y":
            print("Ingrese el filtro para encontrar el nodo a eliminar")
            filtros = {input("Propiedad: "): input("Valor: ")}
            
            eliminar_multiples_nodos_por_labels_y_filtros(uri, auth, nodos, filtros)

        else:
            filtros = {}
            eliminar_multiples_nodos_por_labels_y_filtros(uri, auth, nodos, filtros)

    else:
        print("Ingrese una opción válida")

def menu_eliminacion_relaciones():
    print("¿Qué desea realizar?")
    print("1. Eliminar una relación")
    print("2. Eliminar varias relaciones")
    res9 = input("Ingrese el número de la opción: ")

    if res9 == "1":
        nodo1 = input("Ingrese el nombre del primer nodo: ")
        preg_res = input("Quiere filtrar? (Y/N) ")
        if preg_res == "Y":
            print("Ingrese los datos para los filtros")
            filtro1 = {input("Propiedad: "): input("Valor: ")}
        else:
            filtro1 = {}
            
        nodo2 = input("Ingrese el nombre del segundo nodo: ")
        preg_res = input("Quiere filtrar? (Y/N) ")
        if preg_res == "Y":
            print("Ingrese los datos para el filtro del segundo nodo")
            filtro2 = {input("Propiedad: "): input("Valor: ")}

        else:
            filtro2 = {}
        tipo_relacion = input("Escribe el tipo de relacion que tiene: ").upper()

        eliminar_una_relacion_por_filtro(uri, auth, nodo1, filtro1, tipo_relacion, nodo2, filtro2)

    elif res9 == "2":

        nodo1 = input("Ingrese el nombre del primer nodo: ")
        preg_res = input("Quiere filtrar? (Y/N) ")
        if preg_res == "Y":
            print("Ingrese los datos para los filtros")
            filtro1 = {input("Propiedad: "): input("Valor: ")}
        else:
            filtro1 = {}
            
        nodo2 = input("Ingrese el nombre del segundo nodo: ")
        preg_res = input("Quiere filtrar? (Y/N) ")
        if preg_res == "Y":
            print("Ingrese los datos para el filtro del segundo nodo")
            filtro2 = {input("Propiedad: "): input("Valor: ")}

        else:
            filtro2 = {}
        tipo_relacion = input("Escribe el tipo de relacion que tiene: ").upper()

        eliminar_multiples_relaciones_por_filtro(uri, auth, nodo1, filtro1, tipo_relacion, nodo2, filtro2)

    else:
        print("Ingrese una opción válida")

def main():
    exit = False
    while exit != True:
        primer_menu()
        res1 = input("Elija una opción (solo colocar número de opción): ")
        if res1 == "1":
            menu_nodos()

        elif res1 == "2":
            menu_consulta_nodos()

        elif res1 == "3":
            menu_operaciones_nodos()

        elif res1 == "4":
            menu_crear_relaciones()

        elif res1 == "5": #gestión de relacion
            inputs_relaciones()

        elif res1 == "6": #
            menu_eliminacion_nodos()

        elif res1 == "7":
            menu_eliminacion_relaciones()

        elif res1 == "8":
            exit = True

        else:
            print("Ingrese un número válido")
        
main()
        



