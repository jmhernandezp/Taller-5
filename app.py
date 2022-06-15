from email.message import Message
import os
import logging
from json import dumps
from urllib import response

from flask import Flask, g, Response, request
from neo4j import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path = "/static/")

# Try to load database connection info from environment
url = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "1234")
neo4jVersion = os.getenv("NEO4J_VERSION", "4")
database = os.getenv("NEO4J_DATABASE", "neo4j")
port = os.getenv("PORT", 8080)


driver = GraphDatabase.driver(url, auth = basic_auth(username, password))

def get_db():
    if not hasattr(g, "neo4j_db"):
        if neo4jVersion.startswith("4"):
            g.neo4j_db = driver.session(database = database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, "neo4j_db"):
        g.neo4j_db.close()

@app.route('/')
def home():
    return "casa"

#crea un comprador
@app.route("/comprador", methods=["POST"])
def create():
    nombre=request.json['name']
    db = get_db()
    summary = db.run("create (c: comprador {nombre:$nombre}) return c",nombre=nombre)
    db.close()

    return  "Comprador creado con exito"

#crea un producto
@app.route("/producto", methods=["POST"])
def createproducto():
    nombre=request.json['name']
    db = get_db()
    summary = db.run("create (p: producto {nombre:$nombre}) return p",nombre=nombre)
    db.close()

    return  "Producto creado con exito"

#crea un vendedor
@app.route("/vendedor", methods=["POST"])
def createvendedor():
    nombre=request.json['name']
    db = get_db()
    summary = db.run("create (V: vemdedor {nombre:$nombre}) return v",nombre=nombre)
    db.close()

    return  "Vendedor creado con exito"
@app.route("/compra", methods=['POST'])
def compra():
    db = get_db()
    comprador = request.json['comprador']
    producto = request.json['producto']
    db.run("MATCH (c: comprador {nombre: $comprador}),(p: producto {nombre: $producto}) CREATE (c)-[COMPRA]->(p) ", producto=producto,comprador=comprador)
    return  "comprado con exito"

@app.route("/vende", methods=['POST'])
def vende():
    db = get_db()
    producto = request.json['producto']
    Vendedor = request.json['vendedor']
    Categoria = request.json['categoria']
    db.run("MATCH (V: vendedor {nombre = vendedor},{p: producto {nombre: producto, categoria: $categoria} CREATE (V)-[VENDE]->(p)",     producto=producto, vendedor=vendedor, Categoria=Categoria )
    return  "Producto vendido con exito"


@app.route("/recomendacion", methods=['POST'])
def recomienda():
    db = get_db()
    comprador = request.json['comprador']
    producto = request.json['producto']
    puntuacion = request.json['puntuacio']
    db.run("MATCH (c: comprador {nombre: $comprador}),(p: producto {nombre: $producto}) CREATE (c)-[RECOMIENDA {puntuacion: $puntuacion}]->(p) ", producto=producto,comprador=comprador,puntuacion=puntuacion)
    return  "recomendado con exito"


@app.route("/top", methods=['GET'])
def Top5():
    db = get_db()
    result = db.run("MATCH (c:Comprador)-[a:COMPRA]->(p:Producto)-[r:RECOMIENDA]->(p) RETURN b.nombre AS nombre, AVG(r.calificacion) AS promedio, count(c) as compras ORDER BY compras DESC, promedio DESC LIMIT 5")
    return Response(dumps(result.data()),  mimetype='application/json')



@app.route("/lanzar", methods=["POST"])
def create2():
    return "ok"

if __name__ == '__main__':
    logging.info('Running on port %d, database is at %s', port, url)
    app.run(port=port)