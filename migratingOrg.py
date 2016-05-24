from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Namespace, RDF
import json
import requests
import multiprocessing
from itertools import chain
import functools
import argparse
import uuid
import time
import math

ENDPOINT='http://deepcarbon.tw.rpi.edu:3030/VIVO/query'

sparql = SPARQLWrapper(ENDPOINT)

version = time.strftime("%Y%m%d-%H%M%S")

remove_file_name = "triples-to-remove-" + version
add_file_name = "triples-to-add-" + version
# Generate the list of triples to remove

sparql.setQuery("""
    PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
	PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
	PREFIX owl:   <http://www.w3.org/2002/07/owl#>
	PREFIX swrl:  <http://www.w3.org/2003/11/swrl#>
	PREFIX swrlb: <http://www.w3.org/2003/11/swrlb#>
	PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>
	PREFIX bibo: <http://purl.org/ontology/bibo/>
	PREFIX c4o: <http://purl.org/spar/c4o/>
	PREFIX cito: <http://purl.org/spar/cito/>
	PREFIX dcat: <http://www.w3.org/ns/dcat#>
	PREFIX dco: <http://info.deepcarbon.net/schema#>
	PREFIX event: <http://purl.org/NET/c4dm/event.owl#>
	PREFIX fabio: <http://purl.org/spar/fabio/>
	PREFIX foaf: <http://xmlns.com/foaf/0.1/>
	PREFIX vivo: <http://vivoweb.org/ontology/core#>
    SELECT ?person ?org
    WHERE 
    { 
    ?person dco:inOrganization ?org . 
    ?org dco:hasPeople ?person . 
    }
""")


sparql.setReturnFormat(JSON)
results = sparql.query().convert()
list_of_triples_to_remove = open(remove_file_name, 'w+')
for result in results["results"]["bindings"]:
	person = result["person"]["value"]
	org = result["org"]["value"]
	list_of_triples_to_remove.write("<" + person + ">" + " <http://info.deepcarbon.net/schema#inOrganization> <" + org + "> .\n")
	list_of_triples_to_remove.write("<" + org 	  + ">" + " <http://info.deepcarbon.net/schema#hasPeople> <" 	  + person + "> .\n")

list_of_triples_to_remove.close()


# Generate the list of triples to add

sparql.setQuery("""
    PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
	PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
	PREFIX owl:   <http://www.w3.org/2002/07/owl#>
	PREFIX swrl:  <http://www.w3.org/2003/11/swrl#>
	PREFIX swrlb: <http://www.w3.org/2003/11/swrlb#>
	PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>
	PREFIX bibo: <http://purl.org/ontology/bibo/>
	PREFIX c4o: <http://purl.org/spar/c4o/>
	PREFIX cito: <http://purl.org/spar/cito/>
	PREFIX dcat: <http://www.w3.org/ns/dcat#>
	PREFIX dco: <http://info.deepcarbon.net/schema#>
	PREFIX event: <http://purl.org/NET/c4dm/event.owl#>
	PREFIX fabio: <http://purl.org/spar/fabio/>
	PREFIX foaf: <http://xmlns.com/foaf/0.1/>
	PREFIX vivo: <http://vivoweb.org/ontology/core#>
    SELECT DISTINCT ?person ?org
    WHERE 
    { 
    ?person dco:inOrganization ?org . 
    ?org dco:hasPeople ?person . 
    NOT EXISTS
	{
	?role a vivo:Position.
	?role vivo:relates ?person.
	?role vivo:relates ?org.
	}
    }
""")
sparql.setReturnFormat(JSON)
results = sparql.query().convert()

number_of_separated_files = 20
number_of_results = len(results["results"]["bindings"])
max_number_of_results_per_file = math.floor(number_of_results/(number_of_separated_files-1))


file_number = 1
list_of_triples_to_add = open(add_file_name + "-" + str(file_number).zfill(2), 'w+')
result_count = 0
for result in results["results"]["bindings"]:
	person = result["person"]["value"]
	org = result["org"]["value"]
	role = "http://info.deepcarbon.net/individual/pos-" + str(uuid.uuid4())
	list_of_triples_to_add.write("<" + person + "> <http://vivoweb.org/ontology/core#relatedBy> <" + role + "> .\n")
	list_of_triples_to_add.write("<" + org 	  +	"> <http://vivoweb.org/ontology/core#relatedBy> <" + role + "> .\n")
	list_of_triples_to_add.write("<" + role   + "> <http://vivoweb.org/ontology/core#relates> <" + person + "> .\n")
	list_of_triples_to_add.write("<" + role   + "> <http://vivoweb.org/ontology/core#relates> <" + org + "> .\n")
	list_of_triples_to_add.write("<" + role   + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Position> .\n")
	list_of_triples_to_add.write("<" + role   + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Relationship> .\n")
	list_of_triples_to_add.write("<" + role   + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.obolibrary.org/obo/BFO_0000020> .\n")
	list_of_triples_to_add.write("<" + role   + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.obolibrary.org/obo/BFO_0000001> .\n")
	list_of_triples_to_add.write("<" + role   + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.obolibrary.org/obo/BFO_0000002> .\n")
	list_of_triples_to_add.write("<" + role   + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Thing> .\n")
	list_of_triples_to_add.write("<" + role   +	"> <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#mostSpecificType> <http://vivoweb.org/ontology/core#Position> .\n")
	list_of_triples_to_add.write("<" + role   + '> <http://www.w3.org/2000/01/rdf-schema#label> "Member"^^<http://www.w3.org/2001/XMLSchema#string> .\n')
	result_count += 1
	if result_count >= max_number_of_results_per_file:
		list_of_triples_to_add.close()
		file_number += 1
		list_of_triples_to_add = open(add_file_name + "-" + str(file_number).zfill(2), 'w+')
		result_count = 0

		
list_of_triples_to_add.close()
