SELECT ?name ?age
WHERE
{ ?x <http://example.org/name> ?name. 
  ?x <http://example.org/age> ?age. 
  FILTER ( datatype(?age) = <http://example.org/integer> ) }