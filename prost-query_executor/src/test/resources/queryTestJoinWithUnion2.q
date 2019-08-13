SELECT ?x ?y
WHERE  { { ?book <http://example.org/1/title>  ?x } UNION { ?book <http://example.org/2/title>  ?y } }