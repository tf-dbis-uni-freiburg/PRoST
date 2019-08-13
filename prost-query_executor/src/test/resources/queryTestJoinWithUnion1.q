SELECT ?title
WHERE  {{  ?book <http://example.org/1/title>  ?title. } UNION { ?book <http://example.org/2/title>  ?title. } }