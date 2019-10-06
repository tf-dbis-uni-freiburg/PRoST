SELECT ?name ?mail
WHERE
{ 
	{SELECT DISTINCT ?person ?name 
	WHERE {?person <http://example.org/name> ?name} 
	ORDER BY ?name LIMIT 2 OFFSET 4}
    OPTIONAL { ?person <http://example.org/mail> ?mail}
}