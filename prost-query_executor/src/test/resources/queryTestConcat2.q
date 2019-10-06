SELECT ?name
WHERE  
{ 
	?alice <http://example.org/givenName> ?g. 
	?alice <http://example.org/surname> ?s 
	BIND(CONCAT(?g, " ", ?s) AS ?name)
}