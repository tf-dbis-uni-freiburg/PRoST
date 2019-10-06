SELECT ?name ?mail
WHERE
{
	?person <http://example.org/name> ?name.
	OPTIONAL { ?person <http://example.org/mail> ?mail }
}
ORDER BY ?name LIMIT 2 OFFSET 4