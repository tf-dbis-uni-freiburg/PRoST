SELECT ?person ?shortName
WHERE 
{
	<http://example.org/alice> <http://example.org/knows> ?person .
	{
		SELECT ?person (MIN(?name) AS ?shortName)
		WHERE 
		{ ?person <http://example.org/name> ?name .} 
		GROUP BY ?person
	}
}