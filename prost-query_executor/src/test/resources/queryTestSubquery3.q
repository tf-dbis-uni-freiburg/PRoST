SELECT ?person ?result
WHERE 
{
	<http://example.org/alice> <http://example.org/knows> ?person .
	{
		SELECT ?person ((?a+?b) AS ?result)
		WHERE 
		{ ?person <http://example.org/firstValue> ?a.
		  ?person <http://example.org/secondValue> ?b
		} 
	}
}