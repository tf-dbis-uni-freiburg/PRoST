SELECT ?person ?age
WHERE 
{
	<http://example.org/alice> <http://example.org/knows> ?person .
	{
		SELECT ?person ?age
		WHERE 
		{ ?person <http://example.org/age> ?age.FILTER(?age >= 25)} 
	}
}