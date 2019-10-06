SELECT ?name
WHERE 
{
  ?person <http://example.org/name> ?name .
  FILTER NOT EXISTS { ?person <http://example.org/knows> ?who }
}