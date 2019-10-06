SELECT ?name
WHERE 
{
  ?person <http://example.org/name> ?name .
  FILTER EXISTS { ?person <http://example.org/knows> ?who . FILTER(?who != ?person) }
}