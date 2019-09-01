SELECT ?name
WHERE 
{
  ?person <http://example.org/name> ?name .
  ?person <http://example.org/knows> ?who.
  MINUS { ?person <http://example.org/knows> <http://example.org/B> } 
}