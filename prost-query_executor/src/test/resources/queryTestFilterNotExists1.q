SELECT ?name
WHERE 
{
  ?person ex:name ?name .
  FILTER NOT EXISTS { ?person ex:knows ?who }
}