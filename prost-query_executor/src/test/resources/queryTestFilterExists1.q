SELECT ?name
WHERE 
{
  ?person ex:name ?name .
  FILTER EXISTS { ?person ex:knows ?who . FILTER(?who != ?person) }
}