SELECT ?x 
WHERE 
{
  values ?x { <http://example.org> "string example" "string" }
  FILTER( regex(str(?x), "exam" ))
}