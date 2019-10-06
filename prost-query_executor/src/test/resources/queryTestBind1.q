SELECT ?result 
WHERE 
{
    BIND((?a+?b) AS ?result) .
    ?value <http://example.org/firstValue> ?a .
    ?value <http://example.org/secondValue> ?b .
}