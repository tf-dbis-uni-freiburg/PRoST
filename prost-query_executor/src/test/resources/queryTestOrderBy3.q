SELECT ?title ?price
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/price> ?price
}
ORDER BY DESC(?price)