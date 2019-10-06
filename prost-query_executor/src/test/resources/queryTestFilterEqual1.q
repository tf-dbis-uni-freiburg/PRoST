SELECT ?title ?price
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/price> ?price.FILTER(?price = 50)
}