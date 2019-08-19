SELECT ?title ?genre
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/genre> ?genre.
}