SELECT ?word
WHERE
{ ?words <http://example.org/word> ?word. FILTER (lang(?word) = "EN") }