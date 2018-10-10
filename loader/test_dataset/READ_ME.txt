# how to run test cases:
1) upload data to HDFS
2) build the logical partitions
3) check the results


test_case1: contains one triple with 4 resources. PRoST-loader should discard triples which have more than 3 elements.
test_case2: contains incomplete triples. In one triple the object is missing. In another triple there is only a subject. PRoST-loader should discard incomplete triples.
test_case3: contains empty lines in between triples at the beginning and at end of the file. PRoST-loader should ignore those lines.
test_case4: to assess whether the dot at the end of each line is removed correctly and it does not appear in the PT or VP. At the same time, only the final dot should be removed (this is particularly important for literals).
test_case5: two predicates can be equal in a case insensitive comparison, but distinct in a case sensitive comparison. Example: pred1/givenName and pred1/givenname. Hive's table and column names are case insentive. Therefore, one of this predicates is discarded in the WPT and VP.
test_case6: It contains an empty file (it only contains a dot, otherwise it cannot be uploaded to HDFS). This leads to empty tables. PRoST-loader should raise an exception.
test_case7: It contains triples with prefixes. They should not be uploaded.
test_case8: It contains duplicates. Duplicates should be discarded automatically.


Expected Results:
****test_case1  -> PASSED
[[<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1>,<http://dbpedia.org/resource/Template:Sisterlinks>],
 [<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1>,<http://dbpedia.org/resource/Res3>],
 [<http://dbpedia.org/resource/Anarchism3>,<http://dbpedia.org/property/pro3>,"wow\" \" . \"ok\" hi"],
 [<http://dbpedia.org/resource/Anarchism4>,<http://dbpedia.org/property/pro2>,<http://dbpedia.org/resource/Res4>],
 [<http://dbpedia.org/resource/Anarchism2>,<http://dbpedia.org/property/pro3>,"wow hi"]]
 
****test_case2  -> PASSED
[[<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1>,<http://dbpedia.org/resource/Template:Sisterlinks>],
 [<http://dbpedia.org/resource/Anarchism5>,<http://dbpedia.org/property/pro3>,<http://dbpedia.org/resource/Res2>]]

****test_case3  -> PASSED
[[<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1>,<http://dbpedia.org/resource/Template:Sisterlinks>],
 [<http://dbpedia.org/resource/Anarchism5>,<http://dbpedia.org/property/pro3>,<http://dbpedia.org/resource/Res2>]]

****test_case4  -> PASSED
[[<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1>,<http://dbpedia.org/resource/Template:Sisterlinks>],
 [<http://dbpedia.org/resource/Anarchism5>,<http://dbpedia.org/property/pro3>,"one literal"],
 [<http://dbpedia.org/resource/Anarchism2>,<http://dbpedia.org/property/pro1>,<http://dbpedia.org/resource/Template:Sisterlinks>],
 [<http://dbpedia.org/resource/Anarchism3>,<http://dbpedia.org/property/pro3>,"This literal contains a dot . which should NOT be removed"],
 [<http://dbpedia.org/resource/Anarchism4>,<http://dbpedia.org/property/pro1>,<http://dbpedia.org/resource/Template:Sisterlinks>],
 [<http://dbpedia.org/resource/Anarchism6>,<http://dbpedia.org/property/pro3>,"one literal"^^<type1>]]

****test_case5  -> PASSED
[[<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1/givenname>,<http://dbpedia.org/resource/Template:Sisterlinks>],
 [<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1/givenname>,<http://dbpedia.org/resource/Res3>],
 [<http://dbpedia.org/resource/Anarchism5>,<http://dbpedia.org/property/pro1/givenName>,<http://dbpedia.org/resource/Res1>]]

****test_case6  -> PASSED
ERROR PRoST - Either your HDFS path does not contain any files or no triples were accepted in the given format (nt)

****test_case7  -> ?

****test_case8  -> PASSED
[[<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1/givenname>,<http://dbpedia.org/resource/Template:Sisterlinks>],
[<http://dbpedia.org/resource/Anarchism1>,<http://dbpedia.org/property/pro1/givenname>,<http://dbpedia.org/resource/Res3>],
[<http://dbpedia.org/resource/Anarchism5>,<http://dbpedia.org/property/pro1/givenname>,<http://dbpedia.org/resource/Res1>]]
