test_case1: contains one triple with 4 resources. PRoST-loader should discard triples which have more then 3 elements.
test_case2: contains incomplete triples. In one triple the object is missing. In another triple there is only subject. PRoST-loader should discard incomplete triples.
test_case3: contains empty lines in between triples and at the end of file. PRoST-loader should ignore those lines.
