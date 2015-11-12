Use the following commands
bin/indexDirectory t/t001/code index1 ldhelper.obj --ldaConfig 64, t/t001/lda/64
generates the index directory

bin/queryWithLDA index1 t/t001/lda/ldaHelper.obj t/t001/bugs latestresults/ -scoringCode 1
generates the query results

following files are important and generated 
1. files.dat
2. vocab.dat
3. words.dat
4. theta.dat

