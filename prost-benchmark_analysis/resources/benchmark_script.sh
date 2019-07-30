declare -a SETTINGS=(iwpt.csv iwpt_leftouter.csv leftouter.csv outer.csv vp_inner.csv wpt.csv wpt_inner.csv wpt_iwpt.csv wpt_iwpt_inner.csv)

for s in ${SETTINGS[@]}
do
	java -jar benchmark.jar $s
done