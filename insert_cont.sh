n=10000
output="commands"
len=1000
rm $output
char="1"
for j in `seq 1 $len` ; do
  str=$str$char
done
for i in `seq 1 $n`; do
  echo " set key_$i 0 0 $len \r\n$str " >> $output
#  echo $str >> $output
done