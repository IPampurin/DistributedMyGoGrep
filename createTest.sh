for i in {1..2000}; do
  if [ $i -eq 100 ] || [ $i -eq 500 ] || [ $i -eq 1500 ]; then
    echo "apple"
  elif [ $i -eq 1800 ]; then
    echo "banana"
  else
    echo "line $i some random text without keywords"
  fi
done > test1.txt