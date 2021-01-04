set -x
rp=$(pwd)
cd ~/src
rm -rf zzhdeneva
unzip zzhdeneva.zip 1>/dev/null 2>/dev/null
mv zzhdeneva/config* $rp/../
