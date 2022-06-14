DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PWD=`pwd`
cd $DIR
black --verbose -l 120 $DIR/../mergin
cd $PWD
